/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBinaryBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListBinaryValueReader;
import org.opensearch.common.CheckedSupplier;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.Arrays;

/**
 * Streaming multi-valued {@link SortedSetDocValues} over a repeated Parquet keyword column: each
 * document's values sorted and deduplicated, no segment-wide ordinal structure. An ordinal packs the
 * doc id above the value's position within the document, so {@link #lookupOrd} can reject one issued
 * for another document; segment-global operations throw, as in {@link ParquetSortedDocValues}.
 *
 * <p>The value reader is built lazily on the first value request, so a consumer that never reads
 * values never opens a native cursor.
 */
public final class ParquetSortedSetDocValues extends SortedSetDocValues {

    private static final Logger LOGGER = LogManager.getLogger(ParquetSortedSetDocValues.class);

    /** Closes the value reader once this iterator is unreachable, so the native cursor is not held until the segment retires. */
    private static final Cleaner CLEANER = Cleaner.create();

    /** Recipe for the value reader; runs on the first value request only. */
    private final CheckedSupplier<ListBinaryValueReader, IOException> readerFactory;
    private final int maxDoc;

    /** Heap copies of the current document's values, reused across documents; the batch's buffers are off-heap. */
    private BytesRefBuilder[] slots = new BytesRefBuilder[0];
    /** The current document's distinct values in sorted order; the first {@link #valueCount} entries are live. */
    private BytesRef[] values = new BytesRef[0];
    private int valueCount;
    private int nextValue;

    /** Built lazily by {@link #reader()}; null until the first value request. */
    private ListBinaryValueReader reader;

    private int doc = -1;

    public ParquetSortedSetDocValues(CheckedSupplier<ListBinaryValueReader, IOException> readerFactory, int maxDoc) {
        this.readerFactory = readerFactory;
        this.maxDoc = maxDoc;
    }

    private ListBinaryValueReader reader() throws IOException {
        if (reader == null) {
            ListBinaryValueReader opened = readerFactory.get();
            // Capture only the reader: capturing this iterator would keep it reachable, which is why
            // NativeHandle's own cleaner never fires.
            if (opened instanceof AutoCloseable closeable) {
                CLEANER.register(this, () -> closeCursorAndLogFailure(closeable));
            }
            reader = opened;
        }
        return reader;
    }

    /** Closes the value cursor on the cleaner thread, where throwing would kill the thread silently. */
    private static void closeCursorAndLogFailure(AutoCloseable cursor) {
        try {
            cursor.close();
        } catch (Exception e) {
            LOGGER.warn("failed to close parquet value cursor", e);
        }
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        valueCount = 0;
        nextValue = 0;
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        ListBinaryValueReader valueReader = reader();
        DecodedListBinaryBatch batch = valueReader.decodedListBinaryBatch();
        if (batch == null || batch.contains(target) == false) {
            valueReader.loadListBinaryBatchContaining(target);
            batch = valueReader.decodedListBinaryBatch();
        }
        int start = batch.startItem(target);
        int items = batch.endItem(target) - start;
        if (items == 0) {
            return false;
        }
        copyItems(batch, start, items);
        valueCount = sortAndDeduplicate(items);
        return true;
    }

    /** Copies the document's items into {@link #slots} and points {@link #values} at them. */
    private void copyItems(DecodedListBinaryBatch batch, int start, int items) {
        if (slots.length < items) {
            int grown = slots.length;
            slots = ArrayUtil.grow(slots, items);
            for (int i = grown; i < slots.length; i++) {
                slots[i] = new BytesRefBuilder();
            }
            values = new BytesRef[slots.length];
        }
        for (int i = 0; i < items; i++) {
            BytesRefBuilder slot = slots[i];
            int length = batch.itemLength(start + i);
            slot.grow(length);
            batch.copyItem(start + i, slot.bytes());
            slot.setLength(length);
            values[i] = slot.get();
        }
    }

    /**
     * Parquet keeps a document's list as indexed, order and repeats included; sorting and dropping repeats
     * makes the values match vanilla keyword doc values, which Lucene serves ascending and distinct.
     */
    private int sortAndDeduplicate(int items) {
        Arrays.sort(values, 0, items);
        int distinct = 1;
        for (int i = 1; i < items; i++) {
            if (values[i].bytesEquals(values[distinct - 1]) == false) {
                values[distinct++] = values[i];
            }
        }
        return distinct;
    }

    @Override
    public int docValueCount() {
        return valueCount;
    }

    @Override
    public long nextOrd() {
        return ((long) doc << 32) | nextValue++;
    }

    @Override
    public BytesRef lookupOrd(long ord) {
        int ordDoc = (int) (ord >>> 32);
        int position = (int) ord;
        if (ordDoc != doc || position < 0 || position >= valueCount) {
            throw new UnsupportedOperationException(
                "ordinal "
                    + ord
                    + " was issued for another document (current doc "
                    + doc
                    + "): composite Parquet keyword fields serve per-document streaming ordinals "
                    + "only; consumers requiring segment-global ordinals must use execution_hint:map"
            );
        }
        return values[position];
    }

    @Override
    public long getValueCount() {
        throw new UnsupportedOperationException(
            "getValueCount requires segment-global ordinals, which composite Parquet keyword "
                + "fields do not materialize at read time; aggregations on these fields must use "
                + "execution_hint:map"
        );
    }

    @Override
    public long lookupTerm(BytesRef key) {
        throw new UnsupportedOperationException(
            "lookupTerm requires segment-global ordinals, which composite Parquet keyword "
                + "fields do not materialize at read time; aggregations on these fields must use "
                + "execution_hint:map"
        );
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        valueCount = 0;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
