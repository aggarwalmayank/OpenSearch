/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.NumericValueReader;

import java.io.IOException;

/**
 * {@link BinaryDocValues} over a single-valued Parquet variable-width byte column.
 *
 * <p>The parallel of {@link ParquetNumericDocValues} for the binary borrow kind. Hot path: a presence
 * bit-test plus an in-place read from the reader's resident {@link DecodedBatch}. When the requested
 * document falls outside that batch, {@link NumericValueReader#loadBatchContaining} decodes the batch
 * that holds it - the only step that crosses the native boundary.
 *
 * <p>{@link #binaryValue()} returns the row's bytes exactly as the writer stored them. The reader
 * ({@link NumericValueReader}) is shared with the numeric iterator; only the per-row read differs
 * ({@link DecodedBatch#bytesAt} rather than {@code valueAt}).
 */
public final class ParquetBinaryDocValues extends BinaryDocValues {

    private final NumericValueReader reader;
    private final int maxDoc;

    private int doc = -1;
    /**
     * The current document's bytes. One buffer for the iterator's lifetime, refilled by
     * {@link DecodedBatch#bytesAt} on each positive advance and grown only when a row is longer than
     * any seen before; per the {@link BinaryDocValues} contract the returned value is valid only until
     * the iterator moves again.
     */
    private final BytesRef scratch = new BytesRef(BytesRef.EMPTY_BYTES);
    private BytesRef currentValue;

    public ParquetBinaryDocValues(NumericValueReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        DecodedBatch batch = reader.decodedBatch();
        if (batch == null || batch.contains(target) == false) {
            reader.loadBatchContaining(target);
            batch = reader.decodedBatch();
        }
        boolean present = batch.isPresent(target);
        if (present) {
            batch.bytesAt(target, scratch);
            currentValue = scratch;
        } else {
            currentValue = null;
        }
        return present;
    }

    @Override
    public BytesRef binaryValue() {
        return currentValue;
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
        int d = target;
        while (d < maxDoc) {
            DecodedBatch batch = reader.decodedBatch();
            if (batch == null || batch.contains(d) == false) {
                reader.loadBatchContaining(d);
                batch = reader.decodedBatch();
            }
            // Dense batches answer immediately; sparse batches skip whole all-null bitmap bytes.
            long next = batch.nextPresentRow(d);
            if (next >= 0) {
                doc = (int) next;
                batch.bytesAt(next, scratch);
                currentValue = scratch;
                return doc;
            }
            d = (int) batch.lastRow() + 1;
        }
        doc = NO_MORE_DOCS;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
