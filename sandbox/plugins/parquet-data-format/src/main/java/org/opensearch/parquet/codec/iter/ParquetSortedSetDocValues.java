/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.BinaryPageReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * Streaming {@link SortedSetDocValues} over a Parquet keyword column: per-document values only,
 * no segment-wide ordinal structure. Transient ordinals encode their issuing document
 * ({@code doc << 20 | index}) so a stale ord from another document is rejected, not resolved
 * wrongly. Segment-global operations ({@link #getValueCount()}, {@link #lookupTerm},
 * {@link #termsEnum()}) throw, steering ordinal-comparing consumers to
 * {@code execution_hint: map}; per-document consumers (fetch, bytes views, map-mode aggs) are
 * served zero-copy at O(rows visited).
 */
public final class ParquetSortedSetDocValues extends SortedSetDocValues {

    /** Bits reserved for the value index within a doc; bounds values-per-doc at ~1M. */
    private static final int ORD_INDEX_BITS = 20;
    private static final long ORD_INDEX_MASK = (1L << ORD_INDEX_BITS) - 1;

    private final BinaryPageReader reader;
    private final boolean repeated;
    private final int maxDoc;

    private int doc = -1;
    private BytesRef[] values = new BytesRef[0];
    private int count;
    private int cursor;

    public ParquetSortedSetDocValues(BinaryPageReader reader, boolean repeated, int maxDoc) {
        this.reader = reader;
        this.repeated = repeated;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            count = 0;
            cursor = 0;
            return false;
        }
        doc = target;
        cursor = 0;
        if (repeated) {
            byte[][] raw = reader.readRepeatedBytesAtRow(target);
            count = normalize(raw);
        } else {
            byte[] value = reader.readBytesAtRow(target);
            if (value == null) {
                count = 0;
            } else {
                ensureCapacity(1);
                values[0] = new BytesRef(value);
                count = 1;
            }
        }
        return count > 0;
    }

    /** SORTED_SET semantics: values sorted ascending and de-duplicated within the doc. */
    private int normalize(byte[][] raw) {
        if (raw == null || raw.length == 0) {
            return 0;
        }
        ensureCapacity(raw.length);
        for (int i = 0; i < raw.length; i++) {
            values[i] = new BytesRef(raw[i]);
        }
        Arrays.sort(values, 0, raw.length);
        int unique = 1;
        for (int i = 1; i < raw.length; i++) {
            if (values[i].bytesEquals(values[unique - 1]) == false) {
                values[unique++] = values[i];
            }
        }
        return unique;
    }

    private void ensureCapacity(int needed) {
        if (values.length < needed) {
            values = new BytesRef[Math.max(needed, values.length * 2)];
        }
    }

    @Override
    public long nextOrd() {
        return ((long) doc << ORD_INDEX_BITS) | cursor++;
    }

    @Override
    public int docValueCount() {
        return count;
    }

    @Override
    public BytesRef lookupOrd(long ord) {
        int ordDoc = (int) (ord >>> ORD_INDEX_BITS);
        int index = (int) (ord & ORD_INDEX_MASK);
        if (ordDoc != doc || index >= count) {
            throw new UnsupportedOperationException(
                "ordinal "
                    + ord
                    + " was issued for doc "
                    + ordDoc
                    + " but doc "
                    + doc
                    + " is current: composite Parquet keyword fields serve per-document streaming "
                    + "ordinals only; consumers requiring segment-global ordinals must use "
                    + "execution_hint:map"
            );
        }
        return values[index];
    }

    @Override
    public long getValueCount() {
        throw unsupportedGlobalOrdinals("getValueCount");
    }

    @Override
    public long lookupTerm(BytesRef key) {
        throw unsupportedGlobalOrdinals("lookupTerm");
    }

    private static UnsupportedOperationException unsupportedGlobalOrdinals(String operation) {
        return new UnsupportedOperationException(
            operation
                + " requires segment-global ordinals, which composite Parquet keyword fields do "
                + "not materialize at read time; aggregations on these fields must use "
                + "execution_hint:map"
        );
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
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
        count = 0;
        cursor = 0;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
