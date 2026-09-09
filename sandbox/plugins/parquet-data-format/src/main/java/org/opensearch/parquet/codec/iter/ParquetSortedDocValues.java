/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.bridge.BinaryPageReader;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/**
 * Streaming single-valued {@link SortedDocValues} over a Parquet keyword column: per-document
 * values only, no segment-wide ordinal structure. The doc id doubles as a transient ordinal,
 * resolved immediately via {@link #lookupOrd}; segment-global operations ({@link #getValueCount},
 * {@link #lookupTerm}) throw rather than return wrong results, steering ordinal-comparing
 * consumers to {@code execution_hint: map}.
 */
public final class ParquetSortedDocValues extends SortedDocValues {

    private final BinaryPageReader reader;
    private final int maxDoc;
    private final BytesRef scratch = new BytesRef();

    private int doc = -1;
    private boolean currentPresent;

    public ParquetSortedDocValues(BinaryPageReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            return false;
        }
        doc = target;
        // Zero-copy: serve the value as a view into the resident page buffer.
        PageCache cache = reader.cache();
        if (cache == null || target > cache.lastRow || target < cache.firstRow) {
            reader.loadPageContaining(target);
            cache = reader.cache();
            if (cache == null) {
                currentPresent = false;
                return false;
            }
        }
        currentPresent = cache.isPresent(target);
        if (currentPresent) {
            int rel = (int) (target - cache.firstRow);
            int start = cache.byteOffsets[rel];
            int end = cache.byteOffsets[rel + 1];
            scratch.bytes = cache.byteBuf;
            scratch.offset = start;
            scratch.length = end - start;
        }
        return currentPresent;
    }

    @Override
    public int ordValue() {
        // The doc id doubles as the transient ordinal; lookupOrd verifies it.
        return doc;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
        if (ord != doc || currentPresent == false) {
            throw new UnsupportedOperationException(
                "ordinal "
                    + ord
                    + " was issued for another document (current doc "
                    + doc
                    + "): composite Parquet keyword fields serve per-document streaming ordinals "
                    + "only; consumers requiring segment-global ordinals must use execution_hint:map"
            );
        }
        return scratch;
    }

    @Override
    public int getValueCount() {
        throw new UnsupportedOperationException(
            "getValueCount requires segment-global ordinals, which composite Parquet keyword "
                + "fields do not materialize at read time; aggregations on these fields must use "
                + "execution_hint:map"
        );
    }

    @Override
    public int lookupTerm(BytesRef key) {
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
        currentPresent = false;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
