/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.parquet.codec.UninvertedSortedSetOrdinals;

import java.io.IOException;

/**
 * Segment-global {@link SortedSetDocValues} for a multi-valued keyword field, backed by disk-resident
 * {@link UninvertedSortedSetOrdinals}. Unlike the streaming {@link ParquetSortedSetDocValues} (which
 * only serves per-document transient ords and rejects global-ordinal operations), this exposes real
 * segment-global ordinals, so {@code terms} aggregations run on the {@code global_ordinals} path
 * (no {@code execution_hint:map} required).
 *
 * <p>Per document, {@code advanceExact} looks up the doc's ordinal span {@code [start, end)} from the
 * offsets, and {@link #nextOrd()} walks that slice of the flat ords stream — the ordinals are already
 * ascending and de-duplicated (the build writes each document's set sorted), satisfying SORTED_SET.
 */
public final class ParquetUninvertedSortedSetDocValues extends SortedSetDocValues {

    private final UninvertedSortedSetOrdinals ordinals;
    private final int maxDoc;

    private int doc = -1;
    private long start;
    private long end;
    private long pos;

    public ParquetUninvertedSortedSetDocValues(UninvertedSortedSetOrdinals ordinals, int maxDoc) {
        this.ordinals = ordinals;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            start = end = pos = 0;
            return false;
        }
        doc = target;
        start = ordinals.startOffset(target);
        end = ordinals.endOffset(target);
        pos = start;
        return end > start;
    }

    /** The next global ordinal for the current document. Called exactly {@link #docValueCount()} times. */
    @Override
    public long nextOrd() {
        return ordinals.ordAt(pos++);
    }

    @Override
    public int docValueCount() {
        return (int) (end - start);
    }

    @Override
    public BytesRef lookupOrd(long ord) {
        // Global ordinals: any ord resolves to its term via the shared checkpoint-backed resolver.
        return ordinals.term((int) ord);
    }

    @Override
    public long getValueCount() {
        return ordinals.getValueCount();
    }

    @Override
    public long lookupTerm(BytesRef key) {
        return ordinals.rank(key);
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
        return ordinals.termsEnum();
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
        start = end = pos = 0;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
