/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.UninvertedOrdinals;

import java.io.IOException;

/**
 * Multi-valued {@link SortedSetDocValues} for keyword fields backed by a multi-valued ord file: segment-global
 * ordinals per document; the current document's words come from the streaming reader, other ordinals from the term index.
 */
public final class ParquetUninvertedSortedSetDocValues extends SortedSetDocValues {

    private final UninvertedOrdinals ordinals;
    private final ParquetSortedSetDocValues streaming;
    private final int maxDoc;

    private UninvertedOrdinals.MultiValuedOrdinalsCursor ordCursor;
    private UninvertedOrdinals.TermCursor termCursor;
    /** The current document's ordinals, ascending; the first {@link #docOrdinalCount} entries are live. */
    private long[] docOrdinals = new long[0];
    private int docOrdinalCount;
    private int nextOrdinal;
    private int doc = -1;
    private boolean streamingPositioned = false;

    public ParquetUninvertedSortedSetDocValues(UninvertedOrdinals ordinals, ParquetSortedSetDocValues streaming, int maxDoc) {
        this.ordinals = ordinals;
        this.streaming = streaming;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) {
        docOrdinalCount = 0;
        nextOrdinal = 0;
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            return false;
        }
        doc = target;
        streamingPositioned = false; // value read is lazy; most consumers never need it
        if (ordCursor == null) {
            ordCursor = ordinals.newMultiValuedOrdinalsCursor();
        }
        int count = ordCursor.advance(target);
        if (docOrdinals.length < count) {
            docOrdinals = ArrayUtil.grow(docOrdinals, count);
        }
        for (int i = 0; i < count; i++) {
            docOrdinals[i] = ordCursor.nextOrdinal();
        }
        docOrdinalCount = count;
        return count > 0;
    }

    @Override
    public int docValueCount() {
        return docOrdinalCount;
    }

    @Override
    public long nextOrd() {
        return docOrdinals[nextOrdinal++];
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
        int position = positionInCurrentDoc(ord);
        if (position >= 0) {
            if (streamingPositioned == false) {
                streaming.advanceExact(doc);
                streamingPositioned = true;
            }
            // Both list the document's words sorted and without repeats, so equal counts mean equal positions.
            if (streaming.docValueCount() == docOrdinalCount) {
                return streaming.lookupOrd(((long) doc << 32) | position);
            }
        }
        if (termCursor == null) {
            termCursor = ordinals.newTermCursor();
        }
        return termCursor.term((int) ord);
    }

    /** Where {@code ord} sits among the current document's ordinals, or -1 when it is not one of them. */
    private int positionInCurrentDoc(long ord) {
        if (doc < 0 || doc == NO_MORE_DOCS) {
            return -1;
        }
        for (int i = 0; i < docOrdinalCount; i++) {
            if (docOrdinals[i] == ord) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public long getValueCount() {
        return ordinals.valueCount();
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
        docOrdinalCount = 0;
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
