/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Shared sidecar term-dictionary resolution for uninverted ordinals, used by both the single-valued
 * {@link UninvertedOrdinals} and the multi-valued {@code UninvertedSortedSetOrdinals}.
 *
 * <p>An "ordinal" is a term's 0-based rank in the field's sorted term space. This class holds that
 * term space ({@link Terms} from the Lucene sidecar) plus sparse in-heap <b>checkpoints</b> — every
 * {@code checkpointInterval} terms, the term bytes are sampled — so that:
 * <ul>
 *   <li>{@link #term(int)} / {@link TermCursor} resolve an ordinal to its term via a checkpoint seek
 *       plus a bounded {@code TermsEnum} advance (never a full scan),</li>
 *   <li>{@link #rank(BytesRef)} resolves a term to its ordinal via binary search over checkpoints
 *       plus a bounded walk,</li>
 *   <li>{@link #termsEnum()} exposes the ordinal space in order with a working {@link TermsEnum#ord()}.</li>
 * </ul>
 * This is the term axis only; the per-document ord storage (single ord, or a sorted set of ords) lives
 * in the owning class.
 */
final class KeywordTermOrdinals {

    private final Terms terms;
    private final BytesRef[] checkpoints;
    private final int valueCount;

    KeywordTermOrdinals(Terms terms, BytesRef[] checkpoints, int valueCount) {
        this.terms = terms;
        this.checkpoints = checkpoints;
        this.valueCount = valueCount;
    }

    /** Current checkpoint interval from the dynamic node setting {@code parquet.docvalues.checkpoint.interval}. */
    static int checkpointInterval() {
        return ParquetDocValuesProducer.checkpointInterval();
    }

    /** Number of distinct terms (the ordinal count). */
    int valueCount() {
        return valueCount;
    }

    /** Persists the sampled checkpoint terms (length-prefixed) — shared by every ord-file format. */
    static void writeCheckpoints(IndexOutput out, List<BytesRef> checkpoints) throws IOException {
        for (BytesRef checkpoint : checkpoints) {
            out.writeInt(checkpoint.length);
            out.writeBytes(checkpoint.bytes, checkpoint.offset, checkpoint.length);
        }
    }

    /** Reads {@code checkpointCount} checkpoint terms starting at {@code checkpointStart}. */
    static BytesRef[] readCheckpoints(IndexInput input, long checkpointStart, int checkpointCount) throws IOException {
        input.seek(checkpointStart);
        BytesRef[] checkpoints = new BytesRef[checkpointCount];
        for (int i = 0; i < checkpointCount; i++) {
            int length = input.readInt();
            if (length < 0) {
                throw new IOException("ord file checkpoint length is invalid");
            }
            byte[] bytes = new byte[length];
            input.readBytes(bytes, 0, length);
            checkpoints[i] = new BytesRef(bytes);
        }
        return checkpoints;
    }

    /**
     * The field's real terms enumeration wrapped with ordinal tracking, because consumers like
     * {@code OrdinalMap} require {@link TermsEnum#ord()} which BlockTree does not implement.
     */
    TermsEnum termsEnum() throws IOException {
        return new OrdTrackingTermsEnum(terms.iterator());
    }

    private final class OrdTrackingTermsEnum extends FilterLeafReader.FilterTermsEnum {
        private long position = -1;

        OrdTrackingTermsEnum(TermsEnum in) {
            super(in);
        }

        @Override
        public BytesRef next() throws IOException {
            BytesRef term = in.next();
            if (term != null) {
                position++;
            } else {
                position = valueCount;
            }
            return term;
        }

        @Override
        public long ord() {
            return position;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            int interval = checkpointInterval();
            int checkpoint = (int) (ord / interval);
            in.seekCeil(checkpoints[checkpoint]);
            position = (long) checkpoint * interval;
            while (position < ord) {
                in.next();
                position++;
            }
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            boolean found = in.seekExact(text);
            position = found ? rank(text) : -1;
            return found;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            SeekStatus status = in.seekCeil(text);
            if (status == SeekStatus.END) {
                position = valueCount;
            } else {
                int r = rank(in.term());
                position = r >= 0 ? r : -(r + 1);
            }
            return status;
        }
    }

    /** Resolves an ordinal to its term: checkpoint seek plus a bounded enum advance. */
    BytesRef term(int ord) {
        try {
            int interval = checkpointInterval();
            TermsEnum termsEnum = terms.iterator();
            int checkpoint = ord / interval;
            termsEnum.seekCeil(checkpoints[checkpoint]);
            for (int i = checkpoint * interval; i < ord; i++) {
                termsEnum.next();
            }
            return BytesRef.deepCopyOf(termsEnum.term());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A single-consumer stateful term resolver: ascending ordinal walks cost one enum pass. */
    TermCursor newTermCursor() {
        return new TermCursor();
    }

    /**
     * Stateful ord→term resolution for one consumer (not thread-safe, like doc-values iterators).
     * Advances forward from its last position when the requested ordinal is ahead, so monotonic
     * access amortizes to a single sequential pass over the terms file.
     */
    final class TermCursor {
        private TermsEnum cursorEnum;
        private long cursorOrd = -1;

        public BytesRef term(int ord) {
            try {
                int interval = checkpointInterval();
                long delta = cursorEnum == null ? Long.MAX_VALUE : ord - cursorOrd;
                if (delta < 0 || delta > interval) {
                    cursorEnum = terms.iterator();
                    int checkpoint = ord / interval;
                    cursorEnum.seekCeil(checkpoints[checkpoint]);
                    cursorOrd = (long) checkpoint * interval;
                }
                while (cursorOrd < ord) {
                    cursorEnum.next();
                    cursorOrd++;
                }
                return cursorEnum.term();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The ordinal of {@code key}, or {@code -insertionPoint - 1} (the lookupTerm contract). */
    int rank(BytesRef key) {
        try {
            int interval = checkpointInterval();
            if (checkpoints.length == 0) {
                return -1;
            }
            int low = 0;
            int high = checkpoints.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = checkpoints[mid].compareTo(key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid * interval;
                }
            }
            int checkpoint = Math.max(low - 1, 0);
            TermsEnum termsEnum = terms.iterator();
            termsEnum.seekCeil(checkpoints[checkpoint]);
            int ord = checkpoint * interval;
            BytesRef term = termsEnum.term();
            while (term != null) {
                int cmp = term.compareTo(key);
                if (cmp == 0) {
                    return ord;
                }
                if (cmp > 0) {
                    return -(ord + 1);
                }
                term = termsEnum.next();
                ord++;
            }
            return -(ord + 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
