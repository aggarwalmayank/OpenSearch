/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.BinaryValueReader;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBinaryBatch;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The value cursor must be closed once the iterator that opened it is unreachable, so a cursor
 * reached through a cache-retained leaf reader is not held until its segment retires.
 */
public class ParquetSortedDocValuesTests extends OpenSearchTestCase {

    /** Records close() so the test can observe the cleanup without opening a native cursor. */
    private static final class FakeReader implements BinaryValueReader, AutoCloseable {
        private final AtomicBoolean closed;

        FakeReader(AtomicBoolean closed) {
            this.closed = closed;
        }

        @Override
        public DecodedBinaryBatch decodedBinaryBatch() {
            return null;
        }

        @Override
        public void loadBatchContaining(long row) {}

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static void openCursorThenAbandonIterator(AtomicBoolean closed) throws IOException {
        ParquetSortedDocValues values = new ParquetSortedDocValues(() -> new FakeReader(closed), 10);
        try {
            values.advanceExact(0);
        } catch (RuntimeException expected) {
            // The fake serves no batch; the cursor has already been opened and its cleanup registered.
        }
        assertFalse("cursor must stay open while the iterator is reachable", closed.get());
    }

    public void testCursorIsClosedWhenIteratorBecomesUnreachable() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        openCursorThenAbandonIterator(closed);
        assertBusy(() -> {
            System.gc();
            assertTrue("cursor must be closed once the iterator is unreachable", closed.get());
        });
    }

    /**
     * Segment-global ordinal operations must refuse rather than answer from per-document state, and the
     * message must name the remedy. A caller reaching these has asked for something this tier cannot do.
     */
    public void testGetValueCountRefusesAndNamesTheRemedy() {
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, withoutCursor(10)::getValueCount);
        assertTrue(e.getMessage(), e.getMessage().contains("execution_hint:map"));
    }

    public void testLookupTermRefusesAndNamesTheRemedy() {
        ParquetSortedDocValues values = withoutCursor(10);
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> values.lookupTerm(new BytesRef("alpha")));
        assertTrue(e.getMessage(), e.getMessage().contains("execution_hint:map"));
    }

    /** A target past the segment's last document is answered without opening a cursor. */
    public void testAdvanceExactBeyondMaxDocOpensNoCursor() throws IOException {
        ParquetSortedDocValues values = withoutCursor(10);
        assertFalse(values.advanceExact(10));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.docID());
    }

    /** An iterator whose reader factory fails the test if called, for branches that must not open a cursor. */
    private static ParquetSortedDocValues withoutCursor(int maxDoc) {
        return new ParquetSortedDocValues(() -> { throw new AssertionError("no cursor must be opened"); }, maxDoc);
    }
}
