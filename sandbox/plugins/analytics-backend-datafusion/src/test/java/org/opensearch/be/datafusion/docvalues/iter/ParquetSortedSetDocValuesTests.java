/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedListBinaryBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ListBinaryValueReader;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Each document's list must come back sorted and distinct, as Lucene serves keyword doc values, and the
 * value cursor must be closed once the iterator that opened it is unreachable.
 */
public class ParquetSortedSetDocValuesTests extends OpenSearchTestCase {

    /** Serves heap-built batches and records close(), so the iterator is tested without a native cursor. */
    private static final class FakeListReader implements ListBinaryValueReader, AutoCloseable {
        private final List<DecodedListBinaryBatch> batches;
        private final AtomicBoolean closed;
        private DecodedListBinaryBatch current;
        private int loads;

        FakeListReader(AtomicBoolean closed, DecodedListBinaryBatch... batches) {
            this.closed = closed;
            this.batches = List.of(batches);
        }

        @Override
        public DecodedListBinaryBatch decodedListBinaryBatch() {
            return current;
        }

        @Override
        public void loadListBinaryBatchContaining(long row) {
            for (DecodedListBinaryBatch batch : batches) {
                if (batch.contains(row)) {
                    current = batch;
                    loads++;
                    return;
                }
            }
            throw new AssertionError("no batch holds row " + row);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    public void testEachDocumentsValuesComeSortedAndDistinct() throws IOException {
        SortedSetDocValues values = over(batch(0, words("red", "blue"), words("blue"), null, words("red", "red", "green")));
        assertValues(values, 0, "blue", "red");
        assertValues(values, 1, "blue");
        assertFalse("a null row carries no value", values.advanceExact(2));
        assertValues(values, 3, "green", "red");
    }

    /** Lucene orders terms by unsigned bytes, so an accented word sorts after every ASCII letter. */
    public void testValuesSortInUnsignedByteOrder() throws IOException {
        SortedSetDocValues values = over(batch(0, words("é", "z", "Z")));
        assertValues(values, 0, "Z", "z", "é");
    }

    public void testAnEmptyListRowCarriesNoValue() throws IOException {
        SortedSetDocValues values = over(batch(0, words(), words("red")));
        assertFalse(values.advanceExact(0));
        assertValues(values, 1, "red");
    }

    /** A document longer than any before it grows the copies; the earlier and later values stay intact. */
    public void testALongerDocumentGrowsTheCopiesWithoutCorruptingValues() throws IOException {
        String[] ten = { "j", "i", "h", "g", "f", "e", "d", "c", "b", "a" };
        SortedSetDocValues values = over(batch(0, words("red", "blue"), ten, words("green")));
        assertValues(values, 0, "blue", "red");
        assertValues(values, 1, "a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        assertValues(values, 2, "green");
    }

    public void testLoadsTheBatchContainingEachRequestedRow() throws IOException {
        FakeListReader reader = new FakeListReader(
            new AtomicBoolean(),
            batch(0, words("red", "blue"), words("blue")),
            batch(2, null, words("red", "red", "green"))
        );
        SortedSetDocValues values = new ParquetSortedSetDocValues(() -> reader, 4);
        assertValues(values, 0, "blue", "red");
        assertValues(values, 1, "blue");
        assertValues(values, 3, "green", "red");
        assertEquals("one load per batch, reused for every row inside it", 2, reader.loads);
    }

    public void testLookupOrdRefusesAnotherDocumentsOrdinal() throws IOException {
        SortedSetDocValues values = over(batch(0, words("red", "blue"), words("blue"), null, words("red", "red", "green")));
        assertTrue(values.advanceExact(0));
        long doc0Ordinal = values.nextOrd();
        assertTrue(values.advanceExact(3));
        expectThrows(UnsupportedOperationException.class, () -> values.lookupOrd(doc0Ordinal));
    }

    public void testGetValueCountAndLookupTermRefuseAndNameTheRemedy() {
        ParquetSortedSetDocValues values = withoutCursor(4);
        UnsupportedOperationException count = expectThrows(UnsupportedOperationException.class, values::getValueCount);
        assertTrue(count.getMessage(), count.getMessage().contains("execution_hint:map"));
        UnsupportedOperationException term = expectThrows(
            UnsupportedOperationException.class,
            () -> values.lookupTerm(new BytesRef("red"))
        );
        assertTrue(term.getMessage(), term.getMessage().contains("execution_hint:map"));
    }

    /** A target past the segment's last document is answered without opening a cursor. */
    public void testAdvanceExactBeyondMaxDocOpensNoCursor() throws IOException {
        ParquetSortedSetDocValues values = withoutCursor(4);
        assertFalse(values.advanceExact(4));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.docID());
    }

    public void testCursorIsClosedWhenIteratorBecomesUnreachable() throws Exception {
        AtomicBoolean closed = new AtomicBoolean(false);
        openCursorThenAbandonIterator(closed);
        assertBusy(() -> {
            System.gc();
            assertTrue("cursor must be closed once the iterator is unreachable", closed.get());
        });
    }

    private static void openCursorThenAbandonIterator(AtomicBoolean closed) throws IOException {
        ParquetSortedSetDocValues values = new ParquetSortedSetDocValues(() -> new FakeListReader(closed, batch(0, words("red"))), 1);
        assertTrue(values.advanceExact(0));
        assertFalse("cursor must stay open while the iterator is reachable", closed.get());
    }

    private static void assertValues(SortedSetDocValues values, int doc, String... expected) throws IOException {
        assertTrue("doc " + doc + " must have values", values.advanceExact(doc));
        assertEquals(expected.length, values.docValueCount());
        for (String word : expected) {
            assertEquals(new BytesRef(word), values.lookupOrd(values.nextOrd()));
        }
    }

    private static ParquetSortedSetDocValues over(DecodedListBinaryBatch batch) {
        return new ParquetSortedSetDocValues(() -> new FakeListReader(new AtomicBoolean(), batch), (int) (batch.lastRow() + 1));
    }

    /** An iterator whose reader factory fails the test if called, for branches that must not open a cursor. */
    private static ParquetSortedSetDocValues withoutCursor(int maxDoc) {
        return new ParquetSortedSetDocValues(() -> { throw new AssertionError("no cursor must be opened"); }, maxDoc);
    }

    private static String[] words(String... words) {
        return words;
    }

    /** Lays rows out as the native list export does: one value buffer, item boundaries, row boundaries. A null row has no items. */
    private static DecodedListBinaryBatch batch(long firstRow, String[]... rows) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        List<Integer> byteOffsets = new ArrayList<>(List.of(0));
        int[] rowOffsets = new int[rows.length + 1];
        for (int r = 0; r < rows.length; r++) {
            if (rows[r] != null) {
                for (String word : rows[r]) {
                    bytes.writeBytes(word.getBytes(StandardCharsets.UTF_8));
                    byteOffsets.add(bytes.size());
                }
            }
            rowOffsets[r + 1] = byteOffsets.size() - 1;
        }
        return new DecodedListBinaryBatch(
            firstRow,
            firstRow + rows.length - 1,
            MemorySegment.ofArray(bytes.toByteArray()),
            MemorySegment.ofArray(byteOffsets.stream().mapToInt(Integer::intValue).toArray()),
            MemorySegment.ofArray(rowOffsets)
        );
    }
}
