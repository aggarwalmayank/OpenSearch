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
import org.opensearch.be.datafusion.docvalues.BinaryColumnFixture;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;

import java.nio.file.Path;

/**
 * Drives {@link ParquetBinaryDocValues} over a real Parquet fixture read through the native cursor:
 * the ascending hot path, null handling, forward jumps, {@code nextDoc}, the backward
 * {@code advanceExact} that reopens the forward-only cursor, and scratch-buffer reuse.
 */
public class ParquetBinaryDocValuesTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "blob";
    private static final int ROWS = 300;
    private static final int NULL_EVERY = 5;

    public void testFullAscendingScanAllPresent() throws Exception {
        Path file = createTempDir().resolve("scan.parquet");
        BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, ROWS);
            for (int doc = 0; doc < ROWS; doc++) {
                assertTrue("row " + doc + " should be present", dv.advanceExact(doc));
                assertValue(doc, dv.binaryValue());
            }
            assertFalse("no doc at maxDoc", dv.advanceExact(ROWS));
        }
    }

    public void testNullRowsAreAbsent() throws Exception {
        Path file = createTempDir().resolve("nullable.parquet");
        BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, ROWS);
            for (int doc = 0; doc < ROWS; doc++) {
                boolean present = dv.advanceExact(doc);
                if (doc % NULL_EVERY == 0) {
                    assertFalse("row " + doc + " should be null", present);
                    assertNull("no value on a null row", dv.binaryValue());
                } else {
                    assertTrue("row " + doc + " should be present", present);
                    assertValue(doc, dv.binaryValue());
                }
            }
        }
    }

    public void testNextDocAndAdvanceSkipNulls() throws Exception {
        Path file = createTempDir().resolve("skip.parquet");
        BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, NULL_EVERY);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, ROWS);
            // Row 0 is null, so the first live doc is 1.
            assertEquals(1, dv.nextDoc());
            assertValue(1, dv.binaryValue());
            // advance onto a null row (200 % 5 == 0) lands on the next live doc, 201.
            assertEquals(201, dv.advance(200));
            assertValue(201, dv.binaryValue());
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(ROWS));
        }
    }

    public void testAdvanceOverAllNullColumnExhausts() throws Exception {
        Path file = createTempDir().resolve("allnull.parquet");
        BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, 1);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, ROWS);
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.advance(0));
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, dv.docID());
        }
    }

    public void testBackwardAdvanceExactReopensCursor() throws Exception {
        Path file = createTempDir().resolve("backward.parquet");
        BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, ROWS);
            assertTrue(dv.advanceExact(250));
            assertValue(250, dv.binaryValue());
            // A lower target than the current batch forces the forward-only cursor to reopen.
            assertTrue(dv.advanceExact(10));
            assertValue(10, dv.binaryValue());
        }
    }

    /** One {@link BytesRef} per iterator: each advance refills it, so a retained reference sees the new doc. */
    public void testValuesShareOneScratchBuffer() throws Exception {
        Path file = createTempDir().resolve("scratch.parquet");
        BinaryColumnFixture.write(file, allocator, COLUMN, ROWS, 0);

        try (ParquetColumnReader reader = ParquetColumnReader.open(file, COLUMN)) {
            ParquetBinaryDocValues dv = new ParquetBinaryDocValues(reader, ROWS);
            assertTrue(dv.advanceExact(4)); // 200 bytes: grows the scratch once
            BytesRef first = dv.binaryValue();
            assertTrue(dv.advanceExact(6)); // 3 bytes: fits, no growth
            BytesRef second = dv.binaryValue();
            assertSame("the iterator must hand out its single scratch view", first, second);
            assertValue(6, first);
        }
    }

    private static void assertValue(int doc, BytesRef actual) {
        assertEquals("value at row " + doc, new BytesRef(BinaryColumnFixture.valueAt(doc)), actual);
    }
}
