/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.iter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * {@link BinaryFramingDocValues} must hand OpenSearch's {@code binary} field consumers exactly the header
 * {@code BinaryFieldMapper.CustomBinaryDocValuesField} writes: {@code vInt(count) vInt(length) bytes}. The
 * decisive tests decode through the real consumer ({@link BytesBinaryIndexFieldData}) rather than a
 * re-implementation of it.
 */
public class BinaryFramingDocValuesTests extends OpenSearchTestCase {

    /** A single-valued binary iterator over in-memory rows: the shape {@link ParquetBinaryDocValues} presents. */
    private static final class ArrayBinaryDocValues extends BinaryDocValues {
        private final byte[][] rows; // null = absent
        private int doc = -1;
        /** Counts value reads, so the tests can assert framing is lazy. */
        int valueReads;

        ArrayBinaryDocValues(byte[]... rows) {
            this.rows = rows;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            return rows[target] != null;
        }

        @Override
        public BytesRef binaryValue() {
            valueReads++;
            return new BytesRef(rows[doc]);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            for (int d = target; d < rows.length; d++) {
                if (rows[d] != null) {
                    return doc = d;
                }
            }
            return doc = NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return rows.length;
        }
    }

    public void testFramesAValueAsCountOneThenLengthThenBytes() throws IOException {
        byte[] value = { 0x0A, 0x0B, 0x0C };
        BinaryFramingDocValues framed = new BinaryFramingDocValues(new ArrayBinaryDocValues(value));

        assertTrue(framed.advanceExact(0));
        BytesRef out = framed.binaryValue();
        // vInt(1) = 0x01, vInt(3) = 0x03, then the three value bytes.
        assertArrayEquals(new byte[] { 0x01, 0x03, 0x0A, 0x0B, 0x0C }, Arrays.copyOfRange(out.bytes, out.offset, out.offset + out.length));
    }

    public void testFramesAnEmptyValueAndAValueWhoseLengthNeedsAMultiByteVInt() throws IOException {
        byte[] empty = new byte[0];
        byte[] wide = new byte[300]; // 300 = 0b1_0010_1100 -> vInt bytes 0xAC 0x02
        Arrays.fill(wide, (byte) 0x7F);
        BinaryFramingDocValues framed = new BinaryFramingDocValues(new ArrayBinaryDocValues(empty, wide));

        assertTrue(framed.advanceExact(0));
        BytesRef out = framed.binaryValue();
        assertArrayEquals(new byte[] { 0x01, 0x00 }, Arrays.copyOfRange(out.bytes, out.offset, out.offset + out.length));

        assertTrue(framed.advanceExact(1));
        out = framed.binaryValue();
        assertEquals(1 + 2 + 300, out.length);
        assertEquals(0x01, out.bytes[out.offset]);
        assertEquals((byte) 0xAC, out.bytes[out.offset + 1]);
        assertEquals((byte) 0x02, out.bytes[out.offset + 2]);
        assertArrayEquals(wide, Arrays.copyOfRange(out.bytes, out.offset + 3, out.offset + out.length));
    }

    /** Presence-only consumers (exists queries) never read the value, so advancing must not decode it. */
    public void testFramingIsLazyAndInvalidatedPerDocument() throws IOException {
        ArrayBinaryDocValues in = new ArrayBinaryDocValues(new byte[] { 1 }, new byte[] { 2, 2 });
        BinaryFramingDocValues framed = new BinaryFramingDocValues(in);

        assertTrue(framed.advanceExact(0));
        assertEquals("advance alone must not read the value", 0, in.valueReads);
        framed.binaryValue();
        framed.binaryValue();
        assertEquals("repeated reads of one doc reuse the frame", 1, in.valueReads);

        assertTrue(framed.advanceExact(1));
        BytesRef out = framed.binaryValue();
        assertEquals("moving to a new doc rebuilds the frame", 2, in.valueReads);
        assertArrayEquals(new byte[] { 0x01, 0x02, 2, 2 }, Arrays.copyOfRange(out.bytes, out.offset, out.offset + out.length));
    }

    public void testAbsentDocumentsAndIterationDelegateToTheRawIterator() throws IOException {
        BinaryFramingDocValues framed = new BinaryFramingDocValues(new ArrayBinaryDocValues(new byte[] { 1 }, null, new byte[] { 3 }));

        assertEquals(0, framed.nextDoc());
        assertEquals(0, framed.docID());
        assertFalse("doc 1 holds no value", framed.advanceExact(1));
        assertEquals("advance skips the absent doc", 2, framed.advance(1));
        assertEquals(2, framed.docID());
        assertArrayEquals(new byte[] { 0x01, 0x01, 3 }, toBytes(framed.binaryValue()));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, framed.nextDoc());
        assertEquals(3, framed.cost());
    }

    private static byte[] toBytes(BytesRef ref) {
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }

    /**
     * The contract test: the real {@code binary} field consumer reads the framed value back as one
     * value holding exactly the stored bytes. Without the wrapper, the same consumer would read the
     * first stored byte as the value count and the following bytes as a length, and return garbage or
     * fail on the raw column bytes.
     */
    public void testTheVanillaBinaryFieldConsumerDecodesTheFrame() throws Exception {
        byte[] stored = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };
        String field = "blob";

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.NO));
                writer.addDocument(doc);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                // Stand-in for ParquetDocValuesLeafReader: the segment holds no doc values for the
                // field, so the overlay's answer is the framed Parquet-backed iterator.
                LeafReader overlay = new FilterLeafReader(leaf) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String name) throws IOException {
                        if (field.equals(name)) {
                            return new BinaryFramingDocValues(new ArrayBinaryDocValues(stored));
                        }
                        return super.getBinaryDocValues(name);
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return null;
                    }
                };
                LeafReaderContext context = overlay.getContext();

                LeafFieldData fieldData = new BytesBinaryIndexFieldData(field, CoreValuesSourceType.BYTES).load(context);
                SortedBinaryDocValues values = fieldData.getBytesValues();

                assertTrue(values.advanceExact(0));
                assertEquals("a single-valued field frames exactly one value", 1, values.docValueCount());
                BytesRef decoded = values.nextValue();
                assertArrayEquals(
                    "the consumer must recover the stored bytes unchanged",
                    stored,
                    Arrays.copyOfRange(decoded.bytes, decoded.offset, decoded.offset + decoded.length)
                );
            }
        }
    }

    /** The unframed iterator handed to the same consumer is misread: this is the defect the wrapper closes. */
    public void testTheVanillaBinaryFieldConsumerMisreadsAnUnframedValue() throws Exception {
        // Stored bytes whose first byte is 0x02: the consumer parses it as "two values", which the raw
        // column never meant. (A framed read of the same bytes reports one value, above.)
        byte[] stored = { 0x02, 0x05, 0x01 };
        BinaryDocValues raw = new ArrayBinaryDocValues(stored);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                writer.addDocument(new Document());
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader overlay = new FilterLeafReader(reader.leaves().get(0).reader()) {
                    @Override
                    public BinaryDocValues getBinaryDocValues(String name) {
                        return raw;
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return null;
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return null;
                    }
                };
                LeafFieldData fieldData = new BytesBinaryIndexFieldData("blob", CoreValuesSourceType.BYTES).load(overlay.getContext());
                SortedBinaryDocValues values = fieldData.getBytesValues();
                assertTrue(values.advanceExact(0));
                assertEquals("the raw first byte is misread as the value count", 2, values.docValueCount());
            }
        }
    }
}
