/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link BinaryFramingDocValues}: that a raw Parquet {@code BYTE_ARRAY} value is
 * presented in the framing an OpenSearch {@code binary} field's doc values consumers expect, and that
 * the framing decodes back to the original bytes.
 *
 * <p>The decode side mirrors {@code BinaryDocValuesFetcher} and {@code AbstractBinaryDVLeafFieldData},
 * which live in {@code server} and cannot be reached from here without a Lucene index. Keeping a local
 * decoder means these tests pin the wire format itself rather than one consumer's reading of it: if the
 * framing regresses, every binary read on the Parquet codec silently returns wrong bytes.
 */
public class BinaryFramingDocValuesTests extends OpenSearchTestCase {

    public void testSingleValueRoundTrip() throws IOException {
        byte[] value = new byte[] { 1, 2, 3, 4 };
        BinaryFramingDocValues framing = new BinaryFramingDocValues(new StubBinaryDocValues(new byte[][] { value }));

        assertTrue(framing.advanceExact(0));
        assertArrayEquals(new byte[][] { value }, decode(framing.binaryValue()));
    }

    /** The empty value is a real case: a zero-length BYTE_ARRAY frames as a length-0 value, not as absent. */
    public void testEmptyValueRoundTrip() throws IOException {
        BinaryFramingDocValues framing = new BinaryFramingDocValues(new StubBinaryDocValues(new byte[][] { new byte[0] }));

        assertTrue(framing.advanceExact(0));
        byte[][] decoded = decode(framing.binaryValue());
        assertEquals(1, decoded.length);
        assertEquals(0, decoded[0].length);
    }

    /**
     * A value longer than 127 bytes needs a two byte vInt length, so the header width is not fixed. This
     * is the case a fixed-width header would get wrong.
     */
    public void testMultiByteVIntLength() throws IOException {
        byte[] value = randomByteArrayOfLength(1000);
        BinaryFramingDocValues framing = new BinaryFramingDocValues(new StubBinaryDocValues(new byte[][] { value }));

        assertTrue(framing.advanceExact(0));
        BytesRef framed = framing.binaryValue();
        // vInt(1) is 1 byte, vInt(1000) is 2 bytes.
        assertEquals(3 + value.length, framed.length);
        assertArrayEquals(new byte[][] { value }, decode(framed));
    }

    /**
     * The framing buffer is reused across documents, so a long value followed by a short one must not
     * leave the previous value's tail visible. {@code framed.length} is what guards this.
     */
    public void testBufferReuseAcrossDocuments() throws IOException {
        byte[] longValue = randomByteArrayOfLength(500);
        byte[] shortValue = new byte[] { 7 };
        BinaryFramingDocValues framing = new BinaryFramingDocValues(
            new StubBinaryDocValues(new byte[][] { longValue, shortValue, longValue })
        );

        assertTrue(framing.advanceExact(0));
        assertArrayEquals(new byte[][] { longValue }, decode(framing.binaryValue()));

        assertTrue(framing.advanceExact(1));
        assertArrayEquals(new byte[][] { shortValue }, decode(framing.binaryValue()));

        assertTrue(framing.advanceExact(2));
        assertArrayEquals(new byte[][] { longValue }, decode(framing.binaryValue()));
    }

    /**
     * The delegate hands back a slice of a resident page, i.e. a non-zero offset into a shared array.
     * The framing has to honour that offset rather than reading from the start of the backing array.
     */
    public void testHonoursDelegateOffset() throws IOException {
        byte[] backing = new byte[] { 9, 9, 9, 1, 2, 3, 9, 9 };
        BinaryDocValues delegate = new BinaryDocValues() {
            private int doc = -1;

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public BytesRef binaryValue() {
                return new BytesRef(backing, 3, 3);
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return ++doc;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc;
            }

            @Override
            public long cost() {
                return 1;
            }
        };

        BinaryFramingDocValues framing = new BinaryFramingDocValues(delegate);
        assertTrue(framing.advanceExact(0));
        assertArrayEquals(new byte[][] { new byte[] { 1, 2, 3 } }, decode(framing.binaryValue()));
    }

    /** An absent value must not be framed, and must not leave the previous document's framing behind. */
    public void testAbsentValueNotFramed() throws IOException {
        StubBinaryDocValues stub = new StubBinaryDocValues(new byte[][] { new byte[] { 1 }, null });
        BinaryFramingDocValues framing = new BinaryFramingDocValues(stub);

        assertTrue(framing.advanceExact(0));
        assertFalse(framing.advanceExact(1));
    }

    /** {@code nextDoc} and {@code advance} frame the value too, not just {@code advanceExact}. */
    public void testIterationFramesValues() throws IOException {
        byte[] first = new byte[] { 1, 2 };
        byte[] second = new byte[] { 3, 4, 5 };
        BinaryFramingDocValues framing = new BinaryFramingDocValues(new StubBinaryDocValues(new byte[][] { first, second }));

        assertEquals(0, framing.nextDoc());
        assertArrayEquals(new byte[][] { first }, decode(framing.binaryValue()));

        assertEquals(1, framing.advance(1));
        assertArrayEquals(new byte[][] { second }, decode(framing.binaryValue()));

        assertEquals(BinaryDocValues.NO_MORE_DOCS, framing.nextDoc());
    }

    public void testDelegatesDocIdAndCost() throws IOException {
        StubBinaryDocValues stub = new StubBinaryDocValues(new byte[][] { new byte[] { 1 } });
        BinaryFramingDocValues framing = new BinaryFramingDocValues(stub);

        assertEquals(-1, framing.docID());
        assertTrue(framing.advanceExact(0));
        assertEquals(0, framing.docID());
        assertEquals(stub.cost(), framing.cost());
    }

    /**
     * Decodes the {@code vInt} value count followed by a {@code vInt} length and the raw bytes of each
     * value, which is what {@code BinaryFieldMapper.CustomBinaryDocValuesField#binaryValue()} writes.
     */
    private static byte[][] decode(BytesRef framed) throws IOException {
        BytesStreamInput in = new BytesStreamInput();
        in.reset(framed.bytes, framed.offset, framed.length);
        int count = in.readVInt();
        List<byte[]> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int length = in.readVInt();
            byte[] value = new byte[length];
            in.readBytes(value, 0, length);
            values.add(value);
        }
        assertEquals("framed value has trailing bytes", 0, in.available());
        return values.toArray(new byte[0][]);
    }

    /**
     * Serves one value per doc from a fixed array, repointing a single reused {@link BytesRef} the way
     * {@link ParquetBinaryDocValues} does. A {@code null} entry means the document has no value.
     */
    private static final class StubBinaryDocValues extends BinaryDocValues {

        private final byte[][] values;
        private final BytesRef scratch = new BytesRef();
        private int doc = -1;

        StubBinaryDocValues(byte[][] values) {
            this.values = values;
        }

        @Override
        public boolean advanceExact(int target) {
            doc = target;
            if (target >= values.length || values[target] == null) {
                return false;
            }
            point(target);
            return true;
        }

        @Override
        public BytesRef binaryValue() {
            return scratch;
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
            if (target >= values.length) {
                doc = NO_MORE_DOCS;
                return doc;
            }
            doc = target;
            point(target);
            return doc;
        }

        @Override
        public long cost() {
            return values.length;
        }

        private void point(int target) {
            scratch.bytes = values[target];
            scratch.offset = 0;
            scratch.length = values[target].length;
        }
    }
}
