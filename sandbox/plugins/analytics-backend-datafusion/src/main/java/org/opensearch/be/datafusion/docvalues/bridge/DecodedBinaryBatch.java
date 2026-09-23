/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * One copied-out batch of a variable-width (BYTE_ARRAY) column: values packed back to back, a
 * fence-post offsets array ({@code rows + 1} ints; row i's bytes span posts i..i+1), and a presence
 * bitmap (one bit per row, packed into longs from bit 0).
 *
 * <p>Unlike {@link DecodedBatch}, whose segments borrow Arrow-owned memory, these segments are
 * Java-owned copies: the native batch is released before the load returns, and the views stay valid
 * until the reader's next load, not just its next native call.
 */
public record DecodedBinaryBatch(long firstRow, long lastRow, MemorySegment valueBytes, MemorySegment byteOffsets,
    MemorySegment presenceBits) {

    /** True when the given global row falls within this batch's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }

    /** Constant-time presence test for a global row, which must fall within {@code [firstRow, lastRow]}. */
    public boolean isPresent(long row) {
        if (contains(row) == false) {
            throw new IndexOutOfBoundsException("row " + row + " outside batch [" + firstRow + ", " + lastRow + "]");
        }
        long idx = row - firstRow;
        // Word-granular read, matching the word-granular native writer; bit i of the batch is
        // bit (i % 64) of word (i / 64).
        long word = presenceBits.getAtIndex(ValueLayout.JAVA_LONG, idx >>> 6);
        return (word & (1L << (idx & 63))) != 0;
    }

    /** Byte length of the value at the given global row; zero for an absent row. */
    public int valueLength(long row) {
        long idx = row - firstRow;
        return byteOffsets.getAtIndex(ValueLayout.JAVA_INT, idx + 1) - byteOffsets.getAtIndex(ValueLayout.JAVA_INT, idx);
    }

    /** Byte position where the given global row's value starts inside {@link #valueBytes}. */
    public int valueOffset(long row) {
        return byteOffsets.getAtIndex(ValueLayout.JAVA_INT, row - firstRow);
    }

    /**
     * Copies the value at the given global row into {@code dest} at offset 0 and returns its length.
     * {@code dest} must be at least {@link #valueLength} long; the caller sizes and reuses it across rows.
     */
    public int copyValue(long row, byte[] dest) {
        int offset = valueOffset(row);
        int length = valueLength(row);
        MemorySegment.copy(valueBytes, offset, MemorySegment.ofArray(dest), 0, length);
        return length;
    }
}
