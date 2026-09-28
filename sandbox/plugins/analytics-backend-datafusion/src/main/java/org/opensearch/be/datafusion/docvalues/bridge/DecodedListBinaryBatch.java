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
 * One copied-out batch of a repeated variable-width column. Row {@code r} owns items
 * {@code rowOffsets[r]..rowOffsets[r + 1]}, and item {@code i} owns bytes
 * {@code byteOffsets[i]..byteOffsets[i + 1]} of {@code valueBytes}. The segments are Java-owned
 * copies, valid until the reader's next load.
 *
 * @param firstRow    inclusive global index of the first row in the batch
 * @param lastRow     inclusive global index of the last row in the batch
 * @param valueBytes  every item's bytes, back to back
 * @param byteOffsets item boundaries; holds item count + 1 ints
 * @param rowOffsets  row boundaries as item numbers; holds {@code lastRow - firstRow + 2} ints
 */
public record DecodedListBinaryBatch(long firstRow, long lastRow, MemorySegment valueBytes, MemorySegment byteOffsets,
    MemorySegment rowOffsets) {

    /** True when the given global row falls within this batch's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }

    /** First item number of {@code row}'s list. The caller must have accepted {@code row} via {@link #contains}. */
    public int startItem(long row) {
        return rowOffsets.getAtIndex(ValueLayout.JAVA_INT, row - firstRow);
    }

    /** One past the last item number of {@code row}'s list; equals {@link #startItem} for a row with no values. */
    public int endItem(long row) {
        return rowOffsets.getAtIndex(ValueLayout.JAVA_INT, row - firstRow + 1);
    }

    /** Byte length of the given item. */
    public int itemLength(int item) {
        return byteOffsets.getAtIndex(ValueLayout.JAVA_INT, item + 1) - byteOffsets.getAtIndex(ValueLayout.JAVA_INT, item);
    }

    /**
     * Copies the given item's bytes into {@code dest} at offset 0 and returns its length.
     * {@code dest} must be at least {@link #itemLength} long.
     */
    public int copyItem(int item, byte[] dest) {
        int offset = byteOffsets.getAtIndex(ValueLayout.JAVA_INT, item);
        int length = itemLength(item);
        MemorySegment.copy(valueBytes, offset, MemorySegment.ofArray(dest), 0, length);
        return length;
    }
}
