/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * One decoded batch of a Parquet column, read in place.
 *
 * <p>Holds the inclusive global row range {@code [firstRow, lastRow]} and off-heap views of the
 * decoded values and the packed presence bitset. Both views are borrowed Arrow buffers, read with
 * no on-heap copy; they are valid only until the next batch call on the owning cursor, which always
 * replaces this batch first. {@link #valueKind} selects the width and sign of the per-row read in
 * {@link #valueAt}, or the offsets-based byte read in {@link #bytesAt} for {@link #KIND_BINARY}.
 *
 * @param firstRow          inclusive global index of the first row in the batch
 * @param lastRow           inclusive global index of the last row in the batch
 * @param values            off-heap view of the decoded values, interpreted according to {@link #valueKind}
 * @param valueKind         element interpretation of {@code values}; one of the {@code KIND_*} constants
 * @param valueBitOffset    first value bit of this batch within {@code values}, used only by the bit-packed
 *                          {@link #KIND_BOOL}; zero for the byte-addressed kinds, which fold the offset into the address
 * @param offsets           off-heap view of the i32 offsets buffer for the variable-width {@link #KIND_BINARY}
 *                          (row {@code firstRow + i}'s bytes span {@code values[offsets[i] .. offsets[i+1]]}); {@code null}
 *                          for every fixed-width and bit-packed kind, whose values live wholly in {@code values}
 * @param presenceBits      off-heap view of the packed presence bitset (bit {@code presenceBitOffset + i}
 *                          is set when row {@code firstRow + i} is non-null); {@code null} means every row is present
 * @param presenceBitOffset first presence bit of this batch within {@code presenceBits} (borrowed bitmaps are bit-sliced)
 */
public record DecodedBatch(long firstRow, long lastRow, MemorySegment values, int valueKind, int valueBitOffset, MemorySegment offsets,
    MemorySegment presenceBits, int presenceBitOffset) {

    /** {@link #values} holds one {@code long} of raw bits per row (i64/u64 bits). */
    public static final int KIND_LONG = 1;
    /** {@link #values} holds one sign-extending {@code int} per row. */
    public static final int KIND_INT = 2;
    /** {@link #values} holds one zero-extending {@code int} per row (u32 bits). */
    public static final int KIND_UINT_BITS = 3;
    /** {@link #values} holds one sign-extending {@code short} per row. */
    public static final int KIND_SHORT = 4;
    /** {@link #values} holds one zero-extending {@code short} per row. */
    public static final int KIND_USHORT = 5;
    /** {@link #values} holds one sign-extending {@code byte} per row. */
    public static final int KIND_BYTE = 6;
    /** {@link #values} holds one zero-extending {@code byte} per row. */
    public static final int KIND_UBYTE = 7;
    /** {@link #values} holds one {@code long} of raw f64 bits per row; re-encoded to a Lucene sortable long. */
    public static final int KIND_DOUBLE = 8;
    /** {@link #values} holds one {@code int} of raw f32 bits per row; re-encoded to a sign-extended sortable int. */
    public static final int KIND_FLOAT = 9;
    /**
     * {@link #values} holds one <em>bit</em> per row rather than a whole byte, addressed from
     * {@link #valueBitOffset} exactly as the presence bitmap is addressed from {@code presenceBitOffset}.
     * Yields 0 or 1, the form OpenSearch's boolean field stores in doc values.
     */
    public static final int KIND_BOOL = 10;
    /**
     * {@link #values} holds one {@code short} of raw IEEE-754 half-precision bits per row, re-encoded to
     * the sortable short {@code HalfFloatPoint.halfFloatToSortableShort} produces - the exact form
     * OpenSearch's half_float field stores in doc values.
     */
    public static final int KIND_HALF_FLOAT = 11;
    /**
     * {@link #values} holds the concatenated bytes of every row and {@link #offsets} holds one i32 per row
     * boundary, so row {@code firstRow + i}'s bytes are {@code values[offsets[i] .. offsets[i+1]]}. Read
     * with {@link #bytesAt} rather than {@link #valueAt}.
     */
    public static final int KIND_BINARY = 12;

    /**
     * Constant-time presence test for a global row, which must fall within
     * {@code [firstRow, lastRow]}. Reads a single byte: the bitmap is only guaranteed to be
     * byte-addressable, so a wider read could reach past its last significant byte.
     */
    public boolean isPresent(long row) {
        if (contains(row) == false) {
            throw new IndexOutOfBoundsException("row " + row + " outside batch [" + firstRow + ", " + lastRow + "]");
        }
        if (presenceBits == null) {
            return true;
        }
        long idx = row - firstRow + presenceBitOffset;
        byte bits = presenceBits.get(ValueLayout.JAVA_BYTE, idx >>> 3);
        return (bits & (1 << (idx & 7))) != 0;
    }

    /**
     * Returns the value at the given global row, which the caller must already have accepted through
     * {@link #contains} or {@link #isPresent} - unlike those, this does not range-check, to avoid
     * repeating the check on every value read. An out-of-range row still cannot read past the
     * borrowed buffer: the values segment carries its length, so the access throws instead.
     *
     * <p>The value is returned as a Lucene numeric doc-values {@code long}. Integral
     * kinds sign- or zero-extend the stored width. The float/double kinds re-encode the raw IEEE-754
     * bits into Lucene's order-preserving "sortable" form ({@code doubleToSortableLong} /
     * {@code floatToSortableInt}, the latter sign-extended) - the encoding OpenSearch's float/double
     * fielddata reverses with {@code sortableLongToDouble} / {@code sortableIntToFloat}.
     */
    public long valueAt(long row) {
        long idx = row - firstRow;
        return switch (valueKind) {
            case KIND_LONG -> values.getAtIndex(ValueLayout.JAVA_LONG, idx);
            case KIND_INT -> values.getAtIndex(ValueLayout.JAVA_INT, idx);
            case KIND_UINT_BITS -> Integer.toUnsignedLong(values.getAtIndex(ValueLayout.JAVA_INT, idx));
            case KIND_SHORT -> values.getAtIndex(ValueLayout.JAVA_SHORT, idx);
            case KIND_USHORT -> Short.toUnsignedLong(values.getAtIndex(ValueLayout.JAVA_SHORT, idx));
            case KIND_BYTE -> values.get(ValueLayout.JAVA_BYTE, idx);
            case KIND_UBYTE -> Byte.toUnsignedLong(values.get(ValueLayout.JAVA_BYTE, idx));
            case KIND_DOUBLE -> {
                long bits = values.getAtIndex(ValueLayout.JAVA_LONG, idx);
                yield bits ^ ((bits >> 63) & 0x7fffffffffffffffL);
            }
            case KIND_FLOAT -> {
                int bits = values.getAtIndex(ValueLayout.JAVA_INT, idx);
                yield (long) (bits ^ ((bits >> 31) & 0x7fffffff));
            }
            case KIND_BOOL -> {
                // Bit-packed: read the containing byte and mask; a wider read could reach past the buffer's last byte.
                long bit = idx + valueBitOffset;
                byte bits = values.get(ValueLayout.JAVA_BYTE, bit >>> 3);
                yield (bits & (1 << (bit & 7))) != 0 ? 1L : 0L;
            }
            case KIND_HALF_FLOAT -> {
                // fp16-width sign-flip that produces the sortable short HalfFloatPoint stores.
                short bits = values.getAtIndex(ValueLayout.JAVA_SHORT, idx);
                yield (short) (bits ^ ((bits >> 15) & 0x7fff));
            }
            default -> throw new IllegalStateException("unknown value kind " + valueKind);
        };
    }

    /**
     * Copies the bytes stored at the given global row into {@code dest}, which the caller must already
     * have accepted through {@link #contains} or {@link #isPresent}. Only valid for {@link #KIND_BINARY};
     * the numeric kinds have no byte value and must be read with {@link #valueAt}.
     *
     * <p>Row {@code i = row - firstRow}'s bytes are {@code values[offsets[i] .. offsets[i+1]]}. The two
     * i32 offsets are read from {@link #offsets} and the span copied out of {@link #values} into
     * {@code dest.bytes}, grown when too small, since the borrowed off-heap buffer is valid only until
     * the next batch call. {@code dest.offset} is set to 0 and {@code dest.length} to the row's length,
     * so a caller reusing one {@code dest} across rows allocates only when a row is longer than any
     * seen before.
     */
    public void bytesAt(long row, BytesRef dest) {
        if (valueKind != KIND_BINARY) {
            throw new IllegalStateException("bytesAt is only valid for KIND_BINARY, not value kind " + valueKind);
        }
        long i = row - firstRow;
        // offsets[i], offsets[i+1]: i32 each. offsets carries n+1 entries for n rows, so i+1 is in range.
        int start = offsets.getAtIndex(ValueLayout.JAVA_INT, i);
        int end = offsets.getAtIndex(ValueLayout.JAVA_INT, i + 1);
        int length = end - start;
        if (length < 0) {
            throw new IllegalStateException(
                "negative binary length " + length + " at row " + row + " (offsets " + start + ".." + end + ")"
            );
        }
        if (dest.bytes.length < length) {
            dest.bytes = ArrayUtil.grow(dest.bytes, length);
        }
        MemorySegment.copy(values, ValueLayout.JAVA_BYTE, start, dest.bytes, 0, length);
        dest.offset = 0;
        dest.length = length;
    }

    /** True when the given global row falls within this batch's range. */
    public boolean contains(long row) {
        return row >= firstRow && row <= lastRow;
    }

    /**
     * Returns the first present row at or after {@code fromRow} within this batch, or {@code -1}
     * when no row from {@code fromRow} to {@link #lastRow} is present. {@code fromRow} must fall
     * within {@code [firstRow, lastRow]}.
     *
     * <p>A batch with no presence bitmap is fully dense, so {@code fromRow} itself is the answer.
     * Otherwise the bitmap is scanned a byte at a time, eight rows per read, skipping all-null
     * bytes without testing their bits individually. Reads stay byte-wide because the borrowed
     * bitmap is only guaranteed byte-addressable (see {@link #isPresent}).
     */
    public long nextPresentRow(long fromRow) {
        if (contains(fromRow) == false) {
            throw new IndexOutOfBoundsException("row " + fromRow + " outside batch [" + firstRow + ", " + lastRow + "]");
        }
        if (presenceBits == null) {
            return fromRow;
        }
        final long lastBit = lastRow - firstRow + presenceBitOffset;
        long bit = fromRow - firstRow + presenceBitOffset;
        // First byte: mask off bits below the starting row so an earlier present row is not reported.
        int bits = (presenceBits.get(ValueLayout.JAVA_BYTE, bit >>> 3) & 0xFF) & (0xFF << (bit & 7));
        for (long byteIdx = bit >>> 3;;) {
            if (bits != 0) {
                long foundBit = (byteIdx << 3) + Integer.numberOfTrailingZeros(bits);
                if (foundBit > lastBit) {
                    return -1;
                }
                return firstRow + (foundBit - presenceBitOffset);
            }
            byteIdx++;
            if ((byteIdx << 3) > lastBit) {
                return -1;
            }
            bits = presenceBits.get(ValueLayout.JAVA_BYTE, byteIdx) & 0xFF;
        }
    }
}
