/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

/**
 * In-memory, binary-searchable view of a column's Parquet OffsetIndex and ColumnIndex, built once per
 * column from the parallel arrays the native page-index export fills.
 *
 * <p>Carries only what that export supplies: per-page first row, row count, null count, minimum, and
 * maximum. File offset and compressed size are not exported and so are absent. Minimum and maximum are
 * raw signed-long bits; {@link Long#MIN_VALUE}/{@link Long#MAX_VALUE} mean unknown, which widens the
 * page range so a range filter never wrongly excludes it. A null count of {@code -1} means unknown.
 */
public final class ColumnPageIndex {

    private final long[] firstRowOf;
    private final long[] rowCountOf;
    private final long[] nullCountOf;
    private final long[] minOf;
    private final long[] maxOf;
    private final long totalRows;

    public ColumnPageIndex(long[] firstRowOf, long[] rowCountOf, long[] nullCountOf, long[] minOf, long[] maxOf, long totalRows) {
        int n = firstRowOf.length;
        if (rowCountOf.length != n || nullCountOf.length != n || minOf.length != n || maxOf.length != n) {
            throw new IllegalArgumentException("ColumnPageIndex parallel arrays must have equal length");
        }
        this.firstRowOf = firstRowOf;
        this.rowCountOf = rowCountOf;
        this.nullCountOf = nullCountOf;
        this.minOf = minOf;
        this.maxOf = maxOf;
        this.totalRows = totalRows;
    }

    /** Number of pages. */
    public int pageCount() {
        return firstRowOf.length;
    }

    /** Global index of the first row of page {@code p}. */
    public long firstRowOf(int p) {
        return firstRowOf[p];
    }

    /** Number of rows in page {@code p}. */
    public long numRowsOf(int p) {
        return rowCountOf[p];
    }

    /** Null count of page {@code p}, or {@code -1} when unknown. */
    public long nullCountOf(int p) {
        return nullCountOf[p];
    }

    /** Raw signed-long minimum of page {@code p}; {@link Long#MIN_VALUE} when unknown. */
    public long minOf(int p) {
        return minOf[p];
    }

    /** Raw signed-long maximum of page {@code p}; {@link Long#MAX_VALUE} when unknown. */
    public long maxOf(int p) {
        return maxOf[p];
    }

    /** Total number of rows across all pages. */
    public long totalRows() {
        return totalRows;
    }

    /** Page index containing global row {@code row}, or {@code -1} when {@code row} is out of range. */
    public int pageForRow(long row) {
        if (row < 0 || row >= totalRows) {
            return -1;
        }
        int lo = 0;
        int hi = firstRowOf.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (firstRowOf[mid] <= row) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi; // last page whose first row is <= row
    }

    /** Unknown null counts ({@code -1}) return false, so an undecoded page is never treated as all-null. */
    public boolean isAllNulls(int p) {
        long nc = nullCountOf[p];
        return nc >= 0 && nc == rowCountOf[p];
    }
}
