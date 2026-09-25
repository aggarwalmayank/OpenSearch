/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

/**
 * An in-memory, binary-searchable view of a column's per-page Parquet metadata: the OffsetIndex
 * page boundaries and the ColumnIndex per-page min/max/null-count, as loaded once per
 * {@code (segment, column)} from the native {@code parquet_df_column_page_index} call.
 *
 * <p>Parallel arrays are indexed by page. {@code firstRowOf} is ascending with
 * {@code firstRowOf[0] == 0}; the number of rows in page {@code p} is
 * {@code firstRowOf[p+1] - firstRowOf[p]} (or {@code totalRows - firstRowOf[p]} for the last page).
 * Pages whose stats are unknown carry the sentinel ({@code Long.MIN_VALUE}, {@code Long.MAX_VALUE})
 * from the native load, so a consumer making skip decisions never wrongly excludes them.
 *
 * <p>Backs {@link ParquetDocValuesSkipper}; this holder carries the per-page fields the skipper
 * reads (row boundaries, null count, min, max) and precomputes the column-wide global min, max, and
 * doc count in its constructor so skipper construction is O(1).
 */
public final class ColumnPageIndex {

    private final long[] firstRowOf;
    private final long[] nullCountOf;
    private final long[] minOf;
    private final long[] maxOf;
    private final long totalRows;
    private final long globalMin;
    private final long globalMax;
    private final long globalDocCount;

    public ColumnPageIndex(long[] firstRowOf, long[] nullCountOf, long[] minOf, long[] maxOf, long totalRows) {
        int n = firstRowOf.length;
        if (nullCountOf.length != n || minOf.length != n || maxOf.length != n) {
            throw new IllegalArgumentException("ColumnPageIndex parallel arrays must have equal length");
        }
        this.firstRowOf = firstRowOf;
        this.nullCountOf = nullCountOf;
        this.minOf = minOf;
        this.maxOf = maxOf;
        this.totalRows = totalRows;

        // Single pass over pages to precompute the column-wide global stats.
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long docCount = 0;
        for (int p = 0; p < n; p++) {
            min = Math.min(min, minOf[p]);
            max = Math.max(max, maxOf[p]);
            docCount += docCountOf(p);
        }
        // With no pages there is nothing to bound: use the widest range so a consumer never wrongly
        // excludes this column, and report zero docs.
        this.globalMin = n == 0 ? Long.MIN_VALUE : min;
        this.globalMax = n == 0 ? Long.MAX_VALUE : max;
        this.globalDocCount = docCount;
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
        long next = (p + 1 < firstRowOf.length) ? firstRowOf[p + 1] : totalRows;
        return next - firstRowOf[p];
    }

    /** Null count of page {@code p}, or -1 when unknown. */
    public long nullCountOf(int p) {
        return nullCountOf[p];
    }

    /**
     * Documents with a value in page {@code p}. When the page's null count is unknown (-1) this
     * UNDER-claims (0): consumers use docCount for density checks (all-docs-have-values fast paths),
     * where overclaiming would produce wrong results and underclaiming merely disables an
     * optimization.
     */
    public long docCountOf(int p) {
        long nulls = nullCountOf[p];
        return nulls < 0 ? 0 : numRowsOf(p) - nulls;
    }

    /** Per-page min value raw bits. */
    public long minOf(int p) {
        return minOf[p];
    }

    /** Per-page max value raw bits. */
    public long maxOf(int p) {
        return maxOf[p];
    }

    /** Total number of rows across all pages. */
    public long totalRows() {
        return totalRows;
    }

    /** Column-wide minimum: min of every page's min, or {@code Long.MIN_VALUE} when there are no pages. */
    public long globalMin() {
        return globalMin;
    }

    /** Column-wide maximum: max of every page's max, or {@code Long.MAX_VALUE} when there are no pages. */
    public long globalMax() {
        return globalMax;
    }

    /** Column-wide doc count: sum of every page's {@link #docCountOf(int)}. */
    public long globalDocCount() {
        return globalDocCount;
    }

    /**
     * Binary search: returns the page index containing global row {@code row}, or -1 when
     * {@code row} is out of range. O(log P).
     */
    public int pageForRow(long row) {
        if (row < 0 || row >= totalRows) {
            return -1;
        }
        int lo = 0;
        int hi = firstRowOf.length - 1;
        // Find the highest page whose firstRow <= row.
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (firstRowOf[mid] <= row) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return hi; // hi is the last page with firstRow <= row
    }
}
