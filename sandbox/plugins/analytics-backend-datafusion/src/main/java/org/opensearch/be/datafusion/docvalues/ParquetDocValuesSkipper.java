/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * {@link DocValuesSkipper} backed by the Parquet ColumnIndex (per-page min/max/null-count), exposed
 * through the already-loaded {@link ColumnPageIndex}.
 *
 * <p>This lets Lucene's range machinery avoid requesting excluded pages at all — zero decode, zero
 * FFM, zero iteration for any page whose {@code [min, max]} does not intersect the query range.
 *
 * <p>Single level: level 0 intervals are Parquet pages. The Row ID = Doc ID invariant (the Parquet
 * doc-values read path asserts it) makes page row ranges directly usable as doc ID ranges. Pages
 * with unknown min/max carry the sentinel ({@code Long.MIN_VALUE}, {@code Long.MAX_VALUE}) from the
 * native page-index load, so they intersect every query range and are never wrongly skipped.
 *
 * <p>Only served for integer-shaped columns (long/int/date/boolean): their raw-bits value order
 * matches numeric order. Float/double doc values are raw IEEE-754 bits whose order diverges for
 * negatives, so the producer declines to build a skipper for them (see
 * {@link ParquetDocValuesProducer#getSkipper} and {@link FieldTypeMapping#isRangeSkippable}).
 */
final class ParquetDocValuesSkipper extends DocValuesSkipper {

    private final ColumnPageIndex pageIndex;
    private final int maxDoc;
    private final long globalMin;
    private final long globalMax;
    private final int globalDocCount;

    /** Current page index, -1 before the first advance, pageCount when exhausted. */
    private int page = -1;

    ParquetDocValuesSkipper(ColumnPageIndex pageIndex, int maxDoc) {
        this.pageIndex = pageIndex;
        this.maxDoc = maxDoc;
        // Global stats are precomputed by ColumnPageIndex, so construction is O(1).
        this.globalMin = pageIndex.globalMin();
        this.globalMax = pageIndex.globalMax();
        this.globalDocCount = (int) pageIndex.globalDocCount();
    }

    @Override
    public void advance(int target) {
        if (target >= maxDoc) {
            page = pageIndex.pageCount();
        } else {
            page = pageIndex.pageForRow(target);
        }
    }

    private boolean exhausted() {
        return page >= pageIndex.pageCount();
    }

    @Override
    public int numLevels() {
        return 1;
    }

    @Override
    public int minDocID(int level) {
        if (page < 0) {
            return -1;
        }
        return exhausted() ? DocIdSetIterator.NO_MORE_DOCS : (int) pageIndex.firstRowOf(page);
    }

    @Override
    public int maxDocID(int level) {
        if (page < 0) {
            return -1;
        }
        return exhausted() ? DocIdSetIterator.NO_MORE_DOCS : (int) (pageIndex.firstRowOf(page) + pageIndex.numRowsOf(page) - 1);
    }

    @Override
    public long minValue(int level) {
        return pageIndex.minOf(page);
    }

    @Override
    public long maxValue(int level) {
        return pageIndex.maxOf(page);
    }

    @Override
    public int docCount(int level) {
        return (int) pageIndex.docCountOf(page);
    }

    @Override
    public long minValue() {
        return globalMin;
    }

    @Override
    public long maxValue() {
        return globalMax;
    }

    @Override
    public int docCount() {
        return globalDocCount;
    }
}
