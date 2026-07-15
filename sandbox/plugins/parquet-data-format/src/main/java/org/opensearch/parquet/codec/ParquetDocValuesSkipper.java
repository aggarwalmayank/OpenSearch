/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.parquet.codec.cache.ColumnPageIndex;

import java.io.IOException;

/**
 * DocValues skipper backed by Parquet per-page statistics (Layer 4).
 *
 * <p>Lucene's numeric range comparators call this skipper to decide whether the doc range
 * covered by a Parquet page can possibly contribute to a filter's result set. If the page's
 * {@code [min, max]} does not intersect the query range, or the page is entirely null,
 * Lucene skips the whole page without a doc-values scan — replacing the BKD skip semantics
 * that Parquet-only indices lack.
 *
 * <p>This is a single-level skipper: level 0 is the Parquet page. The number of levels
 * exposed to Lucene is fixed at 1 for the POC. Global aggregates ({@link #minValue()},
 * {@link #maxValue()}, {@link #docCount()}) are precomputed at construction from all pages.
 *
 * <p>All min/max values are exchanged as raw {@code long} bits, matching the Rust FFM
 * bridge contract. Numeric columns use these directly (Lucene's comparator handles the
 * bit-level comparison for {@code long}/{@code int}); float/double columns would require
 * IEEE-754-aware sortable encoding — out of scope for this POC (registered only for
 * integer numeric fields).
 */
public final class ParquetDocValuesSkipper extends DocValuesSkipper {

    private static final Logger LOGGER = LogManager.getLogger(ParquetDocValuesSkipper.class);

    private final ColumnPageIndex pageIndex;
    private final int maxDoc;
    private final String fieldNameForLog;

    // Per-instance stats — one skipper is created per (segment, column) per query;
    // its lifetime = one query on one segment, so these counters describe that scope.
    private long advanceCalls = 0;
    private long pagesLanded = 0;    // increments only when currentPage transitions to a *new* page
    private int  lastLoggedPage = -2;

    // Precomputed global aggregates over all pages.
    private final long globalMin;
    private final long globalMax;
    private final int globalDocCount;

    /** Current page after {@link #advance(int)}; -1 before first advance, {@code pageCount} when exhausted. */
    private int currentPage = -1;

    public ParquetDocValuesSkipper(ColumnPageIndex pageIndex, int maxDoc) {
        this(pageIndex, maxDoc, "<unknown>");
    }

    public ParquetDocValuesSkipper(ColumnPageIndex pageIndex, int maxDoc, String fieldNameForLog) {
        this.pageIndex = pageIndex;
        this.maxDoc = maxDoc;
        this.fieldNameForLog = fieldNameForLog;
        long gMin = Long.MAX_VALUE;
        long gMax = Long.MIN_VALUE;
        long gDocs = 0;
        int nPages = pageIndex.pageCount();
        for (int p = 0; p < nPages; p++) {
            long nc = pageIndex.nullCountOf(p);
            long rows = pageIndex.numRowsOf(p);
            long docs = (nc >= 0) ? (rows - nc) : rows; // when null count unknown, assume all present
            gDocs += docs;
            if (docs > 0) {
                gMin = Math.min(gMin, pageIndex.minOf(p));
                gMax = Math.max(gMax, pageIndex.maxOf(p));
            }
        }
        this.globalMin = (gDocs > 0) ? gMin : Long.MIN_VALUE;
        this.globalMax = (gDocs > 0) ? gMax : Long.MAX_VALUE;
        this.globalDocCount = Math.toIntExact(gDocs);
        LOGGER.info(
            "[PARQUET-DVSKIPPER-CTOR] field={} pages={} maxDoc={} globalMin={} globalMax={} globalDocCount={}",
            fieldNameForLog, nPages, maxDoc, globalMin, globalMax, globalDocCount);
    }

    @Override
    public void advance(int target) throws IOException {
        advanceCalls++;
        if (target >= maxDoc) {
            currentPage = pageIndex.pageCount();
            return;
        }
        int p = pageIndex.pageForRow(target);
        if (p < 0) {
            currentPage = pageIndex.pageCount();
            return;
        }
        if (p != lastLoggedPage) {
            pagesLanded++;
            lastLoggedPage = p;
        }
        currentPage = p;
    }

    /** Total invocations of {@link #advance(int)} since construction. */
    public long advanceCalls() { return advanceCalls; }

    /** Distinct pages the skipper has landed on. */
    public long pagesLanded()  { return pagesLanded; }

    /** Pages Lucene did not need to touch: {@code totalPages - pagesLanded}. */
    public long pagesSkipped() { return pageIndex.pageCount() - pagesLanded; }

    /** Total pages in this column of this segment. */
    public int totalPages()    { return pageIndex.pageCount(); }

    /** Field name (for stats logging). */
    public String fieldName()  { return fieldNameForLog; }

    /**
     * Emit a single-line summary of this skipper's activity — designed to be grep'd
     * from the OpenSearch log after running a search. Called once when the segment
     * reader closes (see {@code ParquetDocValuesProducer.close}).
     */
    public void logStats() {
        LOGGER.info(
            "[SKIPPER-STATS] field={} totalPages={} advanceCalls={} pagesLanded={} pagesSkipped={} skipRate={}%",
            fieldNameForLog,
            totalPages(),
            advanceCalls,
            pagesLanded,
            pagesSkipped(),
            totalPages() == 0 ? 0 : (100L * pagesSkipped() / totalPages()));
    }

    @Override
    public int numLevels() {
        return 1;
    }

    @Override
    public int minDocID(int level) {
        checkLevel(level);
        if (currentPage < 0) return -1;
        if (currentPage >= pageIndex.pageCount()) return DocIdSetIterator.NO_MORE_DOCS;
        return Math.toIntExact(pageIndex.firstRowOf(currentPage));
    }

    @Override
    public int maxDocID(int level) {
        checkLevel(level);
        if (currentPage < 0) return -1;
        if (currentPage >= pageIndex.pageCount()) return DocIdSetIterator.NO_MORE_DOCS;
        long last = pageIndex.firstRowOf(currentPage) + pageIndex.numRowsOf(currentPage) - 1;
        return Math.toIntExact(Math.min(last, maxDoc - 1L));
    }

    @Override
    public long minValue(int level) {
        checkLevel(level);
        return pageIndex.minOf(currentPage);
    }

    @Override
    public long maxValue(int level) {
        checkLevel(level);
        return pageIndex.maxOf(currentPage);
    }

    @Override
    public int docCount(int level) {
        checkLevel(level);
        long rows = pageIndex.numRowsOf(currentPage);
        long nc = pageIndex.nullCountOf(currentPage);
        long docs = (nc >= 0) ? (rows - nc) : rows;
        return Math.toIntExact(docs);
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

    private static void checkLevel(int level) {
        if (level != 0) {
            throw new IllegalArgumentException("ParquetDocValuesSkipper exposes a single level; got " + level);
        }
    }
}
