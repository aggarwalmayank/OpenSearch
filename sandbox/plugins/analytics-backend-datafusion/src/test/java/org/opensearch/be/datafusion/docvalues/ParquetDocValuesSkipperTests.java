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
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link ParquetDocValuesSkipper}: the DocValuesSkipper contract (advance semantics,
 * sentinel doc IDs) and range-driven page skipping over a synthetic {@link ColumnPageIndex}.
 */
public class ParquetDocValuesSkipperTests extends OpenSearchTestCase {

    /**
     * Three 100-row pages with disjoint value ranges:
     * page 0 rows [0,99] values [10,20], page 1 rows [100,199] values [50,60],
     * page 2 rows [200,299] values [90,100]. No nulls.
     */
    private static ColumnPageIndex threePages() {
        return new ColumnPageIndex(
            new long[] { 0, 100, 200 }, // firstRowOf
            new long[] { 0, 0, 0 },     // nullCountOf
            new long[] { 10, 50, 90 },  // minOf
            new long[] { 20, 60, 100 }, // maxOf
            300                         // totalRows
        );
    }

    public void testInitialStateAndAdvance() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        assertEquals(-1, skipper.minDocID(0));
        assertEquals(-1, skipper.maxDocID(0));
        assertEquals(1, skipper.numLevels());

        skipper.advance(0);
        assertEquals(0, skipper.minDocID(0));
        assertEquals(99, skipper.maxDocID(0));
        assertEquals(10L, skipper.minValue(0));
        assertEquals(20L, skipper.maxValue(0));
        assertEquals(100, skipper.docCount(0));

        skipper.advance(150);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));
        assertEquals(50L, skipper.minValue(0));
        assertEquals(60L, skipper.maxValue(0));
    }

    public void testExhaustion() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        skipper.advance(300);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.minDocID(0));
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper.maxDocID(0));
    }

    public void testGlobalStats() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        assertEquals(10L, skipper.minValue());
        assertEquals(100L, skipper.maxValue());
        assertEquals(300, skipper.docCount());
    }

    /** The base-class range advance must land on the first page intersecting the value range. */
    public void testRangeAdvanceSkipsNonIntersectingPages() throws Exception {
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(threePages(), 300);
        // [55, 58] intersects only page 1.
        skipper.advance(55L, 58L);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));

        // A range beyond every page exhausts the skipper.
        DocValuesSkipper skipper2 = new ParquetDocValuesSkipper(threePages(), 300);
        skipper2.advance(200L, 300L);
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, skipper2.minDocID(0));
    }

    /** Pages with the unknown-stats sentinel must intersect every range (never wrongly skipped). */
    public void testUnknownStatsPageIsNeverSkipped() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0, 100 },             // firstRowOf
            new long[] { 0, -1 },              // nullCountOf (page 1 unknown)
            new long[] { 10, Long.MIN_VALUE }, // minOf (page 1 sentinel)
            new long[] { 20, Long.MAX_VALUE }, // maxOf (page 1 sentinel)
            200                                // totalRows
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(idx, 200);
        // Range [500, 600] excludes page 0 (max 20) but must land on the unknown-stats page.
        skipper.advance(500L, 600L);
        assertEquals(100, skipper.minDocID(0));
        assertEquals(199, skipper.maxDocID(0));
        // Unknown null count → docCount under-claims as 0 rather than overclaiming.
        assertEquals(0, skipper.docCount(0));
    }

    public void testDocCountSubtractsNulls() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(
            new long[] { 0 },  // firstRowOf
            new long[] { 30 }, // nullCountOf
            new long[] { 1 },  // minOf
            new long[] { 9 },  // maxOf
            100                // totalRows
        );
        DocValuesSkipper skipper = new ParquetDocValuesSkipper(idx, 100);
        skipper.advance(0);
        assertEquals(70, skipper.docCount(0));
        assertEquals(70, skipper.docCount());
    }

    // Direct assertions on the precomputed global stats held by ColumnPageIndex. The skipper tests
    // above exercise the same values through the skipper; these pin the source directly.

    /** A normal page 0 [10,20] followed by a sentinel page 1 with unknown null count. */
    private static ColumnPageIndex sentinelMixed() {
        return new ColumnPageIndex(
            new long[] { 0, 100 },             // firstRowOf
            new long[] { 0, -1 },              // nullCountOf (page 1 unknown)
            new long[] { 10, Long.MIN_VALUE }, // minOf (page 1 sentinel)
            new long[] { 20, Long.MAX_VALUE }, // maxOf (page 1 sentinel)
            200                                // totalRows
        );
    }

    public void testColumnPageIndexGlobalStatsNormalPages() throws Exception {
        ColumnPageIndex idx = threePages();
        assertEquals(10L, idx.globalMin());
        assertEquals(100L, idx.globalMax());
        assertEquals(300L, idx.globalDocCount());
        assertEquals(100L, idx.docCountOf(0));
        assertEquals(100L, idx.docCountOf(1));
        assertEquals(100L, idx.docCountOf(2));
    }

    public void testColumnPageIndexSentinelPageMakesGlobalsMinMax() throws Exception {
        ColumnPageIndex idx = sentinelMixed();
        // A sentinel page carries (MIN, MAX), so it widens the globals to the full range.
        assertEquals(Long.MIN_VALUE, idx.globalMin());
        assertEquals(Long.MAX_VALUE, idx.globalMax());
    }

    public void testColumnPageIndexUnknownNullCountExcludedFromGlobalDocCount() throws Exception {
        ColumnPageIndex idx = sentinelMixed();
        assertEquals(100L, idx.docCountOf(0)); // known: 100 rows, 0 nulls
        assertEquals(0L, idx.docCountOf(1));   // unknown null count under-claims as 0
        assertEquals(100L, idx.globalDocCount()); // the unknown page contributes nothing
    }

    public void testColumnPageIndexEmpty() throws Exception {
        ColumnPageIndex idx = new ColumnPageIndex(new long[] {}, new long[] {}, new long[] {}, new long[] {}, 0);
        assertEquals(Long.MIN_VALUE, idx.globalMin());
        assertEquals(Long.MAX_VALUE, idx.globalMax());
        assertEquals(0L, idx.globalDocCount());
    }
}
