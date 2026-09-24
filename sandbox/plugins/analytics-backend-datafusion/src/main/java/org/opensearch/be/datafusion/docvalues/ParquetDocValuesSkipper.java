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
import org.opensearch.be.datafusion.docvalues.bridge.ColumnPageIndex;

public final class ParquetDocValuesSkipper extends DocValuesSkipper {

    private final ColumnPageIndex pageIndex;
    private final int maxDoc;
    private final long globalMin;
    private final long globalMax;
    private final int globalDocCount;

    private int page = -1;

    public ParquetDocValuesSkipper(ColumnPageIndex pageIndex, int maxDoc) {
        this.pageIndex = pageIndex;
        this.maxDoc = maxDoc;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        long withValue = 0;
        boolean hasComparableValue = false;
        for (int p = 0; p < pageIndex.pageCount(); p++) {
            if (pageIndex.isAllNulls(p) == false) {
                min = Math.min(min, pageIndex.minOf(p));
                max = Math.max(max, pageIndex.maxOf(p));
                hasComparableValue = true;
            }
            withValue += pageDocCount(pageIndex, p);
        }
        this.globalMin = hasComparableValue ? min : Long.MIN_VALUE;
        this.globalMax = hasComparableValue ? max : Long.MAX_VALUE;
        this.globalDocCount = (int) withValue;
    }

    private static long pageDocCount(ColumnPageIndex pageIndex, int p) {
        long nulls = pageIndex.nullCountOf(p);
        return nulls < 0 ? 0 : pageIndex.numRowsOf(p) - nulls;
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
        return (int) pageDocCount(pageIndex, page);
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
