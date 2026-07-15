/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec.iter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.parquet.bridge.ParquetColumnReader;
import org.opensearch.parquet.codec.cache.PageCache;

import java.io.IOException;

/**
 * Cache-aware {@link NumericDocValues} over a single-valued Parquet primitive column.
 *
 * <p>Hot path: a presence bit-test plus a {@code long[]} index lookup against the column
 * reader's current {@link PageCache}. Cold path (page miss): {@link ParquetColumnReader#loadPageContaining}
 * decodes the page (or applies the Layer 4 all-nulls skip). Float/double values are stored as
 * raw IEEE-754 bits at page-decode time, so {@link #longValue()} returns the Lucene-encoded
 * form directly.
 */
public final class ParquetNumericDocValues extends NumericDocValues {

    private static final Logger LOGGER = LogManager.getLogger(ParquetNumericDocValues.class);

    private final ParquetColumnReader reader;
    private final int maxDoc;

    private int doc = -1;
    private long currentValue;
    private boolean currentPresent;
    private long advanceCalls = 0;
    private long nextDocCalls = 0;
    private long longValueCalls = 0;

    public ParquetNumericDocValues(ParquetColumnReader reader, int maxDoc) {
        this.reader = reader;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (target >= maxDoc) {
            doc = NO_MORE_DOCS;
            currentPresent = false;
            LOGGER.info("[DV-NUM-CALL] advanceExact({}) → false (>=maxDoc={})", target, maxDoc);
            return false;
        }
        doc = target;
        PageCache cache = reader.cache();
        if (cache != null && target <= cache.lastRow && target >= cache.firstRow) {
            // Layer 1/2 hit — served from the resident page, no FFM crossing.
        } else {
            // Layer 1/2 miss — load the page containing this row (Layer 3 → 4 → FFM).
            reader.loadPageContaining(target);
            cache = reader.cache();
            if (cache == null) { // Layer 4: page is all-nulls.
                currentPresent = false;
                currentValue = 0L;
                LOGGER.info("[DV-NUM-CALL] advanceExact({}) → false (all-null page)", target);
                return false;
            }
        }
        currentPresent = cache.isPresent(target);
        currentValue = currentPresent ? cache.valueAt(target) : 0L;
        LOGGER.info("[DV-NUM-CALL] advanceExact({}) → present={} value={}", target, currentPresent, currentValue);
        return currentPresent;
    }

    @Override
    public long longValue() {
        longValueCalls++;
        if (longValueCalls <= 5 || longValueCalls % 500 == 0) {
            LOGGER.info("[DV-NUM-CALL] longValue()#{} → {} (doc={})", longValueCalls, currentValue, doc);
        }
        return currentValue;
    }

    @Override
    public int docID() {
        return doc;
    }

    @Override
    public int nextDoc() throws IOException {
        nextDocCalls++;
        int r = advance(doc + 1);
        if (nextDocCalls <= 5 || nextDocCalls % 500 == 0) {
            LOGGER.info("[DV-NUM-CALL] nextDoc()#{} startFrom={} → {}", nextDocCalls, doc, r);
        }
        return r;
    }

    @Override
    public int advance(int target) throws IOException {
        advanceCalls++;
        for (int d = target; d < maxDoc; d++) {
            if (advanceExact(d)) {
                doc = d;
                if (advanceCalls <= 5 || advanceCalls % 500 == 0) {
                    LOGGER.info("[DV-NUM-CALL] advance({})#{} → {} value={}", target, advanceCalls, d, currentValue);
                }
                return d;
            }
        }
        doc = NO_MORE_DOCS;
        LOGGER.info("[DV-NUM-CALL] advance({})#{} → NO_MORE_DOCS", target, advanceCalls);
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return maxDoc;
    }
}
