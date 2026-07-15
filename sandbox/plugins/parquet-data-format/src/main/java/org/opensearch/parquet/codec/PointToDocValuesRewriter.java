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
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.opensearch.search.approximate.ApproximateScoreQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursively rewrites a Lucene {@link Query} tree so numeric range/term filters can execute
 * against Parquet-backed indices that have no BKD trees.
 *
 * <p>On composite/Parquet-primary indices the Lucene secondary writes no BKD points for numeric
 * fields, but OpenSearch's {@code NumberFieldMapper} still builds point-based queries. At
 * execution time the point side finds an empty {@code PointValues} and the query returns zero
 * hits. This rewriter converts any point-based numeric query into an equivalent doc-values
 * range query — {@link SortedNumericDocValuesField#newSlowRangeQuery} — which runs against the
 * codec-served doc values and consults {@link ParquetDocValuesSkipper} for page-level min/max
 * skipping.
 *
 * <p>Nodes rewritten:
 * <ul>
 *   <li>{@link ApproximateScoreQuery} — unwrap to its original query and re-run rewrite</li>
 *   <li>{@link IndexOrDocValuesQuery} — replace with its doc-values half (recursed)</li>
 *   <li>{@link PointRangeQuery} — replace with the equivalent DV range for INT32/INT64/FLOAT/DOUBLE</li>
 *   <li>{@link BooleanQuery}, {@link ConstantScoreQuery}, {@link BoostQuery},
 *       {@link DisjunctionMaxQuery} — traversed recursively so nested numeric filters
 *       (typical inside a {@code bool.filter}) are also rewritten</li>
 * </ul>
 * Other query types pass through unchanged.
 */
public final class PointToDocValuesRewriter {

    private static final Logger LOGGER = LogManager.getLogger(PointToDocValuesRewriter.class);

    private PointToDocValuesRewriter() {}

    /**
     * Returns the query with every {@link IndexOrDocValuesQuery} in the tree replaced by
     * its doc-values half. Returns the input unchanged when no rewrite is needed.
     */
    public static Query rewrite(Query q) {
        if (q == null) {
            return null;
        }

        // OpenSearch wraps every numeric range in ApproximateScoreQuery(originalPointRange, approxQuery).
        // Both branches are point-based → useless on our secondary. Unwrap to originalQuery and recurse
        // (which will hit the PointRangeQuery case below and convert to a DV range).
        if (q instanceof ApproximateScoreQuery aq) {
            Query original = aq.getOriginalQuery();
            LOGGER.info("[POINT-TO-DV-REWRITE] unwrapping ApproximateScoreQuery → original={}", original);
            return rewrite(original);
        }

        if (q instanceof IndexOrDocValuesQuery idv) {
            Query dv = idv.getRandomAccessQuery();
            LOGGER.info("[POINT-TO-DV-REWRITE] stripping IDVQ point-side; keeping dv: {}", dv);
            return rewrite(dv); // recurse in case the dv half itself contains nested IDVQs
        }

        // OpenSearch's NumberFieldMapper wraps range queries on index-sorted fields in
        // IndexSortSortedNumericDocValuesRangeQuery. That class tries a BKD-based iterator
        // first (returns null for us since we have no BKD) and then falls back to its
        // fallbackQuery — usually an IndexOrDocValuesQuery whose point side also fails.
        // Unwrap to the fallback and recurse so the IDVQ inside gets stripped too.
        if (q instanceof IndexSortSortedNumericDocValuesRangeQuery iss) {
            Query fb = iss.getFallbackQuery();
            LOGGER.info("[POINT-TO-DV-REWRITE] unwrapping IndexSortSortedNumericDocValuesRangeQuery → fallback={}", fb);
            return rewrite(fb);
        }

        // The most important case for our POC: convert a bare PointRangeQuery (from
        // LongPoint.newRangeQuery / IntPoint / FloatPoint / DoublePoint) into the
        // equivalent SortedNumericDocValuesField.newSlowRangeQuery so execution follows
        // the doc-values scan path (where ParquetDocValuesSkipper fires).
        if (q instanceof PointRangeQuery prq) {
            Query converted = convertPointRangeToDvRange(prq);
            if (converted != null) {
                LOGGER.info(
                    "[POINT-TO-DV-REWRITE] PointRangeQuery(field={}, dims={}, bytesPerDim={}) → {}",
                    prq.getField(), prq.getNumDims(), prq.getBytesPerDim(), converted);
                return converted;
            }
            LOGGER.info(
                "[POINT-TO-DV-REWRITE] PointRangeQuery(field={}, dims={}, bytesPerDim={}) — no DV conversion (unsupported shape)",
                prq.getField(), prq.getNumDims(), prq.getBytesPerDim());
            return q;
        }

        if (q instanceof BooleanQuery bq) {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            b.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
            boolean changed = false;
            for (BooleanClause c : bq.clauses()) {
                Query rewrote = rewrite(c.query());
                if (rewrote != c.query()) {
                    changed = true;
                }
                b.add(rewrote, c.occur());
            }
            return changed ? b.build() : q;
        }

        if (q instanceof ConstantScoreQuery csq) {
            Query inner = rewrite(csq.getQuery());
            return inner == csq.getQuery() ? q : new ConstantScoreQuery(inner);
        }

        if (q instanceof BoostQuery bq) {
            Query inner = rewrite(bq.getQuery());
            return inner == bq.getQuery() ? q : new BoostQuery(inner, bq.getBoost());
        }

        if (q instanceof DisjunctionMaxQuery dmq) {
            List<Query> rewrittenDisjuncts = new ArrayList<>(dmq.getDisjuncts().size());
            boolean changed = false;
            for (Query d : dmq.getDisjuncts()) {
                Query rewrote = rewrite(d);
                if (rewrote != d) {
                    changed = true;
                }
                rewrittenDisjuncts.add(rewrote);
            }
            return changed ? new DisjunctionMaxQuery(rewrittenDisjuncts, dmq.getTieBreakerMultiplier()) : q;
        }

        // Leaf query or wrapper we don't specifically handle — leave as-is.
        return q;
    }

    /**
     * Converts a 1-dimensional {@link PointRangeQuery} into the equivalent
     * {@link SortedNumericDocValuesField#newSlowRangeQuery} for supported primitive types.
     * Returns {@code null} for unsupported shapes (multi-dim, byte-array IP, etc.), leaving
     * the caller to keep the original query.
     *
     * <p>4-byte points (INT32) are decoded via {@link IntPoint#decodeDimension} and 8-byte
     * points (INT64) via {@link LongPoint#decodeDimension}. FLOAT/DOUBLE columns encode
     * differently (XORed sign bit for sortability); a future extension can gate by field
     * name and use {@link FloatPoint}/{@link DoublePoint} decoders with the corresponding
     * {@code NumericUtils#floatToSortableInt} / {@code doubleToSortableLong} transform.
     */
    private static Query convertPointRangeToDvRange(PointRangeQuery prq) {
        if (prq.getNumDims() != 1) {
            return null; // multi-dim: geo/IP — separate handling, out of POC scope
        }
        String field = prq.getField();
        byte[] lower = prq.getLowerPoint();
        byte[] upper = prq.getUpperPoint();

        switch (prq.getBytesPerDim()) {
            case 4: {
                long lo = IntPoint.decodeDimension(lower, 0);
                long hi = IntPoint.decodeDimension(upper, 0);
                return SortedNumericDocValuesField.newSlowRangeQuery(field, lo, hi);
            }
            case 8: {
                long lo = LongPoint.decodeDimension(lower, 0);
                long hi = LongPoint.decodeDimension(upper, 0);
                return SortedNumericDocValuesField.newSlowRangeQuery(field, lo, hi);
            }
            default:
                return null; // e.g. 16-byte IP points, not handled in POC
        }
    }
}
