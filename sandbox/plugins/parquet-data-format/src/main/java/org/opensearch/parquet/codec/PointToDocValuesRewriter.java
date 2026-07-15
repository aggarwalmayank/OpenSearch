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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * Recursively rewrites a Lucene {@link Query} tree, replacing every
 * {@link IndexOrDocValuesQuery} with its {@code randomAccessQuery} (doc-values) half.
 *
 * <p>Rationale: on composite/Parquet-primary indices the Lucene secondary does not write
 * BKD point trees for numeric fields, but the {@code NumberFieldMapper} still constructs
 * range/term queries as {@code IndexOrDocValuesQuery(pointQuery, dvQuery)} at mapping
 * time. At execution time the point side finds no BKD, which short-circuits the whole
 * wrapper to zero hits. Stripping the point half up-front forces Lucene to run the
 * doc-values scan (which uses {@link ParquetDocValuesSkipper} for page-level min/max
 * skipping).
 *
 * <p>Common wrappers — {@link BooleanQuery}, {@link ConstantScoreQuery},
 * {@link BoostQuery}, {@link DisjunctionMaxQuery} — are traversed recursively so nested
 * {@code IndexOrDocValuesQuery} nodes (the usual case inside a {@code bool.filter}) are
 * also rewritten. Other query types pass through unchanged.
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

        if (q instanceof IndexOrDocValuesQuery idv) {
            Query dv = idv.getRandomAccessQuery();
            LOGGER.info("[POINT-TO-DV-REWRITE] stripping point-side; keeping dv: {}", dv);
            return rewrite(dv); // recurse in case the dv half itself contains nested IDVQs
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
}
