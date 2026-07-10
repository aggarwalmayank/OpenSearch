/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalValueCount;

import java.util.Map;

/**
 * Translates cardinality (approximate distinct count) metric aggregation to/from Calcite.
 *
 * For response conversion, uses InternalValueCount as a stand-in since InternalCardinality
 * requires an HLL sketch that cannot be reconstructed from a plain count value.
 */
public class CardinalityMetricTranslator extends AbstractMetricTranslator<CardinalityAggregationBuilder> {

    /** Creates a cardinality metric translator. */
    public CardinalityMetricTranslator() {}

    @Override
    public Class<CardinalityAggregationBuilder> getAggregationType() {
        return CardinalityAggregationBuilder.class;
    }

    @Override
    protected SqlAggFunction getAggFunction() {
        return SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
    }

    @Override
    protected String getFieldName(CardinalityAggregationBuilder agg) {
        return agg.field();
    }

    @Override
    public InternalAggregation toInternalAggregation(String name, Object value) {
        long v = value == null ? 0 : ((Number) value).longValue();
        return new InternalValueCount(name, v, Map.of());
    }
}
