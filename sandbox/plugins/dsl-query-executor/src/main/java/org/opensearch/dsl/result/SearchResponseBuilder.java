/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * Builds a {@link SearchResponse} from execution results.
 *
 * Merges HITS and multiple AGGREGATION execution results (one per granularity)
 * into a single SearchResponse by walking the original aggregation tree.
 */
public final class SearchResponseBuilder {

    private SearchResponseBuilder() {}

    /**
     * Builds a SearchResponse from the given results, original search source, and timing.
     *
     * @param results          execution results from the plan executor
     * @param searchSource     the original SearchSourceBuilder (needed for aggregation tree structure)
     * @param convertTimeNanos time spent in DSL-to-RelNode conversion, in nanoseconds
     * @return a SearchResponse
     */
    public static SearchResponse build(List<ExecutionResult> results, SearchSourceBuilder searchSource,
                                       long convertTimeNanos) {
        long tookInMillis = convertTimeNanos / 1_000_000;

        SearchHits hits = buildHits(results);
        InternalAggregations aggs = buildAggregations(results, searchSource);

        SearchResponseSections sections = new SearchResponseSections(hits, aggs, null, false, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private static SearchHits buildHits(List<ExecutionResult> results) {
        return results.stream()
            .filter(r -> r.getType() == QueryPlans.Type.HITS)
            .findFirst()
            .map(HitsResponseBuilder::build)
            .orElse(SearchHits.empty(true));
    }

    private static InternalAggregations buildAggregations(List<ExecutionResult> results,
                                                          SearchSourceBuilder searchSource) {
        boolean hasAggs = results.stream().anyMatch(r -> r.getType() == QueryPlans.Type.AGGREGATION);
        if (!hasAggs || searchSource == null || searchSource.aggregations() == null) {
            return null;
        }

        AggregationRegistry registry = AggregationRegistryFactory.create();
        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, results);
        return builder.build(searchSource.aggregations().getAggregatorFactories());
    }
}
