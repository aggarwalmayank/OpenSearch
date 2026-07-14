/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.msearch;


import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.stats.SearchResponseStatusStats;
import org.opensearch.action.search.MultiSearchRequestBuilder;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.LongAdder;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public class MultiSearchStatsIT extends OpenSearchIntegTestCase {
    private final SearchResponseStatusStats expectedSearchResponseStatusStats = new SearchResponseStatusStats();

    public void testNodeIndicesStatsSearchResponseStatusStatsMultiSearch() {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test").setSource("field", "xxx").get();
        client().prepareIndex("test").setSource("field", "yyy").get();
        refresh();

        int failureCount = randomIntBetween(0, 50);
        int successCount = randomIntBetween(0, 50);

        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();

        for (int i = 0; i < failureCount; i++) {
            multiSearchRequestBuilder.add(client().prepareSearch("noIndex").setQuery(QueryBuilders.termQuery("field", "yyy")));
        }

        for (int i = 0; i < successCount; i++) {
            multiSearchRequestBuilder.add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()));
        }

        MultiSearchResponse response = multiSearchRequestBuilder.get();

        for (MultiSearchResponse.Item item : response) {
            if (item.isFailure()) {
                updateExpectedDocStatusCounter(expectedSearchResponseStatusStats, item.getFailure());
            } else {
                updateExpectedDocStatusCounter(expectedSearchResponseStatusStats, item.getResponse());
            }
        }
        assertSearchResponseStatusStats();
    }

    private void assertSearchResponseStatusStats() {
        SearchResponseStatusStats searchResponseStatusStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .execute()
            .actionGet()
            .getNodes()
            .get(0)
            .getIndices()
            .getStatusCounterStats()
            .getSearchResponseStatusStats();

        assertTrue(
            Arrays.equals(
                searchResponseStatusStats.getSearchResponseStatusCounter(),
                expectedSearchResponseStatusStats.getSearchResponseStatusCounter(),
                Comparator.comparingLong(LongAdder::longValue)
            )
        );
    }

    private void updateExpectedDocStatusCounter(SearchResponseStatusStats expectedSearchResponseStatusStats, SearchResponse r) {
        expectedSearchResponseStatusStats.inc(r.status());
    }

    private void updateExpectedDocStatusCounter(SearchResponseStatusStats expectedSearchResponseStatusStats, Exception e) {
        expectedSearchResponseStatusStats.inc(ExceptionsHelper.status(e));
    }

    @Override
    protected java.util.Collection<Class<? extends org.opensearch.plugins.Plugin>> nodePlugins() {
        java.util.List<Class<? extends org.opensearch.plugins.Plugin>> _mustangPlugins = new java.util.ArrayList<>(super.nodePlugins());
        _mustangPlugins.add(org.opensearch.arrow.allocator.ArrowBasePlugin.class);
        _mustangPlugins.add(org.opensearch.arrow.flight.transport.FlightStreamPlugin.class);
        _mustangPlugins.add(org.opensearch.analytics.AnalyticsPlugin.class);
        _mustangPlugins.add(org.opensearch.be.datafusion.DataFusionPlugin.class);
        _mustangPlugins.add(org.opensearch.be.lucene.LucenePlugin.class);
        _mustangPlugins.add(org.opensearch.composite.CompositeDataFormatPlugin.class);
        _mustangPlugins.add(org.opensearch.parquet.ParquetDataFormatPlugin.class);
        _mustangPlugins.add(org.opensearch.dsl.DslQueryExecutorPlugin.class);
        return _mustangPlugins;
    }


    @Override
    public org.opensearch.common.settings.Settings indexSettings() {
        return org.opensearch.common.settings.Settings.builder()
            .put(super.indexSettings())
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .put("index.composite.secondary_data_formats", "lucene")
            .build();
    }


    @Override
    protected org.opensearch.common.settings.Settings featureFlagSettings() {
        return org.opensearch.common.settings.Settings.builder()
            .put(super.featureFlagSettings())
            .put("opensearch.experimental.feature.transport.stream.enabled", true)
            .put("opensearch.experimental.feature.pluggable.dataformat.enabled", true)
            .build();
    }


    @Override
    public org.opensearch.cluster.health.ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(org.opensearch.common.unit.TimeValue.timeValueMinutes(5), indices);
    }

}
