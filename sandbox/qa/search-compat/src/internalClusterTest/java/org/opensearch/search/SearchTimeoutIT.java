/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search;


import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchTimeoutIT.ScriptedTimeoutPlugin.SCRIPT_NAME;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public class SearchTimeoutIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public SearchTimeoutIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.Arrays.asList(ScriptedTimeoutPlugin.class,
            org.opensearch.arrow.allocator.ArrowBasePlugin.class,
            org.opensearch.arrow.flight.transport.FlightStreamPlugin.class,
            org.opensearch.analytics.AnalyticsPlugin.class,
            org.opensearch.be.datafusion.DataFusionPlugin.class,
            org.opensearch.be.lucene.LucenePlugin.class,
            org.opensearch.composite.CompositeDataFormatPlugin.class,
            org.opensearch.parquet.ParquetDataFormatPlugin.class,
            org.opensearch.dsl.DslQueryExecutorPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    public void testSimpleTimeout() throws Exception {
        for (int i = 0; i < 32; i++) {
            client().prepareIndex("test").setSource("field", "value").get();
        }
        refresh("test");
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch("test")
            .setTimeout(new TimeValue(5, TimeUnit.MILLISECONDS))
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
            .setAllowPartialSearchResults(true)
            .get();
        assertTrue(searchResponse.isTimedOut());
        assertEquals(0, searchResponse.getFailedShards());
    }

    public void testSimpleDoesNotTimeout() throws Exception {
        final int numDocs = 9;
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        }
        indexRandomForConcurrentSearch("test");
        SearchResponse searchResponse = client().prepareSearch("test")
            .setTimeout(new TimeValue(10000, TimeUnit.SECONDS))
            .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
            .setAllowPartialSearchResults(true)
            .get();
        assertFalse(searchResponse.isTimedOut());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(numDocs, searchResponse.getHits().getTotalHits().value());
    }

    public void testPartialResultsIntolerantTimeout() throws Exception {
        client().prepareIndex("test").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        indexRandomForConcurrentSearch("test");
        OpenSearchException ex = expectThrows(
            OpenSearchException.class,
            () -> client().prepareSearch("test")
                .setTimeout(new TimeValue(10, TimeUnit.MILLISECONDS))
                .setQuery(scriptQuery(new Script(ScriptType.INLINE, "mockscript", SCRIPT_NAME, Collections.emptyMap())))
                .setAllowPartialSearchResults(false) // this line causes timeouts to report failures
                .get()
        );
        assertTrue(ex.toString().contains("QueryPhaseExecutionException[Time exceeded]"));
    }

    public static class ScriptedTimeoutPlugin extends MockScriptPlugin {
        static final String SCRIPT_NAME = "search_timeout";

        @Override
        @SuppressForbidden(reason = "Simulating a slow task by sleeping")
        public Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(SCRIPT_NAME, params -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return true;
            });
        }
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
