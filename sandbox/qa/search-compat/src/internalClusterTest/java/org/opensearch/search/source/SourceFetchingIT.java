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

package org.opensearch.search.source;


import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public class SourceFetchingIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public SourceFetchingIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testSourceDefaultBehavior() throws InterruptedException {
        createIndex("test");
        ensureGreen();

        index("test", "type1", "1", "field", "value");
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse response = client().prepareSearch("test").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

        response = client().prepareSearch("test").addStoredField("bla").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client().prepareSearch("test").addStoredField("_source").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

    }

    public void testSourceFiltering() throws InterruptedException {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setSource("field1", "value", "field2", "value2").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse response = client().prepareSearch("test").setFetchSource(false).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client().prepareSearch("test").setFetchSource(true).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

        response = client().prepareSearch("test").setFetchSource("field1", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value"));

        response = client().prepareSearch("test").setFetchSource("hello", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(0));

        response = client().prepareSearch("test").setFetchSource(new String[] { "*" }, new String[] { "field2" }).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value"));

    }

    /**
     * Test Case for #5132: Source filtering with wildcards broken when given multiple patterns
     * https://github.com/elastic/elasticsearch/issues/5132
     */
    public void testSourceWithWildcardFiltering() throws InterruptedException {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setSource("field", "value").get();
        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse response = client().prepareSearch("test").setFetchSource(new String[] { "*.notexisting", "field" }, null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("value"));

        response = client().prepareSearch("test").setFetchSource(new String[] { "field.notexisting.*", "field" }, null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("value"));
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
