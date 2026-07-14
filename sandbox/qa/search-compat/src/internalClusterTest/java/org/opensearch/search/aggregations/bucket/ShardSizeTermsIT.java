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

package org.opensearch.search.aggregations.bucket;


import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.Terms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.hamcrest.Matchers.equalTo;

@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public class ShardSizeTermsIT extends ShardSizeTestCase {

    public ShardSizeTermsIT(Settings staticSettings) {
        super(staticSettings);
    }

    public void testNoShardSizeString() throws Exception {
        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("3", 8L);
        expected.put("2", 5L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testShardSizeEqualsSizeString() throws Exception {
        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .shardSize(3)
                    .showTermDocCountError(true)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("3", 8L);
        expected.put("2", 4L);
        Long expectedDocCount;
        for (Terms.Bucket bucket : buckets) {
            expectedDocCount = expected.get(bucket.getKeyAsString());
            // Doc count can vary when using concurrent segment search. See https://github.com/opensearch-project/OpenSearch/issues/11680
            assertTrue((bucket.getDocCount() == expectedDocCount) || bucket.getDocCount() + bucket.getDocCountError() >= expectedDocCount);
        }
    }

    public void testWithShardSizeString() throws Exception {

        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("3", 8L);
        expected.put("2", 5L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testWithShardSizeStringSingleShard() throws Exception {

        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setRouting(routing1)
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 5L);
        expected.put("2", 4L);
        expected.put("3", 3L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKey())));
        }
    }

    public void testNoShardSizeTermOrderString() throws Exception {
        createIdx("type=keyword");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.key(true))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<String, Long> expected = new HashMap<>();
        expected.put("1", 8L);
        expected.put("2", 5L);
        expected.put("3", 8L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsString())));
        }
    }

    public void testNoShardSizeLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testShardSizeEqualsSizeLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .shardSize(3)
                    .showTermDocCountError(true)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 4L);
        Long expectedDocCount;
        for (Terms.Bucket bucket : buckets) {
            expectedDocCount = expected.get(bucket.getKeyAsNumber().intValue());
            // Doc count can vary when using concurrent segment search. See https://github.com/opensearch-project/OpenSearch/issues/11680
            assertTrue((bucket.getDocCount() == expectedDocCount) || bucket.getDocCount() + bucket.getDocCountError() >= expectedDocCount);
        }
    }

    public void testWithShardSizeLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testWithShardSizeLongSingleShard() throws Exception {

        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setRouting(routing1)
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3)); // we still only return 3 entries (based on the 'size' param)
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 5L);
        expected.put(2, 4L);
        expected.put(3, 3L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testNoShardSizeTermOrderLong() throws Exception {
        createIdx("type=long");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.key(true))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(2, 5L);
        expected.put(3, 8L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testNoShardSizeDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testShardSizeEqualsSizeDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .shardSize(3)
                    .showTermDocCountError(true)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 4L);
        Long expectedDocCount;
        for (Terms.Bucket bucket : buckets) {
            expectedDocCount = expected.get(bucket.getKeyAsNumber().intValue());
            // Doc count can vary when using concurrent segment search. See https://github.com/opensearch-project/OpenSearch/issues/11680
            assertTrue((bucket.getDocCount() == expectedDocCount) || bucket.getDocCount() + bucket.getDocCountError() >= expectedDocCount);
        }
    }

    public void testWithShardSizeDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(3, 8L);
        expected.put(2, 5L); // <-- count is now fixed
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testWithShardSizeDoubleSingleShard() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setRouting(routing1)
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key")
                    .size(3)
                    .collectMode(randomFrom(SubAggCollectionMode.values()))
                    .shardSize(5)
                    .order(BucketOrder.count(false))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 5L);
        expected.put(2, 4L);
        expected.put(3, 3L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    public void testNoShardSizeTermOrderDouble() throws Exception {
        createIdx("type=double");

        indexData();

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(matchAllQuery())
            .addAggregation(
                terms("keys").field("key").size(3).collectMode(randomFrom(SubAggCollectionMode.values())).order(BucketOrder.key(true))
            )
            .get();

        Terms terms = response.getAggregations().get("keys");
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(3));
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(1, 8L);
        expected.put(2, 5L);
        expected.put(3, 8L);
        for (Terms.Bucket bucket : buckets) {
            assertThat(bucket.getDocCount(), equalTo(expected.get(bucket.getKeyAsNumber().intValue())));
        }
    }

    @Override
    protected java.util.Collection<Class<? extends org.opensearch.plugins.Plugin>> nodePlugins() {
        java.util.List<Class<? extends org.opensearch.plugins.Plugin>> _mustangPlugins = new java.util.ArrayList<>();
        _mustangPlugins.add(org.opensearch.arrow.allocator.ArrowBasePlugin.class);
        _mustangPlugins.add(org.opensearch.arrow.flight.transport.FlightStreamPlugin.class);
        _mustangPlugins.add(org.opensearch.analytics.AnalyticsPlugin.class);
        _mustangPlugins.add(org.opensearch.be.datafusion.DataFusionPlugin.class);
        _mustangPlugins.add(org.opensearch.be.lucene.LucenePlugin.class);
        _mustangPlugins.add(org.opensearch.composite.CompositeDataFormatPlugin.class);
        _mustangPlugins.add(org.opensearch.parquet.ParquetDataFormatPlugin.class);
        _mustangPlugins.add(org.opensearch.dsl.DslQueryExecutorPlugin.class);
        _mustangPlugins.addAll(super.nodePlugins());
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
