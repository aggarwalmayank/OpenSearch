/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;


import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.dsl.DslQueryExecutorPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base class for running existing search integration tests through the DSL/Calcite route.
 *
 * Loads all Mustang sandbox plugins so that SearchActionFilter intercepts all _search
 * requests and routes them through the Calcite pipeline. Existing transport-client test
 * code works unchanged — the interception is transparent.
 */
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public abstract class DslCompatTestCase extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ArrowBasePlugin.class);
        plugins.add(AnalyticsPlugin.class);
        plugins.add(DataFusionPlugin.class);
        plugins.add(LucenePlugin.class);
        plugins.add(CompositeDataFormatPlugin.class);
        plugins.add(ParquetDataFormatPlugin.class);
        plugins.add(DslQueryExecutorPlugin.class);
        return plugins;
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
