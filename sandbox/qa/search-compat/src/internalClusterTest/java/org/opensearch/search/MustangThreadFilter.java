/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class MustangThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        String n = t.getName();
        if (n == null) return false;
        if (n.matches("Thread-\\d+")) return true;
        if (n.contains("native-allocator") || n.contains("allocator-rebalancer")) return true;
        return n.contains("arrow-") || n.contains("Arrow")
            || n.contains("datafusion") || n.contains("DataFusion")
            || n.contains("flight-") || n.contains("Flight")
            || n.contains("tokio-")
            || n.contains("Netty") || n.contains("epollEventLoop") || n.contains("nioEventLoop")
            || n.contains("liquidcache") || n.contains("LiquidCache")
            || n.contains("foyer") || n.contains("Foyer")
            || n.contains("analytics-") || n.contains("Analytics")
            || n.contains("mustang")
            || n.contains("parquet-") || n.contains("Parquet")
            || n.contains("calcite") || n.contains("Calcite");
    }
}
