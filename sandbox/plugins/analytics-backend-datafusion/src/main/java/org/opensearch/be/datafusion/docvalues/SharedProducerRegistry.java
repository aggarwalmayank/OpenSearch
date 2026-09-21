/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level registry of segment-lifetime {@link ParquetDocValuesProducer}s, one per segment core.
 * Caches (fielddata, global ordinals) retain search-time wrappers past their request and call
 * doc-values accessors later; those late calls are served here instead of the closed request
 * producer. Created on first use, closed by the segment core's closed-listener.
 */
final class SharedProducerRegistry {

    private static final Map<Object, ParquetDocValuesProducer> PRODUCERS = new ConcurrentHashMap<>();

    private SharedProducerRegistry() {}

    /**
     * The shared producer for the segment identified by {@code coreHelper}, created on first use.
     * Returns {@code null} when there is no core cache helper: no safe lifecycle to attach to, so
     * callers must fail rather than leak.
     */
    static ParquetDocValuesProducer get(IndexReader.CacheHelper coreHelper, SegmentReadState segmentReadState, MapperService mapperService)
        throws IOException {
        if (coreHelper == null) {
            return null;
        }
        Object key = coreHelper.getKey();
        ParquetDocValuesProducer existing = PRODUCERS.get(key);
        if (existing != null) {
            return existing;
        }
        synchronized (PRODUCERS) {
            existing = PRODUCERS.get(key);
            if (existing != null) {
                return existing;
            }
            ParquetDocValuesProducer created = new ParquetDocValuesProducer(segmentReadState, mapperService, true);
            PRODUCERS.put(key, created);
            coreHelper.addClosedListener(k -> {
                ParquetDocValuesProducer removed = PRODUCERS.remove(k);
                if (removed != null) {
                    try {
                        removed.close();
                    } catch (IOException e) {
                        // Segment is going away; nothing actionable.
                    }
                }
            });
            return created;
        }
    }

    /** Number of live shared producers (tests / diagnostics). */
    static int size() {
        return PRODUCERS.size();
    }
}
