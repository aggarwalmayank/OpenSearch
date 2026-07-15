/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

/**
 * Runtime gate for the Parquet DocValues skipper. Backed by the cluster setting
 * {@code plugins.parquet.doc_values_skipper.enabled} — the plugin wires an
 * update-consumer that calls {@link #set(boolean)} on setting change.
 *
 * <p>When disabled, {@link ParquetDocValuesProducer#getSkipper} returns {@code null}
 * and Lucene falls back to a linear doc-values scan (baseline for perf comparison).
 * When enabled, the producer returns a {@link ParquetDocValuesSkipper} that skips
 * whole Parquet pages using per-page min/max stats.
 *
 * <p>Singleton because the skipper is chosen deep inside Lucene's per-segment
 * reader — no plugin-owned context is threaded through. A volatile boolean is fine
 * here: reads are on the query hot path but writes are rare (settings update).
 */
public final class DocValuesSkipperGate {

    public static final DocValuesSkipperGate INSTANCE = new DocValuesSkipperGate();

    private volatile boolean enabled = false;

    private DocValuesSkipperGate() {}

    public boolean isEnabled() {
        return enabled;
    }

    public void set(boolean enabled) {
        this.enabled = enabled;
    }
}
