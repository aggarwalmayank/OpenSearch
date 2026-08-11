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

package org.opensearch.index.fielddata.ordinals;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Utility class to build global ordinals.
 *
 * @opensearch.internal
 */
public enum GlobalOrdinalsBuilder {
    ;

    /**
     * How many per-segment field-data loads inside {@link #build} run concurrently.
     *
     * <p>{@code 1} (default) is the original behavior: one segment at a time. A value {@code > 1}
     * loads that many segments on a bounded pool, overlapping expensive cold loads (e.g. the
     * composite codec's postings→ordinal read/uninvert, whose cost is dominated by scattered mmap
     * page faults) across segments; the cheap native-doc-values load is unaffected in practice.
     * This pool size is the single bound on build concurrency.
     *
     * <p>The value is owned by whichever component wires it (currently the parquet-data-format
     * plugin, via a dynamic cluster setting); {@link #setBuildConcurrency} pushes updates here and
     * {@link #build} reads this field live. Left at {@code 1}, this class behaves exactly as before.
     */
    private static volatile int buildConcurrency = 1;

    /** Names + daemonizes build threads so they are recognizable in dumps and never block shutdown. */
    private static final ThreadFactory BUILD_THREAD_FACTORY = new ThreadFactory() {
        private final AtomicInteger n = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "global-ordinals-build-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    };

    /** Shared daemon pool, (re)sized to match {@link #buildConcurrency}. Guarded by the class monitor. */
    private static ExecutorService loadPool;
    private static int loadPoolSize;

    /**
     * Sets the per-segment load concurrency. Values below 1 are clamped to 1 (serial). Takes effect
     * on the next {@link #build}; an already-running build keeps the pool it started with.
     */
    public static void setBuildConcurrency(int concurrency) {
        buildConcurrency = Math.max(1, concurrency);
    }

    /** Returns a pool sized to {@code concurrency}, rebuilding it if the value changed since last use. */
    private static synchronized ExecutorService loadPool(int concurrency) {
        if (loadPool == null || loadPoolSize != concurrency) {
            ExecutorService old = loadPool;
            loadPool = Executors.newFixedThreadPool(concurrency, BUILD_THREAD_FACTORY);
            loadPoolSize = concurrency;
            if (old != null) {
                old.shutdown(); // let in-flight builds drain on the old pool; never interrupt them
            }
        }
        return loadPool;
    }

    /**
     * Build global ordinals for the provided {@link IndexReader}.
     */
    public static IndexOrdinalsFieldData build(
        final IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        CircuitBreakerService breakerService,
        Logger logger,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) throws IOException {
        return build(indexReader, indexFieldData, breakerService, logger, scriptFunction, () -> {});
    }

    /**
     * Build global ordinals for the provided {@link IndexReader}, with periodic cancellation checks
     * between segment iterations.
     */
    public static IndexOrdinalsFieldData build(
        final IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        CircuitBreakerService breakerService,
        Logger logger,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction,
        Runnable cancellationCheck
    ) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTimeNS = System.nanoTime();

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] subs = new SortedSetDocValues[indexReader.leaves().size()];
        // cancellableSubs wraps each segment's SortedSetDocValues with a cancellation-aware termsEnum()
        // for OrdinalMap.build(), which only calls termsEnum() and getValueCount().
        // atomicFD retains the original unwrapped values to preserve SingletonSortedSetDocValues
        // type for DocValues.unwrapSingleton().
        final SortedSetDocValues[] cancellableSubs = new SortedSetDocValues[indexReader.leaves().size()];
        final int numLeaves = indexReader.leaves().size();
        final int concurrency = buildConcurrency;
        final ExecutorService pool = concurrency > 1 ? loadPool(concurrency) : null;
        if (pool != null && numLeaves > 1) {
            // Parallel per-segment load. Each leaf index is independent: load() + getOrdinalsValues()
            // is where an expensive codec build (e.g. postings uninvert into an .ord) fires, and that
            // is exactly the cold work we want to overlap across segments. OrdinalMap.build below
            // still runs serially once every sub is materialized. Concurrency is bounded by the pool
            // size; the underlying per-segment build cache applies its own admission control.
            final List<Callable<Void>> tasks = new ArrayList<>(numLeaves);
            for (int i = 0; i < numLeaves; ++i) {
                final int idx = i;
                tasks.add(() -> {
                    cancellationCheck.run();
                    atomicFD[idx] = indexFieldData.load(indexReader.leaves().get(idx));
                    subs[idx] = atomicFD[idx].getOrdinalsValues();
                    cancellableSubs[idx] = new CancellableTermsSortedSetDocValues(subs[idx], cancellationCheck);
                    return null;
                });
            }
            try {
                for (Future<Void> f : pool.invokeAll(tasks)) {
                    f.get();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while building global ordinals for " + indexFieldData.getFieldName(), e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
                throw new IOException("failed building global ordinals for " + indexFieldData.getFieldName(), cause);
            }
        } else {
            for (int i = 0; i < numLeaves; ++i) {
                cancellationCheck.run();
                atomicFD[i] = indexFieldData.load(indexReader.leaves().get(i));
                subs[i] = atomicFD[i].getOrdinalsValues();
                cancellableSubs[i] = new CancellableTermsSortedSetDocValues(subs[i], cancellationCheck);
            }
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, cancellableSubs, PackedInts.DEFAULT);
        final long memorySizeInBytes = ordinalMap.ramBytesUsed();
        breakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(memorySizeInBytes);

        if (logger.isDebugEnabled()) {
            logger.debug(
                "global-ordinals [{}][{}] took [{}]",
                indexFieldData.getFieldName(),
                ordinalMap.getValueCount(),
                new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS)
            );
        }
        return new GlobalOrdinalsIndexFieldData(
            indexFieldData.getFieldName(),
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            memorySizeInBytes,
            scriptFunction
        );
    }

    public static IndexOrdinalsFieldData buildEmpty(IndexReader indexReader, IndexOrdinalsFieldData indexFieldData) throws IOException {
        assert indexReader.leaves().size() > 1;

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] subs = new SortedSetDocValues[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            atomicFD[i] = new AbstractLeafOrdinalsFieldData(AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION) {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    return DocValues.emptySortedSet();
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }

                @Override
                public Collection<Accountable> getChildResources() {
                    return Collections.emptyList();
                }

                @Override
                public void close() {}
            };
            subs[i] = atomicFD[i].getOrdinalsValues();
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, subs, PackedInts.DEFAULT);
        return new GlobalOrdinalsIndexFieldData(
            indexFieldData.getFieldName(),
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            0,
            AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
        );
    }

    /**
     * Thin wrapper around {@link SortedSetDocValues} that adds cancellation checks
     * to {@link #termsEnum()} iteration. Used only for the {@code subs} array passed
     * to {@link OrdinalMap#build}, which only calls {@link #termsEnum()} and
     * {@link #getValueCount()}. This avoids wrapping the stored field data values
     * which must preserve their concrete type for {@code DocValues.unwrapSingleton()}.
     */
    private static class CancellableTermsSortedSetDocValues extends SortedSetDocValues {
        private final SortedSetDocValues in;
        private final Runnable cancellationCheck;

        CancellableTermsSortedSetDocValues(SortedSetDocValues in, Runnable cancellationCheck) {
            this.in = in;
            this.cancellationCheck = cancellationCheck;
        }

        @Override
        public TermsEnum termsEnum() throws IOException {
            TermsEnum te = in.termsEnum();
            return new FilterLeafReader.FilterTermsEnum(te) {
                private static final int CHECK_INTERVAL = (1 << 10) - 1; // 1023
                private int calls;

                @Override
                public BytesRef next() throws IOException {
                    if ((calls++ & CHECK_INTERVAL) == 0) {
                        cancellationCheck.run();
                    }
                    return in.next();
                }
            };
        }

        @Override
        public long getValueCount() {
            return in.getValueCount();
        }

        // Methods below are required by SortedSetDocValues but not called by OrdinalMap.build()
        @Override
        public int nextDoc() throws IOException {
            return in.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return in.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public long nextOrd() throws IOException {
            return in.nextOrd();
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return in.lookupOrd(ord);
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }

}
