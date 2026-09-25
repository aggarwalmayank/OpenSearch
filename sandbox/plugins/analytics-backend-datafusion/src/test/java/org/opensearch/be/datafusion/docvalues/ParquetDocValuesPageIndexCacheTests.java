/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link ParquetDocValuesProducer#cachedPageIndex}: the per-column
 * {@link ColumnPageIndex} is loaded once per segment core and reused, failures are not cached, and a
 * concurrent first access shares one instance. The loader is injected so no native call is needed.
 */
public class ParquetDocValuesPageIndexCacheTests extends OpenSearchTestCase {

    /** A producer built through the test-seam constructor; its page-index cache is exercised directly. */
    private ParquetDocValuesProducer producer() {
        Path file = createTempDir().resolve("cache.parquet");
        return new ParquetDocValuesProducer(file, 0L, Settings.EMPTY, 100, null);
    }

    /** Minimal single-page index; identity is what the tests assert on, not its contents. */
    private static ColumnPageIndex pageIndex() {
        return new ColumnPageIndex(new long[] { 0 }, new long[] { 0 }, new long[] { 1 }, new long[] { 9 }, 100);
    }

    public void testSecondCallReturnsCachedInstanceWithoutReloading() throws Exception {
        ParquetDocValuesProducer producer = producer();
        AtomicInteger loads = new AtomicInteger();
        ColumnPageIndex fixed = pageIndex();

        ColumnPageIndex first = producer.cachedPageIndex("f", () -> {
            loads.incrementAndGet();
            return fixed;
        });
        ColumnPageIndex second = producer.cachedPageIndex("f", () -> {
            loads.incrementAndGet();
            return pageIndex(); // must never run; would return a different instance if it did
        });

        assertSame("second call must return the cached instance", first, second);
        assertEquals("loader must run only on the cold miss", 1, loads.get());
    }

    public void testDifferentFieldsLoadIndependently() throws Exception {
        ParquetDocValuesProducer producer = producer();
        ColumnPageIndex a = pageIndex();
        ColumnPageIndex b = pageIndex();

        assertSame(a, producer.cachedPageIndex("a", () -> a));
        assertSame(b, producer.cachedPageIndex("b", () -> b));
        // Each field keeps its own entry.
        assertSame(a, producer.cachedPageIndex("a", () -> { throw new AssertionError("field 'a' must be cached"); }));
        assertSame(b, producer.cachedPageIndex("b", () -> { throw new AssertionError("field 'b' must be cached"); }));
    }

    public void testLoaderFailureIsNotCachedAndRetried() throws Exception {
        ParquetDocValuesProducer producer = producer();
        AtomicInteger attempts = new AtomicInteger();
        ColumnPageIndex recovered = pageIndex();

        // First load throws: the exception propagates and nothing is cached.
        expectThrows(IOException.class, () -> producer.cachedPageIndex("f", () -> {
            attempts.incrementAndGet();
            throw new IOException("cold load failed");
        }));

        // Next call retries (failure not cached) and succeeds.
        ColumnPageIndex got = producer.cachedPageIndex("f", () -> {
            attempts.incrementAndGet();
            return recovered;
        });

        assertSame(recovered, got);
        assertEquals("failed load must not be cached, so the loader runs again", 2, attempts.get());
    }

    public void testConcurrentFirstAccessSharesOneInstance() throws Exception {
        ParquetDocValuesProducer producer = producer();
        int threads = 8;
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<ColumnPageIndex> results = new CopyOnWriteArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.execute(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        // Each thread's loader builds a distinct instance; only the race winner is stored.
                        results.add(producer.cachedPageIndex("f", () -> {
                            loads.incrementAndGet();
                            return pageIndex();
                        }));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                });
            }
            assertTrue("workers did not start", ready.await(10, TimeUnit.SECONDS));
            go.countDown(); // release all at once to force the cold-miss race
            pool.shutdown();
            assertTrue("workers did not finish", pool.awaitTermination(10, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals("every thread must observe a result", threads, results.size());
        ColumnPageIndex winner = results.get(0);
        for (ColumnPageIndex seen : results) {
            assertSame("all threads must observe the same cached instance", winner, seen);
        }
        assertTrue("loader must have run at least once", loads.get() >= 1);
        // Post-race, the cache is warm and the winner is returned without reloading.
        assertSame(winner, producer.cachedPageIndex("f", () -> { throw new AssertionError("cache must be warm"); }));
    }
}
