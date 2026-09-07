/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Node-level cache of {@link UninvertedOrdinals}, keyed by (segment core key, field).
 *
 * <p>Builds are serialized node-wide (one postings sweep at a time — the transient packed buffer
 * and the sweep's CPU never stack). Entries are evicted (and their mapped files closed) by the
 * segment core's closed-listener; the on-disk artifact is keyed by the segment's stable id and
 * survives restarts, so a re-opened segment maps the existing file instead of rebuilding.
 */
public final class UninvertedOrdinalsCache {

    private static final Logger LOGGER = LogManager.getLogger(UninvertedOrdinalsCache.class);
    private static final double EVICTION_WATERMARK_FRACTION = 0.90d;

    /** Marks a (segment, field) whose ordinals failed coverage verification — do not retry. */
    private static final Map<Object, Set<String>> INELIGIBLE = new ConcurrentHashMap<>();
    private static final Map<Object, Map<String, CacheEntry>> CACHE = new ConcurrentHashMap<>();
    private static final Object BUILD_LOCK = new Object();

    /** Default under java.io.tmpdir (unit tests); the plugin points this at the node data path. */
    private static volatile Path ORDS_DIR = Path.of(System.getProperty("java.io.tmpdir"), "opensearch-parquet-ords");
    private static volatile boolean shuttingDown = false;

    private UninvertedOrdinalsCache() {}

    private static final class CacheEntry {
        private final UninvertedOrdinals ords;
        private volatile long lastUsedMillis;
        private int inUse;
        private boolean evicted;

        private CacheEntry(UninvertedOrdinals ords) {
            this.ords = ords;
            this.lastUsedMillis = System.currentTimeMillis();
        }

        private synchronized Lease tryAcquire() {
            if (evicted) {
                return null;
            }
            inUse++;
            lastUsedMillis = System.currentTimeMillis();
            return new Lease(this);
        }

        private synchronized void release() {
            if (inUse <= 0) {
                throw new IllegalStateException("uninverted ordinals lease underflow for " + ords.fileName());
            }
            inUse--;
            lastUsedMillis = System.currentTimeMillis();
        }

        private synchronized boolean tryMarkEvicted() {
            if (evicted || inUse > 0) {
                return false;
            }
            evicted = true;
            return true;
        }

        private String fileName() {
            return ords.fileName();
        }

        private long lastUsedMillis() {
            return lastUsedMillis;
        }
    }

    private static final class CacheLocation {
        private final Map<String, CacheEntry> owner;
        private final String field;
        private final CacheEntry entry;

        private CacheLocation(Map<String, CacheEntry> owner, String field, CacheEntry entry) {
            this.owner = owner;
            this.field = field;
            this.entry = entry;
        }
    }

    private static final class EvictionCandidate {
        private final Path path;
        private final String fileName;
        private final long sizeInBytes;
        private final long lastUsedMillis;
        private final CacheLocation cached;

        private EvictionCandidate(Path path, String fileName, long sizeInBytes, long lastUsedMillis, CacheLocation cached) {
            this.path = path;
            this.fileName = fileName;
            this.sizeInBytes = sizeInBytes;
            this.lastUsedMillis = lastUsedMillis;
            this.cached = cached;
        }
    }

    /** Request-scoped handle that keeps a cache entry in-use until the reader closes. */
    static final class Lease implements AutoCloseable {
        private final CacheEntry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(CacheEntry entry) {
            this.entry = entry;
        }

        UninvertedOrdinals ordinals() {
            return entry.ords;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                entry.release();
            }
        }
    }

    /** Called once at plugin init: ord files live with the node's data, not in tmp. */
    public static void setOrdsDir(Path dir) {
        ORDS_DIR = dir;
        shuttingDown = false;
        cleanupAtStartup(dir);
    }

    /** Called at plugin close: aborts in-flight builds so node shutdown is not held hostage. */
    public static void shutdown() {
        shuttingDown = true;
    }

    /**
     * Builds ordinals, retrying ONCE after deleting the on-disk file when verification fails on
     * a pre-existing file: a file left by a crashed or killed process may be stale for reasons a
     * rebuild fixes (segment data moved on after an unclean stop). Only a failure on a FRESH
     * build is genuine (unindexed stored values) and latches the field ineligible.
     */
    private static UninvertedOrdinals buildWithRetry(String fileKey, Terms terms, int maxDoc, long expectedNonNullDocs) throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        boolean preExisting = Files.exists(ORDS_DIR.resolve(fileName));
        try {
            return UninvertedOrdinals.build(
                ORDS_DIR,
                fileKey,
                terms,
                maxDoc,
                expectedNonNullDocs,
                () -> shuttingDown || Thread.currentThread().isInterrupted()
            );
        } catch (IllegalStateException e) {
            if (preExisting == false) {
                throw e;
            }
            LOGGER.warn("ord file [{}] failed verification ({}); deleting and rebuilding once", fileName, e.getMessage());
            Files.deleteIfExists(ORDS_DIR.resolve(fileName));
            return UninvertedOrdinals.build(
                ORDS_DIR,
                fileKey,
                terms,
                maxDoc,
                expectedNonNullDocs,
                () -> shuttingDown || Thread.currentThread().isInterrupted()
            );
        }
    }

    private static long evictionTargetBytes(long budget) {
        return Math.min(budget, (long) Math.floor(budget * EVICTION_WATERMARK_FRACTION));
    }

    private static long fileLastUsedMillis(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }

    private static void cleanupAtStartup(Path dir) {
        long budget = ParquetDocValuesProducer.uninvertMaxDiskBytes();
        long target = evictionTargetBytes(budget);
        long used = 0;
        List<Path> files = new ArrayList<>();
        try (Stream<Path> listing = Files.list(dir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                if (file.getFileName().toString().endsWith(".tmp")) {
                    Files.deleteIfExists(file);
                } else {
                    used += Files.size(file);
                    files.add(file);
                }
            }
        } catch (NoSuchFileException e) {
            return;
        } catch (IOException e) {
            LOGGER.warn("ords directory startup cleanup failed for [{}]: {}", dir, e.getMessage());
            return;
        }
        if (used <= budget) {
            return;
        }
        files.sort(Comparator.comparingLong(UninvertedOrdinalsCache::fileLastUsedMillis));
        for (Path victim : files) {
            if (used <= target) {
                break;
            }
            try {
                long size = Files.size(victim);
                Files.deleteIfExists(victim);
                used -= size;
                LOGGER.info("reclaimed ord file [{}] at startup (over budget)", victim.getFileName());
            } catch (IOException e) {
                // skip
            }
        }
    }

    /** Transient refusal: budget can be raised or freed, so it is never latched as INELIGIBLE. */
    private static final class BudgetExceededException extends IllegalStateException {
        BudgetExceededException(String message) {
            super(message);
        }
    }

    /**
     * Keeps the ords directory within {@code parquet.docvalues.uninvert.max_disk_bytes}. Active
     * files are those with {@code inUse > 0}. When eviction starts, reclaim least-recently-used
     * evictable files until projected usage reaches the watermark target; if that cannot bring
     * usage under the hard limit, the build is refused.
     */
    private static void enforceDiskBudget(String fileKey, Terms terms, int maxDoc) throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        if (Files.exists(ORDS_DIR.resolve(fileName))) {
            return;
        }
        long budget = ParquetDocValuesProducer.uninvertMaxDiskBytes();
        long estimate = UninvertedOrdinals.estimatedDiskBytes(Math.max(terms.size(), 0), maxDoc);
        long used = 0;
        long target = evictionTargetBytes(budget);

        Map<String, CacheLocation> cachedFiles = new HashMap<>();
        for (Map<String, CacheEntry> perSegment : CACHE.values()) {
            for (Map.Entry<String, CacheEntry> fieldEntry : perSegment.entrySet()) {
                CacheEntry entry = fieldEntry.getValue();
                cachedFiles.put(entry.fileName(), new CacheLocation(perSegment, fieldEntry.getKey(), entry));
            }
        }

        List<EvictionCandidate> candidates = new ArrayList<>();
        try (Stream<Path> listing = Files.list(ORDS_DIR)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                long size = Files.size(file);
                used += size;
                String candidateName = file.getFileName().toString();
                CacheLocation cached = cachedFiles.get(candidateName);
                long lastUsedMillis = cached != null ? cached.entry.lastUsedMillis() : fileLastUsedMillis(file);
                candidates.add(new EvictionCandidate(file, candidateName, size, lastUsedMillis, cached));
            }
        } catch (NoSuchFileException e) {
            return;
        }

        if (used + estimate <= budget) {
            return;
        }

        candidates.sort(Comparator.comparingLong(candidate -> candidate.lastUsedMillis));
        for (EvictionCandidate victim : candidates) {
            if (used + estimate <= target) {
                break;
            }
            if (victim.cached != null) {
                if (victim.cached.entry.tryMarkEvicted() == false) {
                    continue;
                }
                victim.cached.owner.remove(victim.cached.field, victim.cached.entry);
                try {
                    victim.cached.entry.ords.close();
                } catch (IOException e) {
                    LOGGER.debug("failed closing evicted ords [{}]: {}", victim.fileName, e.getMessage());
                }
            }
            try {
                if (Files.deleteIfExists(victim.path)) {
                    used -= victim.sizeInBytes;
                    LOGGER.info("reclaimed ord file [{}] to satisfy disk budget", victim.fileName);
                }
            } catch (IOException e) {
                LOGGER.debug("failed reclaiming ord file [{}]: {}", victim.fileName, e.getMessage());
            }
        }

        if (used + estimate > budget) {
            throw new BudgetExceededException(
                "uninverted ordinals disk budget exceeded: "
                    + used
                    + "B used + "
                    + estimate
                    + "B needed > "
                    + budget
                    + "B (parquet.docvalues.uninvert.max_disk_bytes)"
            );
        }
    }

    /**
     * Acquires the uninverted ordinals for {@code field}, building or re-mapping on first use.
     * Returns {@code null} when the segment lacks a core cache identity or a terms index.
     */
    static Lease acquire(LeafReader leaf, SegmentInfo segmentInfo, String field, long expectedNonNullDocs) throws IOException {
        IndexReader.CacheHelper helper = leaf.getCoreCacheHelper();
        Terms terms = leaf.terms(field);
        if (helper == null || terms == null) {
            return null;
        }
        Object key = helper.getKey();
        Set<String> ineligible = INELIGIBLE.get(key);
        if (ineligible != null && ineligible.contains(field)) {
            return null;
        }
        Map<String, CacheEntry> perSegment = CACHE.computeIfAbsent(key, k -> {
            helper.addClosedListener(closedKey -> {
                // Core close happens only after Lucene has retired the segment reader, so there
                // should be no live leases here; unlike budget eviction we rely on that lifecycle
                // guarantee rather than checking inUse before closing entries.
                INELIGIBLE.remove(closedKey);
                Map<String, CacheEntry> removed = CACHE.remove(closedKey);
                if (removed != null) {
                    for (CacheEntry entry : removed.values()) {
                        try {
                            entry.ords.close();
                        } catch (IOException e) {
                            // Segment is going away; nothing actionable.
                        }
                    }
                }
            });
            return new ConcurrentHashMap<>();
        });

        for (;;) {
            CacheEntry cached = perSegment.get(field);
            if (cached != null) {
                Lease lease = cached.tryAcquire();
                if (lease != null) {
                    return lease;
                }
                perSegment.remove(field, cached);
                continue;
            }
            synchronized (BUILD_LOCK) {
                cached = perSegment.get(field);
                if (cached != null) {
                    Lease lease = cached.tryAcquire();
                    if (lease != null) {
                        return lease;
                    }
                    perSegment.remove(field, cached);
                    continue;
                }
                String fileKey = StringHelper.idToString(segmentInfo.getId()) + "-" + field;
                try {
                    enforceDiskBudget(fileKey, terms, leaf.maxDoc());
                    UninvertedOrdinals built = buildWithRetry(fileKey, terms, leaf.maxDoc(), expectedNonNullDocs);
                    CacheEntry entry = new CacheEntry(built);
                    Lease lease = entry.tryAcquire();
                    if (lease == null) {
                        throw new IllegalStateException("new uninverted ordinals entry unexpectedly unavailable");
                    }
                    perSegment.put(field, entry);
                    return lease;
                } catch (BudgetExceededException e) {
                    LOGGER.warn("refusing uninverted ordinals for field [{}]: {}", field, e.getMessage());
                    return null;
                } catch (IllegalStateException e) {
                    LOGGER.warn("refusing uninverted ordinals for field [{}]: {}", field, e.getMessage());
                    INELIGIBLE.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(field);
                    return null;
                }
            }
        }
    }
}
