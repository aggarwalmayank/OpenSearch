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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
 * Node-level cache of {@link UninvertedOrdinals}, keyed by (segment core key, field). Builds are
 * serialized node-wide; entries close with their segment core; the on-disk file is keyed by the
 * segment's stable id and survives restarts, so a re-opened segment maps it instead of rebuilding.
 */
public final class UninvertedOrdinalsCache {

    private static final Logger LOGGER = LogManager.getLogger(UninvertedOrdinalsCache.class);
    private static final double EVICTION_WATERMARK_FRACTION = 0.90d;

    /** Marks a (segment, field) whose ordinals failed coverage verification — do not retry. */
    private static final Map<Object, Set<String>> INELIGIBLE = new ConcurrentHashMap<>();
    private static final Map<Object, Map<String, CacheEntry>> CACHE = new ConcurrentHashMap<>();
    private static final Object BUILD_LOCK = new Object();

    /** Directories already created and swept of stale .tmp files this process lifetime. */
    private static final Set<Path> PREPARED_DIRS = ConcurrentHashMap.newKeySet();
    private static volatile boolean shuttingDown = false;

    /** Name of the per-shard ord-file directory, a sibling of the shard's store directory. */
    static final String ORDS_DIR_NAME = "parquet-ords";

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

    /**
     * Called once at plugin init: wipes the pre-move node-global directory (its files are
     * unreachable derived data; they rebuild on demand at the per-shard location).
     */
    public static void setOrdsDir(Path legacyDir) {
        shuttingDown = false;
        wipeLegacyDir(legacyDir);
    }

    /** Called at plugin close: aborts in-flight builds so node shutdown is not held hostage. */
    public static void shutdown() {
        shuttingDown = true;
    }

    /**
     * The segment's ord-file directory: {@code parquet-ords} beside the shard's store (like the
     * translog — never inside the store, whose unknown files recovery deletes). {@code null} when
     * the directory is not filesystem-backed: no shard folder to place or budget files in.
     */
    static Path resolveOrdsDir(org.apache.lucene.store.Directory directory) {
        org.apache.lucene.store.Directory unwrapped = org.apache.lucene.store.FilterDirectory.unwrap(directory);
        if (unwrapped instanceof org.apache.lucene.store.FSDirectory) {
            Path storeDir = ((org.apache.lucene.store.FSDirectory) unwrapped).getDirectory();
            Path shardDir = storeDir.getParent();
            if (shardDir != null) {
                return shardDir.resolve(ORDS_DIR_NAME);
            }
        }
        return null;
    }

    /** Creates the directory and sweeps stale {@code .tmp} files, once per process lifetime. */
    private static void prepareDir(Path ordsDir) throws IOException {
        if (PREPARED_DIRS.add(ordsDir) == false) {
            return;
        }
        Files.createDirectories(ordsDir);
        try (Stream<Path> listing = Files.list(ordsDir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                if (file.getFileName().toString().endsWith(".tmp")) {
                    Files.deleteIfExists(file);
                }
            }
        }
    }

    /**
     * Builds ordinals, retrying once (after deleting the file) when verification fails on a
     * PRE-EXISTING file — it may be stale from a crashed process. A failure on a fresh build is
     * genuine (unindexed stored values) and latches the field ineligible.
     */
    private static UninvertedOrdinals buildWithRetry(Path ordsDir, String fileKey, Terms terms, int maxDoc, long expectedNonNullDocs)
        throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        boolean preExisting = Files.exists(ordsDir.resolve(fileName));
        try {
            return UninvertedOrdinals.build(
                ordsDir,
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
            Files.deleteIfExists(ordsDir.resolve(fileName));
            return UninvertedOrdinals.build(
                ordsDir,
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

    /** Removes ord and tmp files from the pre-move node-global directory; nothing reads them now. */
    private static void wipeLegacyDir(Path dir) {
        try (Stream<Path> listing = Files.list(dir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                String name = file.getFileName().toString();
                if (name.endsWith(".ord") || name.endsWith(".tmp")) {
                    if (Files.deleteIfExists(file)) {
                        LOGGER.info("removed legacy ord file [{}] (ord files now live beside each shard's store)", name);
                    }
                }
            }
        } catch (NoSuchFileException e) {
            // nothing to migrate
        } catch (IOException e) {
            LOGGER.warn("legacy ords directory cleanup failed for [{}]: {}", dir, e.getMessage());
        }
    }

    /** Transient refusal: budget can be raised or freed, so it is never latched as INELIGIBLE. */
    private static final class BudgetExceededException extends IllegalStateException {
        BudgetExceededException(String message) {
            super(message);
        }
    }

    /**
     * Keeps a shard's ords directory within {@code max_disk_percent} of that shard's store size.
     * Over budget: evict least-recently-used unpinned files down to the watermark; if it still
     * does not fit, refuse the build (transient). No-op when the store size is unknowable.
     */
    private static void enforceDiskBudget(Path ordsDir, String fileKey, Terms terms, int maxDoc) throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        if (Files.exists(ordsDir.resolve(fileName))) {
            return;
        }
        long storeBytes = shardStoreBytes(ordsDir);
        if (storeBytes < 0) {
            return; // fallback dir or unreadable store: no meaningful base, do not refuse
        }
        long budget = (long) (storeBytes * ParquetDocValuesProducer.uninvertMaxDiskPercent() / 100.0d);
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
        try (Stream<Path> listing = Files.list(ordsDir)) {
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
                "uninverted ordinals disk budget exceeded for shard: "
                    + used
                    + "B used + "
                    + estimate
                    + "B needed > "
                    + budget
                    + "B ("
                    + ParquetDocValuesProducer.uninvertMaxDiskPercent()
                    + "% of "
                    + storeBytes
                    + "B shard store; parquet.docvalues.uninvert.max_disk_percent)"
            );
        }
    }

    /**
     * Shard store bytes: everything under the ords dir's parent except the ords dir and the
     * translog. Returns {@code -1} when unknowable (no parent, or walk failure) — callers treat
     * that as "budget not enforceable".
     */
    // package-private for tests
    static long shardStoreBytes(Path ordsDir) {
        Path shardDir = ordsDir.getParent();
        if (shardDir == null || Files.isDirectory(shardDir) == false) {
            return -1;
        }
        long[] total = { 0 };
        try {
            Files.walkFileTree(shardDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (dir.equals(ordsDir) || dir.getFileName().toString().equals("translog")) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    total[0] += attrs.size();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE; // file vanished mid-walk (merge, delete): skip
                }
            });
        } catch (IOException e) {
            LOGGER.debug("failed sizing shard store beside [{}]: {}", ordsDir, e.getMessage());
            return -1;
        }
        return total[0];
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
                // Core close happens after Lucene retires the reader, so no live leases remain.
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
                    Path ordsDir = resolveOrdsDir(segmentInfo.dir);
                    if (ordsDir == null) {
                        // No filesystem shard folder: permanent for this segment, latch like a
                        // coverage failure; the field is served by the streaming path.
                        LOGGER.warn(
                            "refusing uninverted ordinals for field [{}]: segment directory [{}] is not filesystem-backed",
                            field,
                            segmentInfo.dir.getClass().getName()
                        );
                        INELIGIBLE.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(field);
                        return null;
                    }
                    prepareDir(ordsDir);
                    enforceDiskBudget(ordsDir, fileKey, terms, leaf.maxDoc());
                    UninvertedOrdinals built = buildWithRetry(ordsDir, fileKey, terms, leaf.maxDoc(), expectedNonNullDocs);
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
