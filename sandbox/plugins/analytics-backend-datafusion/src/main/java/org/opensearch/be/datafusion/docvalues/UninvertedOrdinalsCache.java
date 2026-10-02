/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.KeyedLock;
import org.opensearch.core.common.breaker.CircuitBreaker;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.Cleaner;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * Node-level cache of {@link UninvertedOrdinals}, keyed by (segment core key, field). Builds of
 * different files run in parallel (each reserving its build heap on the fielddata breaker) while
 * mutations of one file serialize on its per-file lock. The on-disk file is keyed by the segment's
 * backing parquet file name and survives restarts.
 */
public final class UninvertedOrdinalsCache {

    private static final Logger logger = LogManager.getLogger(UninvertedOrdinalsCache.class);

    /** Runs lease releases when their holders are garbage-collected; see releaseWhenUnreachable. */
    private static final Cleaner UNREACHABLE_LEASE_CLEANER = Cleaner.create();

    /** Loaded ordinals by segment core key, then field name; entries are removed when the segment core closes. */
    private static final Map<Object, Map<String, FieldOrdinalsEntry>> ORDINALS_BY_SEGMENT = new ConcurrentHashMap<>();

    /**
     * One lock per ord FILE: every mutation of a file (build, load, delete) runs under its lock,
     * so builds of different fields proceed in parallel while two threads can never build,
     * or build-vs-delete, the same file concurrently. Entries are removed with the
     * owning segment core.
     */
    private static final KeyedLock<String> FILE_LOCKS = new KeyedLock<>();

    /** Charged for a from-scratch build's transient heap; set to the node fielddata breaker at plugin init. */
    private static volatile CircuitBreaker buildBreaker;

    /** Idle time after which an unused ord file is deleted; any use resets the clock. */
    private static volatile long deleteUnusedAfterMillis = TimeValue.timeValueDays(7).millis();

    /** Node data roots, for the deletion pass to find every shard's ords dir. Set by the plugin. */
    private static volatile Path[] dataRoots = new Path[0];

    /**
     * How often an in-use file's last-modified time is refreshed to record "last used" on disk
     * (for the deletion pass's cold-file phase, which judges idleness by the file's last-modified time
     * after the in-memory clock is lost to a restart). The on-disk record may lag true last use
     * by at most this much — harmless against a threshold measured in days, and it caps the cost at
     * one metadata write per file per interval instead of one per query.
     */
    private static final long LAST_USED_PERSIST_INTERVAL_MILLIS = TimeValue.timeValueMinutes(5).millis();

    private static volatile boolean shuttingDown = false;

    private UninvertedOrdinalsCache() {}

    /**
     * Called once at plugin init: begins a node lifecycle by re-arming the cache (the class is
     * static and outlives the node in same-JVM restarts, notably tests, where a previous node's
     * {@link #shutdown()} would otherwise keep refusing builds).
     */
    public static void start() {
        shuttingDown = false;
    }

    /** Called at plugin close: aborts in-flight builds so node shutdown is not held hostage. */
    public static void shutdown() {
        shuttingDown = true;
    }

    /** Idle time before an unused ord file is deleted; any use resets the clock. */
    public static void setDeleteUnusedAfter(TimeValue deleteUnusedAfter) {
        deleteUnusedAfterMillis = deleteUnusedAfter.millis();
    }

    /** Fielddata breaker charged for from-scratch build heap; set by the plugin at startup. */
    public static void setBuildBreaker(CircuitBreaker breaker) {
        buildBreaker = Objects.requireNonNull(breaker, "build breaker must not be null; pass the node FIELDDATA breaker");
    }

    /** Node data roots so the deletion pass can find every shard's ords dir, incl. never-queried ones. */
    public static void setDataRoots(Path[] roots) {
        // Null can only mean the caller forgot to pass the node's data paths — fail at startup
        // rather than accept it and silently disable the cold-file deletion.
        dataRoots = Objects.requireNonNull(roots, "data roots must not be null").clone();
    }

    /**
     * Releases {@code lease} when {@code holder} is garbage-collected — i.e. when the query
     * reading through it has let go of it. Reader close is not a reliable release signal:
     * OpenSearch's fielddata cache retains leaf readers and reads through them after close.
     */
    static void releaseWhenUnreachable(Object holder, Lease lease) {
        // The action must not reference holder, or it would never become collectable.
        UNREACHABLE_LEASE_CLEANER.register(holder, lease::close);
    }

    /**
     * Acquires the uninverted ordinals for {@code field}, building or re-mapping on first use.
     * Returns {@code null} when the segment lacks a core cache identity or a terms index.
     */
    static Lease acquire(LeafReader leaf, SegmentInfo segmentInfo, String field, long expectedNonNullDocs) throws IOException {
        IndexReader.CacheHelper segmentCore = leaf.getCoreCacheHelper();
        Terms terms = leaf.terms(field);
        if (segmentCore == null || terms == null) {
            return null;
        }
        Object segmentCoreKey = segmentCore.getKey();
        Map<String, FieldOrdinalsEntry> perSegment = ORDINALS_BY_SEGMENT.computeIfAbsent(segmentCoreKey, k -> {
            segmentCore.addClosedListener(closedKey -> {
                // Core close follows the last reader's close, so no search can still read these ordinals.
                Map<String, FieldOrdinalsEntry> segmentEntries = ORDINALS_BY_SEGMENT.get(closedKey);
                if (segmentEntries != null) {
                    for (FieldOrdinalsEntry entry : segmentEntries.values()) {
                        try {
                            entry.ords.close();
                        } catch (IOException e) {
                            // Segment is going away; nothing actionable.
                        }
                    }
                    // Removed only after closing, so the delete pass never deletes a file that is still mapped.
                    ORDINALS_BY_SEGMENT.remove(closedKey);
                }
            });
            return new ConcurrentHashMap<>();
        });

        Lease lease = leaseExistingEntry(perSegment, field);
        if (lease != null) {
            return lease;
        }
        // Key the ord file by the segment's backing parquet file, so a removed parquet file's ord
        // files can be deleted from the parquet file name alone (deleteOrdFilesOfDeletedParquetFiles).
        String parquetFile = segmentInfo.getAttribute(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE);
        if (parquetFile == null || parquetFile.isEmpty()) {
            throw new IllegalStateException(
                "segment ["
                    + segmentInfo.name
                    + "] has no ["
                    + ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE
                    + "] attribute; cannot locate its uninverted-ordinal file"
            );
        }
        String parquetFileStem = OrdFilePaths.parquetFileStem(Path.of(parquetFile).getFileName().toString());
        String fileKey = parquetFileStem + "-" + field;
        try (Releasable ignored = FILE_LOCKS.acquire(OrdFilePaths.ordFileName(fileKey))) {
            // Re-check under the lock: another query may have built the entry while we waited.
            lease = leaseExistingEntry(perSegment, field);
            if (lease != null) {
                return lease;
            }
            return loadOrBuildOrdinals(perSegment, segmentInfo, segmentCoreKey, field, fileKey, terms, leaf.maxDoc(), expectedNonNullDocs);
        }
    }

    /**
     * Leases the field's existing entry; {@code null} when the field has none. An entry the
     * deletion pass evicted between our map read and the lease attempt is unlinked (only if still
     * mapped, never displacing a newer entry) so the caller can proceed to build fresh.
     */
    private static Lease leaseExistingEntry(Map<String, FieldOrdinalsEntry> perSegment, String field) {
        FieldOrdinalsEntry existing = perSegment.get(field);
        if (existing == null) {
            return null;
        }
        Lease lease = existing.tryAcquire();
        if (lease != null) {
            existing.persistLastUsedTimeIfDue(System.currentTimeMillis());
            return lease;
        }
        perSegment.remove(field, existing);
        return null;
    }

    /**
     * Loads the ord file, or builds it from postings under a build permit, then caches the entry
     * and returns its first lease. Must run under the file's lock. Returns {@code null} only when
     * the segment directory is not filesystem-backed (ordinals unavailable in this environment).
     * Verification failures propagate and fail the query: a file that cannot be built correctly
     * is an integrity problem to surface, not a condition to hide.
     */
    private static Lease loadOrBuildOrdinals(
        Map<String, FieldOrdinalsEntry> perSegment,
        SegmentInfo segmentInfo,
        Object segmentCoreKey,
        String field,
        String fileKey,
        Terms terms,
        int maxDoc,
        long expectedNonNullDocs
    ) throws IOException {
        Path ordsDir = OrdFilePaths.resolveOrdsDir(segmentInfo.dir);
        if (ordsDir == null) {
            logger.warn(
                "refusing uninverted ordinals for field [{}]: segment directory [{}] is not filesystem-backed",
                field,
                segmentInfo.dir.getClass().getName()
            );
            return null;
        }
        OrdFilePaths.prepareDir(ordsDir);
        // An existing usable file is only memory-mapped, so it needs no reservation. load()
        // deletes an invalid leftover and returns null, sending us to the build path.
        UninvertedOrdinals built = UninvertedOrdinals.load(ordsDir, fileKey, terms, maxDoc, expectedNonNullDocs);
        if (built == null) {
            // From-scratch build holds a transient maxDoc-by-bits heap buffer; reserve it on the
            // fielddata breaker before allocating and release the reservation once the build ends.
            CircuitBreaker breaker = buildBreaker;
            if (breaker == null) {
                throw new IllegalStateException("ord file build breaker not set; DataFusionPlugin must register OrdFileBuildBreakerBinder");
            }
            long reserve = UninvertedOrdinals.buildHeapBytes(maxDoc, terms.size(), expectedNonNullDocs);
            breaker.addEstimateBytesAndMaybeBreak(reserve, "ord_file_build:" + OrdFilePaths.ordFileName(fileKey));
            try {
                built = UninvertedOrdinals.build(
                    ordsDir,
                    fileKey,
                    terms,
                    maxDoc,
                    expectedNonNullDocs,
                    () -> shuttingDown || Thread.currentThread().isInterrupted()
                );
            } finally {
                breaker.addWithoutBreaking(-reserve);
            }
        }
        FieldOrdinalsEntry entry = new FieldOrdinalsEntry(built, ordsDir.resolve(OrdFilePaths.ordFileName(fileKey)));
        Lease lease = entry.tryAcquire();
        if (lease == null) {
            throw new IllegalStateException("new uninverted ordinals entry unexpectedly unavailable");
        }
        perSegment.put(field, entry);
        return lease;
    }

    /** Runs one deletion pass; scheduled by the plugin every few minutes. */
    public static void deleteUnusedOrdFiles() {
        deleteUnusedOrdFiles(System.currentTimeMillis());
    }

    /**
     * Deletes ord files unused for longer than the configured threshold. In-use files
     * are protected by the lease refcount ({@link FieldOrdinalsEntry#tryMarkEvicted}); deletions run
     * under the file's mutation lock so they can never race a concurrent build or load.
     */
    // package-private overload for deterministic tests
    static void deleteUnusedOrdFiles(long now) {
        long threshold = deleteUnusedAfterMillis;
        evictIdleEntriesInCache(now, threshold);
        deleteExpiredFilesOnDisk(now, threshold);
    }

    /**
     * Evicts cache entries idle longer than the threshold: close the memory-map, then delete the file.
     * The in-memory last-used time (refreshed on every lease) is the idleness clock. Entries a
     * query is using are skipped and retried next pass.
     */
    private static void evictIdleEntriesInCache(long now, long threshold) {
        for (Map<String, FieldOrdinalsEntry> perSegment : ORDINALS_BY_SEGMENT.values()) {
            for (Map.Entry<String, FieldOrdinalsEntry> fieldEntry : perSegment.entrySet()) {
                FieldOrdinalsEntry entry = fieldEntry.getValue();
                if (now - entry.lastUsedMillis() <= threshold) {
                    continue;
                }
                try (Releasable ignored = FILE_LOCKS.acquire(entry.fileName())) {
                    if (entry.tryMarkEvicted() == false) {
                        int held = entry.leasesHeld();
                        if (held > 0) {
                            // Legitimate for a long-running query; repeated across passes it
                            // means a lease was never released and the entry can never evict.
                            logger.warn(
                                "ord file [{}] is idle past the delete threshold but held by {} unreleased lease(s)",
                                entry.fileName(),
                                held
                            );
                        }
                        continue;
                    }
                    perSegment.remove(fieldEntry.getKey(), entry);
                    try {
                        entry.ords.close();
                    } catch (IOException e) {
                        logger.debug("failed closing evicted ords [{}]: {}", entry.fileName(), e.getMessage());
                    }
                    try {
                        if (Files.deleteIfExists(entry.file)) {
                            logger.info("evicted idle ord file [{}] (unused for {} ms)", entry.fileName(), now - entry.lastUsedMillis());
                        }
                    } catch (IOException e) {
                        logger.debug("failed deleting evicted ord file [{}]: {}", entry.fileName(), e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Deletes ord files with no cache entry — orphans of merged-away segments and files of
     * shards never queried since restart — judged by last-modified time. Walks every
     * {@code <dataRoot>/nodes/0/indices/<indexUuid>/<shard>/parquet-ords}.
     */
    private static void deleteExpiredFilesOnDisk(long now, long threshold) {
        OrdFilePaths.forEachOrdFile(dataRoots, file -> {
            if (now - OrdFilePaths.lastModifiedMillis(file) <= threshold) {
                return;
            }
            String name = file.getFileName().toString();
            // Same lock as acquire's build/load: a query loading this file and this delete cannot interleave.
            try (Releasable ignored = FILE_LOCKS.acquire(name)) {
                // Re-check under the lock: a concurrent acquire may have just loaded it.
                if (isFileCached(name)) {
                    return;
                }
                try {
                    if (Files.deleteIfExists(file)) {
                        logger.info("evicted cold ord file [{}] (not used for longer than the delete threshold)", name);
                    }
                } catch (IOException e) {
                    logger.debug("failed deleting cold ord file [{}]: {}", name, e.getMessage());
                }
            }
        });
    }

    private static boolean isFileCached(String fileName) {
        for (Map<String, FieldOrdinalsEntry> perSegment : ORDINALS_BY_SEGMENT.values()) {
            for (FieldOrdinalsEntry entry : perSegment.values()) {
                if (entry.fileName().equals(fileName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Deletes the ord files of parquet files the engine just removed; failures are logged, never thrown. */
    public static void deleteOrdFilesOfDeletedParquetFiles(Path shardDataPath, Collection<String> parquetFileNames) {
        if (parquetFileNames == null || parquetFileNames.isEmpty()) {
            return;
        }
        Path ordsDir = OrdFilePaths.resolveOrdsDir(shardDataPath);
        if (Files.isDirectory(ordsDir) == false) {
            return;
        }
        for (String parquetFileName : parquetFileNames) {
            if (parquetFileName.endsWith(".parquet") == false) {
                continue;
            }
            deleteOrdFilesOfDeletedParquetFile(ordsDir, OrdFilePaths.parquetFileStem(parquetFileName));
        }
    }

    /** Deletes every ord file (and any leftover build temp) of the removed parquet file with this stem. */
    private static void deleteOrdFilesOfDeletedParquetFile(Path ordsDir, String parquetFileStem) {
        // A parquet stem has no '-', so stem _parquet_file_generation_3 cannot match
        // _parquet_file_generation_33-city.ord or _parquet_file_generation_merged_3-city.ord.
        String prefix = parquetFileStem + "-";
        try (Stream<Path> listing = Files.list(ordsDir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                String name = file.getFileName().toString();
                if (name.startsWith(prefix) == false) {
                    continue;
                }
                boolean isOrd = name.endsWith(".ord");
                boolean isTmp = name.endsWith(".ord.tmp");
                if (isOrd == false && isTmp == false) {
                    continue;
                }
                // Same per-file lock as acquire's build/load and the eviction passes, which key on the
                // bare .ord name; a leftover .ord.tmp locks under that same name without its .tmp suffix.
                String lockKey = isTmp ? name.substring(0, name.length() - ".tmp".length()) : name;
                try (Releasable ignored = FILE_LOCKS.acquire(lockKey)) {
                    if (Files.deleteIfExists(file)) {
                        logger.debug("deleted ord file [{}] whose parquet file was removed", name);
                    }
                } catch (IOException e) {
                    logger.warn("failed deleting ord file [{}] of a removed parquet file: {}", name, e.getMessage());
                }
            }
        } catch (NoSuchFileException e) {
            // ords dir vanished (shard deleted): nothing to do
        } catch (IOException e) {
            logger.warn("failed listing ords dir [{}] for a removed parquet file: {}", ordsDir, e.getMessage());
        } catch (UncheckedIOException e) {
            logger.warn("failed listing ords dir [{}] for a removed parquet file: {}", ordsDir, e.getCause().getMessage());
        }
    }

    /** Request-scoped handle that keeps a cache entry in-use until the reader closes. */
    static final class Lease implements AutoCloseable {
        private final FieldOrdinalsEntry entry;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(FieldOrdinalsEntry entry) {
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

    private static final class FieldOrdinalsEntry {
        private final UninvertedOrdinals ords;
        private final Path file;
        private volatile long lastUsedMillis;
        private volatile long lastTouchMillis;
        private int inUse;
        private boolean evicted;

        private FieldOrdinalsEntry(UninvertedOrdinals ords, Path file) {
            this.ords = ords;
            this.file = file;
            this.lastUsedMillis = System.currentTimeMillis();
            this.lastTouchMillis = this.lastUsedMillis;
        }

        private synchronized Lease tryAcquire() {
            if (evicted) {
                return null;
            }
            inUse++;
            lastUsedMillis = System.currentTimeMillis();
            logger.debug("lease acquired [{}] inUse={} thread={}", ords.fileName(), inUse, Thread.currentThread().getName());
            return new Lease(this);
        }

        private synchronized void release() {
            if (inUse <= 0) {
                throw new IllegalStateException("uninverted ordinals lease underflow for " + ords.fileName());
            }
            inUse--;
            lastUsedMillis = System.currentTimeMillis();
            logger.debug("lease released [{}] inUse={} thread={}", ords.fileName(), inUse, Thread.currentThread().getName());
        }

        private synchronized int leasesHeld() {
            return inUse;
        }

        private synchronized boolean tryMarkEvicted() {
            if (evicted || inUse > 0) {
                return false;
            }
            evicted = true;
            return true;
        }

        /**
         * Refreshes the file's last-modified time so idleness survives restarts (in-memory
         * {@code lastUsedMillis} is lost with the process; the deletion pass's cold-file phase reads
         * the file's last-modified time instead).
         * At most one metadata write per {@link #LAST_USED_PERSIST_INTERVAL_MILLIS}.
         */
        private void persistLastUsedTimeIfDue(long now) {
            if (now - lastTouchMillis < LAST_USED_PERSIST_INTERVAL_MILLIS) {
                return;
            }
            lastTouchMillis = now;
            try {
                Files.setLastModifiedTime(file, FileTime.fromMillis(now));
            } catch (IOException e) {
                logger.debug("failed touching ord file [{}]: {}", file.getFileName(), e.getMessage());
            }
        }

        private String fileName() {
            return ords.fileName();
        }

        private long lastUsedMillis() {
            return lastUsedMillis;
        }
    }
}
