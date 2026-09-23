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
import org.apache.lucene.util.StringHelper;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Node-level cache of {@link UninvertedOrdinals}, keyed by (segment core key, field). Builds of
 * different files run in parallel (bounded by build permits); mutations of one file serialize on
 * its per-file lock; entries close with their segment core. The on-disk file is keyed by the
 * segment's stable id and survives restarts; unused files are deleted by the periodic deletion pass.
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
    private static final Map<String, Object> FILE_LOCKS = new ConcurrentHashMap<>();

    /**
     * Bounds concurrent FROM-SCRATCH builds node-wide: each holds a transient packed buffer of
     * {@code maxDoc × bits} heap (~287 MB at 100M docs), so parallelism must be capped. Loads of
     * existing files and cache hits never take a permit.
     */
    private static final ResizableSemaphore BUILD_PERMITS = new ResizableSemaphore(2);

    /** Idle time after which an unused ord file is deleted; {@code 0} = keep forever. */
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

    /** Idle time before an unused ord file is deleted; {@code TimeValue.ZERO} = never delete. */
    public static void setDeleteUnusedAfter(TimeValue deleteUnusedAfter) {
        deleteUnusedAfterMillis = deleteUnusedAfter.millis();
    }

    /** Cap on concurrent from-scratch builds (dynamic cluster setting). */
    public static void setMaxConcurrentBuilds(int max) {
        BUILD_PERMITS.resize(max);
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
                // Core close happens after Lucene retires the reader, so no live leases remain.
                Map<String, FieldOrdinalsEntry> removed = ORDINALS_BY_SEGMENT.remove(closedKey);
                if (removed != null) {
                    for (FieldOrdinalsEntry entry : removed.values()) {
                        FILE_LOCKS.remove(entry.fileName());
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

        Lease lease = leaseExistingEntry(perSegment, field);
        if (lease != null) {
            return lease;
        }
        String fileKey = StringHelper.idToString(segmentInfo.getId()) + "-" + field;
        synchronized (fileLock(OrdFilePaths.ordFileName(fileKey))) {
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
        try {
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
            // Cheap path first: an existing usable file is just memory-mapped — no permit. load()
            // deletes an invalid leftover (crashed process, stale layout) and returns null, which
            // sends us to the build path like any other cold field.
            UninvertedOrdinals built = UninvertedOrdinals.load(ordsDir, fileKey, terms, maxDoc, expectedNonNullDocs);
            if (built == null) {
                // From-scratch build: bounded node-wide, each holds a maxDoc×bits buffer.
                BUILD_PERMITS.acquire();
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
                    BUILD_PERMITS.release();
                }
            }
            FieldOrdinalsEntry entry = new FieldOrdinalsEntry(built, ordsDir.resolve(OrdFilePaths.ordFileName(fileKey)));
            Lease lease = entry.tryAcquire();
            if (lease == null) {
                throw new IllegalStateException("new uninverted ordinals entry unexpectedly unavailable");
            }
            perSegment.put(field, entry);
            return lease;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted waiting for a build permit for field [" + field + "]", e);
        }
    }

    /** Runs one deletion pass; scheduled by the plugin every few minutes. */
    public static void deleteUnusedOrdFiles() {
        deleteUnusedOrdFiles(System.currentTimeMillis());
    }

    /**
     * Deletes ord files unused for longer than the configured threshold ({@code 0} = keep forever). In-use files
     * are protected by the lease refcount ({@link FieldOrdinalsEntry#tryMarkEvicted}); deletions run
     * under the file's mutation lock so they can never race a concurrent build or load.
     */
    // package-private overload for deterministic tests
    static void deleteUnusedOrdFiles(long now) {
        long threshold = deleteUnusedAfterMillis;
        if (threshold <= 0) {
            return;
        }
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
                synchronized (fileLock(entry.fileName())) {
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
            synchronized (fileLock(name)) {
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

    /** The per-file mutation lock: build, load, and delete of one ord file serialize on this. */
    private static Object fileLock(String fileName) {
        return FILE_LOCKS.computeIfAbsent(fileName, k -> new Object());
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

    /** Semaphore whose permit count can follow a dynamic cluster setting. */
    private static final class ResizableSemaphore extends Semaphore {
        /** Configured capacity — tracked here because {@link #availablePermits()} only reports
         * currently-free permits, which is the wrong baseline for resizing mid-build. */
        private int capacity;

        ResizableSemaphore(int permits) {
            super(permits, true); // fair: cold queries acquire build capacity in arrival order
            this.capacity = permits;
        }

        synchronized void resize(int newSize) {
            if (newSize > capacity) {
                release(newSize - capacity);
            } else if (newSize < capacity) {
                reducePermits(capacity - newSize);
            }
            capacity = newSize;
        }
    }
}
