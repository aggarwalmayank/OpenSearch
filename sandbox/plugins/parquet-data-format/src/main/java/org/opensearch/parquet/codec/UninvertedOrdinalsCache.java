/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-level cache of {@link UninvertedOrdinals}, keyed by (segment core key, field).
 *
 * <p>Concurrent builds are allowed: striped per-key locks keep the SAME (segment, field) from
 * building twice at once, while DIFFERENT keys build in parallel, and disk-budget accounting
 * reserves space under a dedicated lock so parallel builds cannot both claim the same free bytes.
 * How many builds run at once — and thus how many transient {@code maxDoc}-sized build buffers
 * coexist — is bounded upstream by the fixed-size pool in {@code GlobalOrdinalsBuilder} (its
 * {@code parquet.fielddata.global_ordinals.build_concurrency} setting; default 1 = serial, the
 * original behavior). Entries are evicted (and their mapped files closed) by the segment core's
 * closed-listener; the on-disk artifact is keyed by the segment's stable id and survives restarts,
 * so a re-opened segment maps the existing file instead of rebuilding.
 */
public final class UninvertedOrdinalsCache {

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        UninvertedOrdinalsCache.class
    );
    /** Marks a (segment, field) whose ordinals failed coverage verification — do not retry. */
    private static final Map<Object, java.util.Set<String>> INELIGIBLE = new ConcurrentHashMap<>();

    private static final Map<Object, Map<String, UninvertedOrdinals>> CACHE = new ConcurrentHashMap<>();

    /**
     * Striped locks keyed by fileKey hash: the SAME (segment, field) never builds twice at once
     * (which would race on the shared {@code .ord.tmp} write), while DIFFERENT keys proceed in
     * parallel. Fixed size — no per-key map to leak as segments come and go.
     *
     * <p>Concurrent builds are NOT capped here: how many run at once is bounded upstream by the
     * fixed-size pool in {@code GlobalOrdinalsBuilder} (per its build_concurrency setting), which
     * also bounds how many transient {@code maxDoc}-sized build buffers coexist. The single-segment
     * path that bypasses that pool is bounded by the search thread pool.
     */
    private static final int LOCK_STRIPES = 64;
    private static final Object[] KEY_LOCKS = new Object[LOCK_STRIPES];
    static {
        for (int i = 0; i < LOCK_STRIPES; i++) {
            KEY_LOCKS[i] = new Object();
        }
    }

    private static Object keyLock(String fileKey) {
        return KEY_LOCKS[(fileKey.hashCode() & 0x7fffffff) % LOCK_STRIPES];
    }

    /** Serializes disk-budget accounting so concurrent builds cannot both "see room" for the same bytes. */
    private static final Object DISK_BUDGET_LOCK = new Object();
    /** Bytes reserved by in-flight builds whose {@code .ord} is not yet on disk (counted against the budget). */
    private static final AtomicLong RESERVED_BYTES = new AtomicLong();

    /** Default under java.io.tmpdir (unit tests); the plugin points this at the node data path. */
    private static volatile Path ORDS_DIR = Path.of(System.getProperty("java.io.tmpdir"), "opensearch-parquet-ords");

    private static volatile boolean shuttingDown = false;

    /**
     * Called once at plugin init: ord files live with the node's data, not in tmp. Also performs
     * crash hygiene: interrupted builds' {@code .tmp} files are deleted, and the directory is
     * trimmed to the disk budget oldest-first (nothing is pinned yet at startup).
     */
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
        boolean preExisting = java.nio.file.Files.exists(ORDS_DIR.resolve(fileName));
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
            java.nio.file.Files.deleteIfExists(ORDS_DIR.resolve(fileName));
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

    private static void cleanupAtStartup(Path dir) {
        long budget = ParquetDocValuesProducer.uninvertMaxDiskBytes();
        long used = 0;
        List<Path> files = new java.util.ArrayList<>();
        try (java.util.stream.Stream<Path> listing = java.nio.file.Files.list(dir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                if (file.getFileName().toString().endsWith(".tmp")) {
                    java.nio.file.Files.deleteIfExists(file); // interrupted build leftovers
                } else {
                    used += java.nio.file.Files.size(file);
                    files.add(file);
                }
            }
        } catch (java.nio.file.NoSuchFileException e) {
            return;
        } catch (IOException e) {
            LOGGER.warn("ords directory startup cleanup failed for [{}]: {}", dir, e.getMessage());
            return;
        }
        if (used <= budget) {
            return;
        }
        files.sort(java.util.Comparator.comparingLong(f -> {
            try {
                return java.nio.file.Files.getLastModifiedTime(f).toMillis();
            } catch (IOException e) {
                return Long.MAX_VALUE;
            }
        }));
        for (Path victim : files) {
            if (used <= budget) {
                break;
            }
            try {
                long size = java.nio.file.Files.size(victim);
                java.nio.file.Files.deleteIfExists(victim);
                used -= size;
                LOGGER.info("reclaimed ord file [{}] at startup (over budget)", victim.getFileName());
            } catch (IOException e) {
                // skip
            }
        }
    }

    private UninvertedOrdinalsCache() {}

    /** Transient refusal: budget can be raised or freed, so it is never latched as INELIGIBLE. */
    private static final class BudgetExceededException extends IllegalStateException {
        BudgetExceededException(String message) {
            super(message);
        }
    }

    /**
     * Keeps the ords directory within {@code parquet.docvalues.uninvert.max_disk_bytes}. Files
     * belonging to live cache entries are pinned; everything else (closed segments, merged-away
     * segments, other fields' leftovers) is reclaimable oldest-mtime-first. If the new file
     * still does not fit after reclaim, the build is refused — bounded disk, loud fallback.
     */
    private static long enforceDiskBudget(String fileKey, Terms terms, int maxDoc) throws IOException {
        String fileName = "parquet-ords-" + fileKey + ".ord";
        if (java.nio.file.Files.exists(ORDS_DIR.resolve(fileName))) {
            return 0; // reusing an existing file adds no disk, nothing to reserve
        }
        long budget = ParquetDocValuesProducer.uninvertMaxDiskBytes();
        long termCount = Math.max(terms.size(), 0);
        long bits = org.apache.lucene.util.packed.DirectWriter.bitsRequired(termCount + 1);
        long estimate = (maxDoc * bits + 7) / 8 + 1024;
        // Accounting (scan + reclaim + reserve) is serialized so concurrent builds cannot both
        // "see room" for the same free bytes. RESERVED_BYTES covers in-flight builds whose .ord
        // is not yet on disk; the caller releases its reservation once the build finishes.
        synchronized (DISK_BUDGET_LOCK) {
            java.util.Set<String> pinned = new java.util.HashSet<>();
            for (Map<String, UninvertedOrdinals> perSegment : CACHE.values()) {
                for (UninvertedOrdinals live : perSegment.values()) {
                    pinned.add(live.fileName());
                }
            }
            long used = 0;
            List<Path> reclaimable = new java.util.ArrayList<>();
            try (java.util.stream.Stream<Path> listing = java.nio.file.Files.list(ORDS_DIR)) {
                for (Path file : (Iterable<Path>) listing::iterator) {
                    used += java.nio.file.Files.size(file);
                    if (pinned.contains(file.getFileName().toString()) == false) {
                        reclaimable.add(file);
                    }
                }
            } catch (java.nio.file.NoSuchFileException e) {
                // directory not created yet: nothing on disk. Still reserve against in-flight builds.
                RESERVED_BYTES.addAndGet(estimate);
                return estimate;
            }
            long reserved = RESERVED_BYTES.get();
            if (used + reserved + estimate <= budget) {
                RESERVED_BYTES.addAndGet(estimate);
                return estimate;
            }
            reclaimable.sort(java.util.Comparator.comparingLong(f -> {
                try {
                    return java.nio.file.Files.getLastModifiedTime(f).toMillis();
                } catch (IOException e) {
                    return Long.MAX_VALUE;
                }
            }));
            for (Path victim : reclaimable) {
                if (used + reserved + estimate <= budget) {
                    break;
                }
                try {
                    long size = java.nio.file.Files.size(victim);
                    java.nio.file.Files.deleteIfExists(victim);
                    used -= size;
                } catch (IOException e) {
                    // still referenced by an mmap on some platforms or raced; skip
                }
            }
            if (used + reserved + estimate > budget) {
                throw new BudgetExceededException(
                    "uninverted ordinals disk budget exceeded: "
                        + used
                        + "B used + "
                        + reserved
                        + "B reserved (in-flight) + "
                        + estimate
                        + "B needed > "
                        + budget
                        + "B (parquet.docvalues.uninvert.max_disk_bytes)"
                );
            }
            RESERVED_BYTES.addAndGet(estimate);
            return estimate;
        }
    }

    /**
     * The uninverted ordinals for {@code field}, building (or re-mapping) on first use.
     * Returns {@code null} when the segment lacks a core cache identity or a terms index.
     */
    static UninvertedOrdinals get(LeafReader leaf, SegmentInfo segmentInfo, String field, long expectedNonNullDocs) throws IOException {
        IndexReader.CacheHelper helper = leaf.getCoreCacheHelper();
        Terms terms = leaf.terms(field);
        if (helper == null || terms == null) {
            return null;
        }
        Object key = helper.getKey();
        java.util.Set<String> ineligible = INELIGIBLE.get(key);
        if (ineligible != null && ineligible.contains(field)) {
            return null;
        }
        Map<String, UninvertedOrdinals> perSegment = CACHE.computeIfAbsent(key, k -> {
            helper.addClosedListener(closedKey -> {
                INELIGIBLE.remove(closedKey);
                Map<String, UninvertedOrdinals> removed = CACHE.remove(closedKey);
                if (removed != null) {
                    for (UninvertedOrdinals ords : removed.values()) {
                        try {
                            ords.close();
                        } catch (IOException e) {
                            // Segment is going away; nothing actionable.
                        }
                    }
                }
            });
            return new ConcurrentHashMap<>();
        });
        UninvertedOrdinals cached = perSegment.get(field);
        if (cached != null) {
            return cached;
        }
        String fileKey = StringHelper.idToString(segmentInfo.getId()) + "-" + field;
        // The per-key stripe lock lets DIFFERENT (segment, field) builds run at once (that is the
        // parallelism the GlobalOrdinalsBuilder pool drives) while ensuring the SAME key is never
        // built twice concurrently (they would race on the .ord.tmp write). Concurrency and heap
        // are bounded upstream by that pool's size, so there is no separate cap here.
        synchronized (keyLock(fileKey)) {
            cached = perSegment.get(field);
            if (cached != null) {
                return cached;
            }
            long reserved = 0;
            try {
                reserved = enforceDiskBudget(fileKey, terms, leaf.maxDoc());
                UninvertedOrdinals built = buildWithRetry(fileKey, terms, leaf.maxDoc(), expectedNonNullDocs);
                perSegment.put(field, built);
                return built;
            } catch (BudgetExceededException e) {
                // Disk budget refusals are transient (budget can be raised, files can be
                // reclaimed): log and fall back WITHOUT latching, so the next query retries.
                LOGGER.warn("refusing uninverted ordinals for field [{}]: {}", field, e.getMessage());
                return null;
            } catch (IllegalStateException e) {
                // Coverage verification failed: postings do not represent every stored value
                // (ignore_above truncation and the like). Serving them would silently
                // undercount. Remember the refusal and let global-ordinal consumers hit the
                // streaming iterator's loud fail-fast toward execution_hint:map.
                LOGGER.warn("refusing uninverted ordinals for field [{}]: {}", field, e.getMessage());
                INELIGIBLE.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(field);
                return null;
            } finally {
                // Release the disk reservation: on success the .ord is now on disk and counted
                // by future scans; on failure nothing durable was written. Either way the
                // in-flight reservation must not linger.
                if (reserved > 0) {
                    RESERVED_BYTES.addAndGet(-reserved);
                }
            }
        }
    }
}
