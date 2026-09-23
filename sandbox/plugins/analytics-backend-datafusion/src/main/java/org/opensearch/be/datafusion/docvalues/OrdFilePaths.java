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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Where uninverted-ordinal ({@code .ord}) files live on disk and how to find them: per-shard
 * directory resolution, file naming, directory preparation, and the data-root walk. Holds no
 * cache state and no eviction policy.
 */
final class OrdFilePaths {

    private static final Logger logger = LogManager.getLogger(OrdFilePaths.class);

    /** Name of the per-shard ord-file directory, a sibling of the shard's store directory. */
    static final String ORDS_DIR_NAME = "parquet-ords";

    /**
     * The ord file's name for a (segment, field) key: {@code parquet-ords-<segmentId>-<field>.ord}.
     * The segment id is stable across restarts, so the name is too — that is what lets a file
     * built by a previous process be found and reused.
     */
    static String ordFileName(String fileKey) {
        return "parquet-ords-" + fileKey + ".ord";
    }

    /**
     * Memo of ords directories whose one-time preparation (create + stale {@code .tmp} cleanup)
     * has already run in this process, so the thousandth {@code prepareDir} call is a set lookup,
     * not filesystem work. Never cleaned: it holds one path per shard on this node, and
     * re-preparing after a segment closes would be wasted work anyway.
     */
    private static final Set<Path> PREPARED_DIRS = ConcurrentHashMap.newKeySet();

    private OrdFilePaths() {}

    /**
     * The segment's ord-file directory: {@code parquet-ords} beside the shard's store (like the
     * translog — never inside the store, whose unknown files recovery deletes). {@code null} when
     * the directory is not filesystem-backed: no shard folder to place files in.
     */
    static Path resolveOrdsDir(Directory directory) {
        Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FSDirectory) {
            Path storeDir = ((FSDirectory) unwrapped).getDirectory();
            Path shardDir = storeDir.getParent();
            if (shardDir != null) {
                return shardDir.resolve(ORDS_DIR_NAME);
            }
        }
        return null;
    }

    /**
     * Creates the directory and sweeps stale {@code .tmp} files, once per process lifetime.
     * Synchronized so a second query racing into the same cold shard waits for the preparation
     * to finish instead of building into a dir whose {@code .tmp} sweep is still running (the
     * sweep could delete its half-written file). Once-per-dir work: contention is negligible.
     */
    static synchronized void prepareDir(Path ordsDir) throws IOException {
        // add() returns false when the dir is already in the set — already prepared, nothing to do.
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
     * Walks every shard's ords directory under the given data roots — each node dir's
     * {@code indices/<index>/<shard>/parquet-ords} — and hands each {@code .ord} file to
     * {@code action}. Vanished directories (shard deleted mid-walk) are skipped silently.
     */
    static void forEachOrdFile(Path[] dataRoots, Consumer<Path> action) {
        for (Path root : dataRoots) {
            Path nodes = root.resolve("nodes");
            if (Files.isDirectory(nodes) == false) {
                continue;
            }
            try (Stream<Path> nodeDirs = Files.list(nodes)) {
                for (Path nodeDir : (Iterable<Path>) nodeDirs::iterator) {
                    forEachOrdFileUnder(nodeDir.resolve("indices"), action);
                }
            } catch (IOException e) {
                logger.debug("ord-file walk failed listing node dirs under [{}]: {}", nodes, e.getMessage());
            }
        }
    }

    /** Hands every {@code .ord} file under {@code indicesRoot} (each index, each shard) to {@code fileAction}. */
    private static void forEachOrdFileUnder(Path indicesRoot, Consumer<Path> fileAction) {
        if (Files.isDirectory(indicesRoot) == false) {
            return;
        }
        try (Stream<Path> indexDirs = Files.list(indicesRoot)) {
            for (Path indexDir : (Iterable<Path>) indexDirs::iterator) {
                try (Stream<Path> shardDirs = Files.list(indexDir)) {
                    for (Path shardDir : (Iterable<Path>) shardDirs::iterator) {
                        Path ordsDir = shardDir.resolve(ORDS_DIR_NAME);
                        if (Files.isDirectory(ordsDir) == false) {
                            continue;
                        }
                        forEachOrdFileIn(ordsDir, fileAction);
                    }
                } catch (IOException e) {
                    logger.debug("ord-file walk failed under [{}]: {}", indexDir, e.getMessage());
                }
            }
        } catch (IOException e) {
            logger.debug("ord-file walk failed under [{}]: {}", indicesRoot, e.getMessage());
        }
    }

    /** Hands every {@code .ord} file inside one ords directory to {@code fileAction}. */
    private static void forEachOrdFileIn(Path ordsDir, Consumer<Path> fileAction) {
        try (Stream<Path> listing = Files.list(ordsDir)) {
            for (Path file : (Iterable<Path>) listing::iterator) {
                if (file.getFileName().toString().endsWith(".ord")) {
                    fileAction.accept(file);
                }
            }
        } catch (NoSuchFileException e) {
            // dir vanished (shard deleted) — fine
        } catch (IOException e) {
            logger.debug("ord-file walk failed in [{}]: {}", ordsDir, e.getMessage());
        }
    }

    /** The file's last-modified time in epoch millis; {@code Long.MAX_VALUE} (never idle) when unreadable. */
    static long lastModifiedMillis(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }
}
