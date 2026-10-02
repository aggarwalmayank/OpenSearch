/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;

/**
 * Deletion of unused uninverted-ordinal files by
 * {@link UninvertedOrdinalsCache#deleteUnusedOrdFiles(long)}: idle files go, in-use files are held
 * by the lease refcount, and fresh files survive.
 */
public class UninvertedOrdinalsDeleteUnusedTests extends OpenSearchTestCase {

    private static final long DELETE_AFTER_MILLIS = TimeValue.timeValueMinutes(10).millis();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        UninvertedOrdinalsCache.start();
        UninvertedOrdinalsCache.setBuildBreaker(new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
        UninvertedOrdinalsCache.setDeleteUnusedAfter(TimeValue.timeValueMillis(DELETE_AFTER_MILLIS));
    }

    @Override
    public void tearDown() throws Exception {
        UninvertedOrdinalsCache.setDeleteUnusedAfter(TimeValue.timeValueDays(7));
        UninvertedOrdinalsCache.setDataRoots(new Path[0]);
        super.tearDown();
    }

    public void testPassDeletesIdleFileAndKeepsFreshOne() throws Exception {
        Path shardDir = createTempDir();
        withOrdinalsBuilt(shardDir, (leaf, ordFile) -> {
            // Fresh: a pass "now" (well within the threshold) must keep the file.
            UninvertedOrdinalsCache.deleteUnusedOrdFiles(System.currentTimeMillis());
            assertTrue("fresh file must survive the pass", Files.exists(ordFile));

            // Idle past the threshold: a pass from the future must delete it.
            UninvertedOrdinalsCache.deleteUnusedOrdFiles(System.currentTimeMillis() + DELETE_AFTER_MILLIS + 1000);
            assertFalse("idle file must be evicted", Files.exists(ordFile));
        });
    }

    public void testPassNeverDeletesFileHeldByALease() throws Exception {
        Path shardDir = createTempDir();
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory directory = FSDirectory.open(storeDir)) {
            indexCityDocs(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                leaf.getSegmentInfo().info.putAttribute(
                    ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE,
                    shardDir.resolve("parquet").resolve("_parquet_file_generation_1.parquet").toString()
                );
                UninvertedOrdinalsCache.Lease lease = UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 3);
                assertNotNull(lease);
                Path ordFile = onlyOrdFile(shardDir);

                // In use: even a far-future pass must not touch it.
                UninvertedOrdinalsCache.deleteUnusedOrdFiles(System.currentTimeMillis() + DELETE_AFTER_MILLIS * 10);
                assertTrue("in-use file must never be evicted", Files.exists(ordFile));

                // Released: the next pass past the threshold may evict it.
                lease.close();
                UninvertedOrdinalsCache.deleteUnusedOrdFiles(System.currentTimeMillis() + DELETE_AFTER_MILLIS * 10);
                assertFalse("released idle file must be evicted", Files.exists(ordFile));
            }
        }
    }

    /** Cold files (no cache entry: orphans of merged segments, never-queried shards) go by last-modified time. */
    public void testPassDeletesColdFilesByLastModifiedTime() throws Exception {
        Path root = createTempDir();
        Path ordsDir = root.resolve("nodes").resolve("0").resolve("indices").resolve("idxUuid").resolve("0").resolve("parquet-ords");
        Files.createDirectories(ordsDir);
        Path oldFile = ordsDir.resolve("_parquet_file_generation_3-city.ord");
        Path freshFile = ordsDir.resolve("_parquet_file_generation_4-city.ord");
        Files.write(oldFile, new byte[16]);
        Files.write(freshFile, new byte[16]);
        long now = System.currentTimeMillis();
        Files.setLastModifiedTime(oldFile, FileTime.fromMillis(now - DELETE_AFTER_MILLIS - 60_000));
        Files.setLastModifiedTime(freshFile, FileTime.fromMillis(now));
        UninvertedOrdinalsCache.setDataRoots(new Path[] { root });

        UninvertedOrdinalsCache.deleteUnusedOrdFiles(now);

        assertFalse("cold file older than the threshold must be deleted", Files.exists(oldFile));
        assertTrue("cold file within the threshold must survive", Files.exists(freshFile));
    }

    // ---- fixtures ----

    interface OrdinalsConsumer {
        void accept(SegmentReader leaf, Path ordFile) throws Exception;
    }

    /** Builds ordinals for {@code city} on a fresh shard, releases the lease, and runs the body with the reader still open. */
    private static void withOrdinalsBuilt(Path shardDir, OrdinalsConsumer body) throws Exception {
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory directory = FSDirectory.open(storeDir)) {
            indexCityDocs(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                leaf.getSegmentInfo().info.putAttribute(
                    ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE,
                    shardDir.resolve("parquet").resolve("_parquet_file_generation_1.parquet").toString()
                );
                UninvertedOrdinalsCache.Lease lease = UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 3);
                assertNotNull("fixture requires a successful build", lease);
                lease.close();
                body.accept(leaf, onlyOrdFile(shardDir));
            }
        }
    }

    private static void indexCityDocs(Directory directory) throws Exception {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (String value : new String[] { "delhi", "mumbai", "pune" }) {
                Document document = new Document();
                document.add(new StringField("city", value, Field.Store.NO));
                writer.addDocument(document);
            }
            writer.commit();
        }
    }

    private static Path onlyOrdFile(Path shardDir) throws Exception {
        try (var listing = Files.list(shardDir.resolve("parquet-ords"))) {
            return listing.filter(f -> f.toString().endsWith(".ord")).findFirst().orElseThrow();
        }
    }

    /** Delete-on-merge removes only the named parquet file's ord files, never a longer-stem sibling. */
    public void testDeleteOrdFilesOfDeletedParquetFilesMatchesStemExactly() throws Exception {
        Path shardDir = createTempDir();
        Path ordsDir = shardDir.resolve("parquet-ords");
        Files.createDirectories(ordsDir);

        Path target = Files.createFile(ordsDir.resolve("_parquet_file_generation_3-city.ord"));
        Path targetOtherField = Files.createFile(ordsDir.resolve("_parquet_file_generation_3-country.ord"));
        Path leftoverTmp = Files.createFile(ordsDir.resolve("_parquet_file_generation_3-city.ord.tmp"));
        // Siblings that share the "_parquet_file_generation_3" leading text but are different files.
        Path longerGeneration = Files.createFile(ordsDir.resolve("_parquet_file_generation_33-city.ord"));
        Path mergedGeneration = Files.createFile(ordsDir.resolve("_parquet_file_generation_merged_3-city.ord"));

        UninvertedOrdinalsCache.deleteOrdFilesOfDeletedParquetFiles(shardDir, java.util.List.of("_parquet_file_generation_3.parquet"));

        assertFalse("the stem's city ord file must be deleted", Files.exists(target));
        assertFalse("the stem's country ord file must be deleted", Files.exists(targetOtherField));
        assertFalse("the stem's leftover build temp must be deleted", Files.exists(leftoverTmp));
        assertTrue("_parquet_file_generation_33 must survive stem _parquet_file_generation_3", Files.exists(longerGeneration));
        assertTrue("_parquet_file_generation_merged_3 must survive stem _parquet_file_generation_3", Files.exists(mergedGeneration));
    }

    /** A missing ords directory is a no-op, not an error. */
    public void testDeleteOrdFilesOfDeletedParquetFilesIsNoOpWhenOrdsDirMissing() {
        UninvertedOrdinalsCache.deleteOrdFilesOfDeletedParquetFiles(
            createTempDir(),
            java.util.List.of("_parquet_file_generation_1.parquet")
        );
        // reaching here without throwing is the assertion
    }

    /** Null, empty and non-.parquet inputs delete nothing and never throw. */
    public void testDeleteOrdFilesIgnoresNullEmptyAndNonParquetNames() throws Exception {
        Path shardDir = createTempDir();
        Path ordsDir = Files.createDirectories(shardDir.resolve("parquet-ords"));
        Path keep = Files.createFile(ordsDir.resolve("_parquet_file_generation_7-city.ord"));

        UninvertedOrdinalsCache.deleteOrdFilesOfDeletedParquetFiles(shardDir, List.of("_parquet_file_generation_7"));
        UninvertedOrdinalsCache.deleteOrdFilesOfDeletedParquetFiles(shardDir, null);
        UninvertedOrdinalsCache.deleteOrdFilesOfDeletedParquetFiles(shardDir, List.of());
        assertTrue("these inputs must not delete any ord file", Files.exists(keep));
    }
}
