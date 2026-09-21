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
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

/**
 * Deletion of unused uninverted-ordinal files: idle files are deleted by
 * {@link UninvertedOrdinalsCache#deleteUnusedOrdFiles(long)}, in-use files are protected by the lease
 * refcount, fresh files survive, a zero threshold keeps files forever, and cold files with no
 * cache entry (orphans, never-queried shards) are removed by their last-modified time.
 */
public class UninvertedOrdinalsDeleteUnusedTests extends OpenSearchTestCase {

    private static final long DELETE_AFTER_MILLIS = TimeValue.timeValueMinutes(10).millis();

    @Override
    public void setUp() throws Exception {
        super.setUp();
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

    public void testZeroThresholdKeepsFilesForever() throws Exception {
        UninvertedOrdinalsCache.setDeleteUnusedAfter(TimeValue.ZERO);
        Path shardDir = createTempDir();
        withOrdinalsBuilt(shardDir, (leaf, ordFile) -> {
            UninvertedOrdinalsCache.deleteUnusedOrdFiles(System.currentTimeMillis() + DELETE_AFTER_MILLIS * 1000);
            assertTrue("zero threshold must keep files forever", Files.exists(ordFile));
        });
    }

    /** Cold files (no cache entry — orphans of merged segments, never-queried shards) go by their last-modified time. */
    public void testPassDeletesColdFilesByLastModifiedTime() throws Exception {
        Path root = createTempDir();
        Path ordsDir = root.resolve("nodes").resolve("0").resolve("indices").resolve("idxUuid").resolve("0").resolve("parquet-ords");
        Files.createDirectories(ordsDir);
        Path oldFile = ordsDir.resolve("parquet-ords-deadbeef-city.ord");
        Path freshFile = ordsDir.resolve("parquet-ords-cafebabe-city.ord");
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
}
