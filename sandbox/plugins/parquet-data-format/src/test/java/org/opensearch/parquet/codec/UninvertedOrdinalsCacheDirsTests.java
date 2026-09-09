/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tests for the per-shard ord-file location and the percent-of-store disk budget:
 * {@link UninvertedOrdinalsCache#resolveOrdsDir}, {@link UninvertedOrdinalsCache#shardStoreBytes},
 * the legacy-directory wipe in {@link UninvertedOrdinalsCache#setOrdsDir}, and the budget
 * enforcement end to end through {@link UninvertedOrdinalsCache#acquire}.
 */
public class UninvertedOrdinalsCacheDirsTests extends OpenSearchTestCase {

    public void testResolveOrdsDirIsShardSiblingOfFsStore() throws Exception {
        Path shardDir = createTempDir();
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory directory = FSDirectory.open(storeDir)) {
            assertEquals(shardDir.resolve("parquet-ords"), UninvertedOrdinalsCache.resolveOrdsDir(directory));
        }
    }

    public void testResolveOrdsDirUnwrapsFilterDirectories() throws Exception {
        Path shardDir = createTempDir();
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory fs = FSDirectory.open(storeDir); Directory wrapped = new FilterDirectory(new FilterDirectory(fs) {
        }) {
        }) {
            assertEquals(shardDir.resolve("parquet-ords"), UninvertedOrdinalsCache.resolveOrdsDir(wrapped));
        }
    }

    public void testResolveOrdsDirRefusesNonFsDirectories() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            assertNull("no shard folder exists for a non-filesystem directory", UninvertedOrdinalsCache.resolveOrdsDir(directory));
        }
    }

    /** A non-filesystem segment refuses ordinals end to end: acquire returns null, nothing is written. */
    public void testAcquireRefusesNonFsDirectory() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new StringField("city", "delhi", org.apache.lucene.document.Field.Store.NO));
                writer.addDocument(document);
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                assertNull(UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 1));
                // Latched: the second attempt refuses without re-resolving.
                assertNull(UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 1));
            }
        }
    }

    public void testShardStoreBytesExcludesOrdsDirAndTranslog() throws Exception {
        Path shardDir = createTempDir();
        Path ordsDir = shardDir.resolve("parquet-ords");
        Files.createDirectories(shardDir.resolve("index"));
        Files.createDirectories(shardDir.resolve("translog"));
        Files.createDirectories(shardDir.resolve("_state"));
        Files.createDirectories(ordsDir);
        Files.write(shardDir.resolve("index").resolve("segment.parquet"), new byte[100]);
        Files.write(shardDir.resolve("index").resolve("_0.tim"), new byte[40]);
        Files.write(shardDir.resolve("_state").resolve("state.st"), new byte[10]);
        Files.write(shardDir.resolve("translog").resolve("translog.tlog"), new byte[500]);
        Files.write(ordsDir.resolve("parquet-ords-a-city.ord"), new byte[300]);

        assertEquals(150, UninvertedOrdinalsCache.shardStoreBytes(ordsDir));
    }

    public void testShardStoreBytesUnenforceableWithoutParent() {
        assertEquals(-1, UninvertedOrdinalsCache.shardStoreBytes(Path.of("/")));
    }

    public void testSetOrdsDirWipesLegacyOrdAndTmpFilesOnly() throws Exception {
        Path legacy = createTempDir();
        Files.write(legacy.resolve("parquet-ords-a-city.ord"), new byte[10]);
        Files.write(legacy.resolve("parquet-ords-b-city.ord.tmp"), new byte[10]);
        Files.write(legacy.resolve("unrelated.txt"), new byte[10]);

        UninvertedOrdinalsCache.setOrdsDir(legacy);

        assertFalse(Files.exists(legacy.resolve("parquet-ords-a-city.ord")));
        assertFalse(Files.exists(legacy.resolve("parquet-ords-b-city.ord.tmp")));
        assertTrue(Files.exists(legacy.resolve("unrelated.txt")));
    }

    /**
     * End to end through {@link UninvertedOrdinalsCache#acquire}: with a permissive percent the
     * .ord file is built under {@code <shard>/parquet-ords/}; with a zero percent the build is
     * refused (transient — acquire returns null, nothing latched) and no file is written.
     */
    public void testAcquireBuildsUnderShardPathAndHonoursPercentBudget() throws Exception {
        double before = ParquetDocValuesProducer.uninvertMaxDiskPercent();
        try {
            Path allowedShard = createTempDir();
            ParquetDocValuesProducer.setUninvertMaxDiskPercent(100.0);
            assertNotNull("permissive budget must build", acquireOnFreshShard(allowedShard));
            try (var listing = Files.list(allowedShard.resolve("parquet-ords"))) {
                assertTrue("an .ord file must exist under <shard>/parquet-ords", listing.anyMatch(f -> f.toString().endsWith(".ord")));
            }

            Path refusedShard = createTempDir();
            ParquetDocValuesProducer.setUninvertMaxDiskPercent(0.0);
            assertNull("zero budget must refuse", acquireOnFreshShard(refusedShard));
            Path refusedOrds = refusedShard.resolve("parquet-ords");
            if (Files.isDirectory(refusedOrds)) {
                try (var listing = Files.list(refusedOrds)) {
                    assertFalse("no .ord file may be written when refused", listing.anyMatch(f -> f.toString().endsWith(".ord")));
                }
            }

            // Budget refusal is transient: raising the percent lets the SAME shard build.
            ParquetDocValuesProducer.setUninvertMaxDiskPercent(100.0);
            assertNotNull("raised budget must build after a refusal", acquireOnFreshShard(refusedShard));
        } finally {
            ParquetDocValuesProducer.setUninvertMaxDiskPercent(before);
        }
    }

    /**
     * Regression: on a tiny shard, a percentage of the store is smaller than one .ord file's
     * fixed overhead (~1 KiB), which used to refuse every build on small test indices. Any
     * non-zero percent must admit the build via the budget floor.
     */
    public void testSmallShardBuildsUnderDefaultPercentBudget() throws Exception {
        double before = ParquetDocValuesProducer.uninvertMaxDiskPercent();
        try {
            ParquetDocValuesProducer.setUninvertMaxDiskPercent(10.0);
            Path smallShard = createTempDir();
            assertNotNull("a tiny shard must not be refused by the percent budget", acquireOnFreshShard(smallShard));
        } finally {
            ParquetDocValuesProducer.setUninvertMaxDiskPercent(before);
        }
    }

    /** Indexes three docs with postings for {@code city} into {@code <shard>/index} and acquires ordinals. */
    private static UninvertedOrdinalsCache.Lease acquireOnFreshShard(Path shardDir) throws Exception {
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory directory = FSDirectory.open(storeDir)) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                for (String value : new String[] { "delhi", "mumbai", "pune" }) {
                    Document document = new Document();
                    document.add(new StringField("city", value, org.apache.lucene.document.Field.Store.NO));
                    writer.addDocument(document);
                }
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                UninvertedOrdinalsCache.Lease lease = UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 3);
                if (lease != null) {
                    lease.close();
                }
                return lease;
            }
        }
    }
}
