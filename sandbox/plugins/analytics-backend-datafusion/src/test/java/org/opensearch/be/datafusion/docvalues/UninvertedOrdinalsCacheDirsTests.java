/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

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
 * Tests for the per-shard ord-file location: {@link OrdFilePaths#resolveOrdsDir} and building
 * end to end through {@link UninvertedOrdinalsCache#acquire}.
 */
public class UninvertedOrdinalsCacheDirsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Another test in this JVM may have closed a plugin, which sets the cache's static
        // shuttingDown and makes every later build refuse.
        UninvertedOrdinalsCache.start();
    }

    public void testResolveOrdsDirIsShardSiblingOfFsStore() throws Exception {
        Path shardDir = createTempDir();
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory directory = FSDirectory.open(storeDir)) {
            assertEquals(shardDir.resolve("parquet-ords"), OrdFilePaths.resolveOrdsDir(directory));
        }
    }

    public void testResolveOrdsDirUnwrapsFilterDirectories() throws Exception {
        Path shardDir = createTempDir();
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        try (Directory fs = FSDirectory.open(storeDir); Directory wrapped = new FilterDirectory(new FilterDirectory(fs) {
        }) {
        }) {
            assertEquals(shardDir.resolve("parquet-ords"), OrdFilePaths.resolveOrdsDir(wrapped));
        }
    }

    public void testResolveOrdsDirRefusesNonFsDirectories() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            assertNull("no shard folder exists for a non-filesystem directory", OrdFilePaths.resolveOrdsDir(directory));
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

    /** End to end through {@link UninvertedOrdinalsCache#acquire}: the .ord file is built under {@code <shard>/parquet-ords/}. */
    public void testAcquireBuildsUnderShardPath() throws Exception {
        Path shardDir = createTempDir();
        assertNotNull("acquire must build on a fresh shard", acquireOnFreshShard(shardDir));
        try (var listing = Files.list(shardDir.resolve("parquet-ords"))) {
            assertTrue("an .ord file must exist under <shard>/parquet-ords", listing.anyMatch(f -> f.toString().endsWith(".ord")));
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
