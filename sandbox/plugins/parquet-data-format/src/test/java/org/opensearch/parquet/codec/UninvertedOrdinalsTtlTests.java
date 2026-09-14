/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

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
 * TTL eviction of uninverted-ordinal files: idle files are deleted by
 * {@link UninvertedOrdinalsCache#sweepExpiredOrdinals(long)}, in-use files are protected by the lease
 * refcount, fresh files survive, {@code ttl = 0} keeps files forever, and cold files with no
 * cache entry (orphans, never-queried shards) are removed by their last-modified time.
 */
public class UninvertedOrdinalsTtlTests extends OpenSearchTestCase {

    private static final long TTL_MILLIS = TimeValue.timeValueMinutes(10).millis();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        UninvertedOrdinalsCache.setTtl(TimeValue.timeValueMillis(TTL_MILLIS));
    }

    @Override
    public void tearDown() throws Exception {
        UninvertedOrdinalsCache.setTtl(TimeValue.timeValueDays(7));
        UninvertedOrdinalsCache.setDataRoots(new Path[0]);
        super.tearDown();
    }

    public void testSweepDeletesIdleFileAndKeepsFreshOne() throws Exception {
        Path shardDir = createTempDir();
        withOrdinalsBuilt(shardDir, (leaf, ordFile) -> {
            // Fresh: a sweep "now" (well within TTL) must keep the file.
            UninvertedOrdinalsCache.sweepExpiredOrdinals(System.currentTimeMillis());
            assertTrue("fresh file must survive the sweep", Files.exists(ordFile));

            // Idle past TTL: a sweep from the future must delete it.
            UninvertedOrdinalsCache.sweepExpiredOrdinals(System.currentTimeMillis() + TTL_MILLIS + 1000);
            assertFalse("idle file must be evicted", Files.exists(ordFile));
        });
    }

    public void testSweepNeverDeletesFileHeldByALease() throws Exception {
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

                // In use: even a far-future sweep must not touch it.
                UninvertedOrdinalsCache.sweepExpiredOrdinals(System.currentTimeMillis() + TTL_MILLIS * 10);
                assertTrue("in-use file must never be evicted", Files.exists(ordFile));

                // Released: the next expired sweep may evict it.
                lease.close();
                UninvertedOrdinalsCache.sweepExpiredOrdinals(System.currentTimeMillis() + TTL_MILLIS * 10);
                assertFalse("released idle file must be evicted", Files.exists(ordFile));
            }
        }
    }

    public void testTtlZeroKeepsFilesForever() throws Exception {
        UninvertedOrdinalsCache.setTtl(TimeValue.ZERO);
        Path shardDir = createTempDir();
        withOrdinalsBuilt(shardDir, (leaf, ordFile) -> {
            UninvertedOrdinalsCache.sweepExpiredOrdinals(System.currentTimeMillis() + TTL_MILLIS * 1000);
            assertTrue("ttl=0 must keep files forever", Files.exists(ordFile));
        });
    }

    /** Cold files (no cache entry — orphans of merged segments, never-queried shards) go by their last-modified time. */
    public void testSweepDeletesColdFilesByLastModifiedTime() throws Exception {
        Path root = createTempDir();
        Path ordsDir = root.resolve("nodes").resolve("0").resolve("indices").resolve("idxUuid").resolve("0").resolve("parquet-ords");
        Files.createDirectories(ordsDir);
        Path oldFile = ordsDir.resolve("parquet-ords-deadbeef-city.ord");
        Path freshFile = ordsDir.resolve("parquet-ords-cafebabe-city.ord");
        Files.write(oldFile, new byte[16]);
        Files.write(freshFile, new byte[16]);
        long now = System.currentTimeMillis();
        Files.setLastModifiedTime(oldFile, FileTime.fromMillis(now - TTL_MILLIS - 60_000));
        Files.setLastModifiedTime(freshFile, FileTime.fromMillis(now));
        UninvertedOrdinalsCache.setDataRoots(new Path[] { root });

        UninvertedOrdinalsCache.sweepExpiredOrdinals(now);

        assertFalse("cold file older than ttl must be deleted", Files.exists(oldFile));
        assertTrue("cold file within ttl must survive", Files.exists(freshFile));
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
