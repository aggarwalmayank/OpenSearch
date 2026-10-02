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
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

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
        UninvertedOrdinalsCache.setBuildBreaker(new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
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

    /** A failed preparation is not remembered: once the obstacle is gone, the next call creates the folder. */
    public void testPrepareDirRetriesAfterFailure() throws Exception {
        Path ordsDir = createTempDir().resolve("parquet-ords");
        // A plain file where the folder should go makes createDirectories throw.
        Files.createFile(ordsDir);
        expectThrows(IOException.class, () -> OrdFilePaths.prepareDir(ordsDir));

        Files.delete(ordsDir);
        OrdFilePaths.prepareDir(ordsDir);
        assertTrue("the second call must create the folder", Files.isDirectory(ordsDir));
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
                // Stamp a parquet attribute so acquire reaches the non-FS check; a ByteBuffersDirectory
                // still has no ords dir, so acquire returns null and nothing is written.
                leaf.getSegmentInfo().info.putAttribute(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE, "_parquet_file_generation_1.parquet");
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
                leaf.getSegmentInfo().info.putAttribute(
                    ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE,
                    shardDir.resolve("parquet").resolve("_parquet_file_generation_1.parquet").toString()
                );
                UninvertedOrdinalsCache.Lease lease = UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 3);
                if (lease != null) {
                    lease.close();
                }
                return lease;
            }
        }
    }

    /** A build that trips the breaker leaves no file behind and the reservation is fully unwound. */
    public void testBuildTrippingBreakerLeavesNoFileAndReleasesReservation() throws Exception {
        AccountingBreaker breaker = new AccountingBreaker(1L); // 1 byte: any build reservation trips it
        UninvertedOrdinalsCache.setBuildBreaker(breaker);
        try {
            Path shardDir = createTempDir();
            expectThrows(CircuitBreakingException.class, () -> acquireOnFreshShard(shardDir));
            Path ordsDir = shardDir.resolve("parquet-ords");
            if (Files.exists(ordsDir)) {
                try (var listing = Files.list(ordsDir)) {
                    assertFalse(
                        "a tripped build must leave no .ord or .tmp file",
                        listing.anyMatch(f -> f.toString().endsWith(".ord") || f.toString().endsWith(".tmp"))
                    );
                }
            }
            assertEquals("the reservation must be unwound after a trip", 0L, breaker.getUsed());
        } finally {
            UninvertedOrdinalsCache.setBuildBreaker(new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
        }
    }

    /** A successful build reserves and then releases, ending with the breaker back at zero. */
    public void testSuccessfulBuildEndsWithBreakerAtZero() throws Exception {
        AccountingBreaker breaker = new AccountingBreaker(Long.MAX_VALUE);
        UninvertedOrdinalsCache.setBuildBreaker(breaker);
        try {
            assertNotNull("acquire must build on a fresh shard", acquireOnFreshShard(createTempDir()));
            assertEquals("the reservation must be released after a successful build", 0L, breaker.getUsed());
        } finally {
            UninvertedOrdinalsCache.setBuildBreaker(new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));
        }
    }

    /** Minimal byte-accounting breaker: trips when a reservation would exceed its limit. */
    private static final class AccountingBreaker extends NoopCircuitBreaker {
        private final long limit;
        private final AtomicLong used = new AtomicLong();

        AccountingBreaker(long limit) {
            super(CircuitBreaker.FIELDDATA);
            this.limit = limit;
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            if (used.addAndGet(bytes) > limit) {
                used.addAndGet(-bytes);
                throw new CircuitBreakingException("ord build over limit: " + label, used.get() + bytes, limit, getDurability());
            }
            return used.get();
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            return used.addAndGet(bytes);
        }

        @Override
        public long getUsed() {
            return used.get();
        }
    }

    /** A segment without its parquet-file attribute cannot locate its ord file, so acquire rejects it. */
    public void testAcquireRejectsSegmentWithoutParquetFileAttribute() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                Document document = new Document();
                document.add(new StringField("city", "delhi", Field.Store.NO));
                writer.addDocument(document);
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                // No PARQUET_FILE_ATTRIBUTE stamped.
                IllegalStateException missing = expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 1)
                );
                assertTrue(missing.getMessage().contains("cannot locate its uninverted-ordinal file"));
                leaf.getSegmentInfo().info.putAttribute(ParquetSegmentLayout.PARQUET_FILE_ATTRIBUTE, "");
                expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinalsCache.acquire(leaf, leaf.getSegmentInfo().info, "city", 1)
                );
            }
        }
    }

    /** A file name that does not end in .parquet is rejected, never used as a stem. */
    public void testParquetFileStemRejectsANonParquetName() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> OrdFilePaths.parquetFileStem("_parquet_file_generation_1.txt")
        );
        assertTrue(e.getMessage().contains("expected a parquet file name ending in .parquet"));
    }
}
