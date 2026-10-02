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
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class UninvertedOrdinalsTests extends OpenSearchTestCase {

    /**
     * Doc→ord assertions in these tests require document order to survive {@code forceMerge};
     * randomized merge policies may shuffle docs, so the writer is deliberately deterministic.
     */
    private static IndexWriterConfig newDeterministicConfig() {
        return new IndexWriterConfig().setMergePolicy(new LogDocMergePolicy());
    }

    public void testReloadUsesPersistedCheckpointsWithoutCheckpointRebuild() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "reload";
        final int checkpointInterval = UninvertedOrdinals.configuredCheckpointInterval();

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            final int termCount = checkpointInterval + 128;
            for (int i = 0; i < termCount; i++) {
                Document doc = new Document();
                doc.add(new StringField("f", termValue(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms baseTerms = leaf.terms("f");
                assertNotNull(baseTerms);

                try (
                    UninvertedOrdinals built = UninvertedOrdinals.build(ordsDir, fileKey, baseTerms, leaf.maxDoc(), termCount, () -> false)
                ) {
                    assertEquals(termCount, built.valueCount());
                    UninvertedOrdinals.OrdinalCursor cursor = built.newOrdinalCursor();
                    assertEquals(0, cursor.ordinal(0));
                    assertEquals(termCount - 1, cursor.ordinal(termCount - 1));
                }

                AtomicInteger iteratorCalls = new AtomicInteger();
                Terms countingTerms = countingTerms(baseTerms, iteratorCalls);
                try (UninvertedOrdinals reloaded = UninvertedOrdinals.load(ordsDir, fileKey, countingTerms, leaf.maxDoc(), termCount)) {
                    assertEquals("existing .ord should load without rebuilding checkpoints", 0, iteratorCalls.get());
                    assertEquals(termCount, reloaded.valueCount());
                    assertEquals(termCount - 1, reloaded.newOrdinalCursor().ordinal(termCount - 1));
                    int probe = checkpointInterval + 5;
                    assertEquals(termValue(probe), reloaded.term(probe).utf8ToString());
                    assertEquals(1, iteratorCalls.get());
                    assertEquals(probe, reloaded.rank(new BytesRef(termValue(probe))));
                }
            }
        }
    }

    /**
     * The interval a .ord was built with is recorded in its header and honoured on read, so a
     * setting change must not invalidate an existing file nor shift its ord&harr;term mapping.
     */
    public void testReloadHonoursTheIntervalTheFileWasBuiltWith() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "interval-change";
        final int buildInterval = UninvertedOrdinals.configuredCheckpointInterval();
        final int termCount = buildInterval + 64;

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            for (int i = 0; i < termCount; i++) {
                Document doc = new Document();
                doc.add(new StringField("f", termValue(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), termCount, () -> false)
                ) {
                    // built with the default interval
                }

                // Simulate a dynamic cluster-setting change to a different interval.
                UninvertedOrdinals.setCheckpointInterval(buildInterval * 2 + 7);
                try {
                    AtomicInteger iteratorCalls = new AtomicInteger();
                    Terms countingTerms = countingTerms(terms, iteratorCalls);
                    try (UninvertedOrdinals reloaded = UninvertedOrdinals.load(ordsDir, fileKey, countingTerms, leaf.maxDoc(), termCount)) {
                        assertNotNull("existing file must load regardless of the live interval setting", reloaded);
                        assertEquals("interval change must not force a re-uninvert", 0, iteratorCalls.get());
                        for (int ord : new int[] { 0, 1, buildInterval - 1, buildInterval, buildInterval + 5, termCount - 1 }) {
                            assertEquals("ord->term must survive the setting change", termValue(ord), reloaded.term(ord).utf8ToString());
                            assertEquals("term->ord must survive the setting change", ord, reloaded.rank(new BytesRef(termValue(ord))));
                        }
                    }
                } finally {
                    UninvertedOrdinals.setCheckpointInterval(buildInterval);
                }
            }
        }
    }

    /**
     * A sparse field whose values span two blocks, the first one constant, must read back
     * the ordinal of every document on both sides of the block boundary.
     */
    public void testSparseFieldAcrossBlocksReadsBackEveryOrdinal() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "sparse-blocks";
        final int blockSize = 1 << 16; // UninvertedOrdinals.BLOCK_SIZE
        final int numPresent = blockSize + 1000;
        final int maxDoc = numPresent * 2;

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            for (int doc = 0; doc < maxDoc; doc++) {
                Document document = new Document();
                if (doc % 2 == 0) {
                    document.add(new StringField("f", sparseValue(doc / 2, blockSize), Field.Store.NO));
                }
                writer.addDocument(document);
            }
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (UninvertedOrdinals built = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), numPresent, () -> false)) {
                    assertFalse(built.isDense());
                    assertSparseOrdinals(built, maxDoc, blockSize);
                }
                try (UninvertedOrdinals reloaded = UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), numPresent)) {
                    assertNotNull(reloaded);
                    assertSparseOrdinals(reloaded, maxDoc, blockSize);
                }
            }
        }
    }

    public void testCorruptAssignedDocsMetadataTriggersRebuild() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "assigned-docs";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            addDoc(writer, "gamma");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms baseTerms = leaf.terms("f");
                assertNotNull(baseTerms);

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, baseTerms, leaf.maxDoc(), 3, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.WRITE)) {
                    ByteBuffer assignedDocs = ByteBuffer.allocate(Long.BYTES).putLong(2L).flip();
                    channel.write(assignedDocs, 20L); // magic, version, maxDoc, termCount
                }

                AtomicInteger iteratorCalls = new AtomicInteger();
                Terms countingTerms = countingTerms(baseTerms, iteratorCalls);
                assertNull(
                    "corrupt assignedDocs metadata must be rejected by load",
                    UninvertedOrdinals.load(ordsDir, fileKey, countingTerms, leaf.maxDoc(), 3)
                );
                assertFalse("the invalid file must be deleted so a rebuild can replace it", Files.exists(ordFile));
                try (
                    UninvertedOrdinals rebuilt = UninvertedOrdinals.build(ordsDir, fileKey, countingTerms, leaf.maxDoc(), 3, () -> false)
                ) {
                    assertTrue("rebuild walks the postings", iteratorCalls.get() > 0);
                    assertEquals("beta", rebuilt.term(1).utf8ToString());
                    assertEquals(2, rebuilt.rank(new BytesRef("gamma")));
                }
            }
        }
    }

    public void testCoverageMismatchFailsBeforePublishingOrdFile() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "coverage-mismatch";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.addDocument(new Document());
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 3, () -> false)
                );
                assertTrue(e.getMessage().contains("ordinal coverage mismatch"));
                assertFalse("coverage mismatch should fail before publishing the ord file", Files.exists(ordFile));
            }
        }
    }

    public void testBitFlipInsideOrdinalStreamIsRejectedAndWarned() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "bit-flip";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            addDoc(writer, "gamma");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 3, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                // Flip one bit in the ordinal stream body: header fields stay plausible, only the CRC can catch it.
                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    long bodyOffset = 40; // inside the ordinal stream (fixed header is 37 bytes)
                    ByteBuffer one = ByteBuffer.allocate(1);
                    channel.read(one, bodyOffset);
                    one.put(0, (byte) (one.get(0) ^ 0x01)).rewind();
                    channel.write(one, bodyOffset);
                }

                assertNull(
                    "a bit flip inside the body must be rejected by the checksum",
                    UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), 3)
                );
                assertFalse("the corrupt file must be deleted so a rebuild can replace it", Files.exists(ordFile));
            }
        }
    }

    public void testTruncatedFileIsRejected() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "truncated";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 2, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.WRITE)) {
                    channel.truncate(channel.size() - 5);
                }

                assertNull("a truncated file must be rejected", UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), 2));
                assertFalse(Files.exists(ordFile));
            }
        }
    }

    public void testVersion1FileIsRejectedIntoTheRebuildPath() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "old-version";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 2, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                // Rewrite the version field to 1: pre-checksum files must be rejected, not misread.
                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.WRITE)) {
                    byte[] v1 = { 1, 0, 0, 0 }; // little-endian int 1
                    channel.write(ByteBuffer.wrap(v1), 4); // magic
                }

                assertNull(
                    "a version-1 file must be rejected into the rebuild path",
                    UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), 2)
                );
                assertFalse(Files.exists(ordFile));

                try (UninvertedOrdinals rebuilt = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 2, () -> false)) {
                    assertEquals("beta", rebuilt.term(1).utf8ToString());
                }
            }
        }
    }

    private static Terms countingTerms(Terms delegate, AtomicInteger iteratorCalls) {
        return new FilterLeafReader.FilterTerms(delegate) {
            @Override
            public TermsEnum iterator() throws IOException {
                iteratorCalls.incrementAndGet();
                return in.iterator();
            }
        };
    }

    private static void addDoc(IndexWriter writer, String value) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("f", value, Field.Store.NO));
        writer.addDocument(doc);
    }

    private static String termValue(int ord) {
        return String.format(Locale.ROOT, "term-%04d", ord);
    }

    private static String sparseValue(int valueIndex, int blockSize) {
        return valueIndex < blockSize ? "a" : "b" + (valueIndex % 7);
    }

    private static void assertSparseOrdinals(UninvertedOrdinals ords, int maxDoc, int blockSize) throws Exception {
        UninvertedOrdinals.OrdinalCursor cursor = ords.newOrdinalCursor();
        for (int doc = 0; doc < maxDoc; doc++) {
            int valueIndex = doc / 2;
            int expected = doc % 2 == 1 ? -1 : valueIndex < blockSize ? 0 : 1 + valueIndex % 7;
            assertEquals("doc " + doc, expected, cursor.ordinal(doc));
        }
    }

    /** An ord file built for a different maxDoc is stale; load deletes it and returns null. */
    public void testLoadRejectsFileBuiltForADifferentMaxDoc() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "maxdoc-mismatch";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 2, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                assertNull(
                    "an ord file built for a different maxDoc must be rejected",
                    UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc() + 1, 2)
                );
                assertFalse("the stale file must be deleted so a rebuild can replace it", Files.exists(ordFile));
            }
        }
    }

    /** Without a non-null count the build cannot verify coverage, so it refuses. */
    public void testBuildRefusesWhenNonNullCountUnknown() throws Exception {
        Path ordsDir = createTempDir();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinals.build(ordsDir, "no-stats", terms, leaf.maxDoc(), -1, () -> false)
                );
                assertTrue(e.getMessage().contains("cannot verify ordinal coverage"));
            }
        }
    }

    /** A cancelled build throws and leaves no ord file behind. */
    public void testBuildStopsWhenCancelled() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "cancelled";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                IOException e = expectThrows(
                    IOException.class,
                    () -> UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 1, () -> true)
                );
                assertTrue(e.getMessage().contains("ordinal build cancelled"));
                // The cancel check fires before any temp or ord file is created, so none is left behind.
                assertFalse("a cancelled build must leave no ord file", Files.exists(ordFile));
            }
        }
    }

    /** An ord file whose term count differs from the live segment is stale; load deletes it and returns null. */
    public void testLoadRejectsFileWithADifferentTermCount() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "termcount-mismatch";
        Path ordFile = ordsDir.resolve(fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            // One segment, one maxDoc: field f has 3 distinct terms, field g has 2.
            String[][] rows = { { "alpha", "x" }, { "beta", "x" }, { "gamma", "y" } };
            for (String[] row : rows) {
                Document doc = new Document();
                doc.add(new StringField("f", row[0], Field.Store.NO));
                doc.add(new StringField("g", row[1], Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms threeTerms = leaf.terms("f");
                Terms twoTerms = leaf.terms("g");
                assertNotNull(threeTerms);
                assertNotNull(twoTerms);
                assertEquals(3, threeTerms.size());
                assertEquals(2, twoTerms.size());

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, threeTerms, leaf.maxDoc(), 3, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                // Same maxDoc, fewer terms: validateMetadata rejects on termCount before coverage.
                assertNull(
                    "an ord file whose termCount differs from the segment must be rejected",
                    UninvertedOrdinals.load(ordsDir, fileKey, twoTerms, leaf.maxDoc(), 2)
                );
                assertFalse("the stale file must be deleted so a rebuild can replace it", Files.exists(ordFile));
            }
        }
    }
}
