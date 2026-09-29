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
import org.opensearch.be.datafusion.docvalues.bridge.ParquetCodecBridge.ColumnValueCounts;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class UninvertedOrdinalsTests extends OpenSearchTestCase {

    /** Parquet's view of the tags example: 6 values stored (red twice in doc 3), 3 rows with values, a list column. */
    private static final ColumnValueCounts TAGS_COUNTS = new ColumnValueCounts(6, 3, true);
    /** Header positions: magic 4, version 4, maxDoc 4, termCount 8, assignedDocs 8, interval 4, block shift 4, dense 1. */
    private static final long MULTI_VALUED_FLAG_OFFSET = 37;
    private static final long ORDINAL_COUNT_OFFSET = 38;

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
                    UninvertedOrdinals built = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        baseTerms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(termCount, termCount, false),
                        () -> false
                    )
                ) {
                    assertEquals(termCount, built.valueCount());
                    UninvertedOrdinals.OrdinalCursor cursor = built.newOrdinalCursor();
                    assertEquals(0, cursor.ordinal(0));
                    assertEquals(termCount - 1, cursor.ordinal(termCount - 1));
                }

                AtomicInteger iteratorCalls = new AtomicInteger();
                Terms countingTerms = countingTerms(baseTerms, iteratorCalls);
                try (
                    UninvertedOrdinals reloaded = UninvertedOrdinals.load(
                        ordsDir,
                        fileKey,
                        countingTerms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(termCount, termCount, false)
                    )
                ) {
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
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        terms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(termCount, termCount, false),
                        () -> false
                    )
                ) {
                    // built with the default interval
                }

                // Simulate a dynamic cluster-setting change to a different interval.
                UninvertedOrdinals.setCheckpointInterval(buildInterval * 2 + 7);
                try {
                    AtomicInteger iteratorCalls = new AtomicInteger();
                    Terms countingTerms = countingTerms(terms, iteratorCalls);
                    try (
                        UninvertedOrdinals reloaded = UninvertedOrdinals.load(
                            ordsDir,
                            fileKey,
                            countingTerms,
                            leaf.maxDoc(),
                            new ColumnValueCounts(termCount, termCount, false)
                        )
                    ) {
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

    public void testCorruptAssignedDocsMetadataTriggersRebuild() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "assigned-docs";
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            addDoc(writer, "gamma");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms baseTerms = leaf.terms("f");
                assertNotNull(baseTerms);

                try (
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        baseTerms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(3, 3, false),
                        () -> false
                    )
                ) {
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
                    UninvertedOrdinals.load(ordsDir, fileKey, countingTerms, leaf.maxDoc(), new ColumnValueCounts(3, 3, false))
                );
                assertFalse("the invalid file must be deleted so a rebuild can replace it", Files.exists(ordFile));
                try (
                    UninvertedOrdinals rebuilt = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        countingTerms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(3, 3, false),
                        () -> false
                    )
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
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

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
                    () -> UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), new ColumnValueCounts(3, 3, false), () -> false)
                );
                assertTrue(e.getMessage().contains("ordinal coverage mismatch"));
                assertFalse("coverage mismatch should fail before publishing the ord file", Files.exists(ordFile));
            }
        }
    }

    public void testBitFlipInsideOrdinalStreamIsRejectedAndWarned() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "bit-flip";
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            addDoc(writer, "gamma");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        terms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(3, 3, false),
                        () -> false
                    )
                ) {
                    assertTrue(Files.exists(ordFile));
                }

                // Flip one bit in the ordinal stream body: header fields stay plausible, only the CRC can catch it.
                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    long bodyOffset = 40; // inside the ordinal stream (fixed header is 38 bytes)
                    ByteBuffer one = ByteBuffer.allocate(1);
                    channel.read(one, bodyOffset);
                    one.put(0, (byte) (one.get(0) ^ 0x01)).rewind();
                    channel.write(one, bodyOffset);
                }

                assertNull(
                    "a bit flip inside the body must be rejected by the checksum",
                    UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), new ColumnValueCounts(3, 3, false))
                );
                assertFalse("the corrupt file must be deleted so a rebuild can replace it", Files.exists(ordFile));
            }
        }
    }

    public void testTruncatedFileIsRejected() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "truncated";
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        terms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(2, 2, false),
                        () -> false
                    )
                ) {
                    assertTrue(Files.exists(ordFile));
                }

                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.WRITE)) {
                    channel.truncate(channel.size() - 5);
                }

                assertNull(
                    "a truncated file must be rejected",
                    UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), new ColumnValueCounts(2, 2, false))
                );
                assertFalse(Files.exists(ordFile));
            }
        }
    }

    public void testUnsupportedVersionFileIsRejectedIntoTheRebuildPath() throws Exception {
        Path ordsDir = createTempDir();
        String fileKey = "old-version";
        Path ordFile = ordsDir.resolve("parquet-ords-" + fileKey + ".ord");

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                Terms terms = leaf.terms("f");
                assertNotNull(terms);

                try (
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        terms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(2, 2, false),
                        () -> false
                    )
                ) {
                    assertTrue(Files.exists(ordFile));
                }

                // Rewrite the version field to 99: a version this code does not support must be rejected, not misread.
                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.WRITE)) {
                    byte[] unsupported = { 99, 0, 0, 0 }; // little-endian int 99
                    channel.write(ByteBuffer.wrap(unsupported), 4); // after the magic
                }

                assertNull(
                    "a file of an unsupported version must be rejected into the rebuild path",
                    UninvertedOrdinals.load(ordsDir, fileKey, terms, leaf.maxDoc(), new ColumnValueCounts(2, 2, false))
                );
                assertFalse(Files.exists(ordFile));

                try (
                    UninvertedOrdinals rebuilt = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        terms,
                        leaf.maxDoc(),
                        new ColumnValueCounts(2, 2, false),
                        () -> false
                    )
                ) {
                    assertEquals("beta", rebuilt.term(1).utf8ToString());
                }
            }
        }
    }

    /** A multi-valued file gives each document its distinct words in ascending ordinal order, after build and after load. */
    public void testMultiValuedBuildAndLoadServeEachDocumentsOrdinals() throws Exception {
        Path ordsDir = createTempDir();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals built = buildTags(ordsDir, leaf)) {
                    assertTrue(built.isMultiValued());
                    assertEquals("blue, green, red", 3, built.valueCount());
                    assertTagsOrdinals(built);
                }
                try (UninvertedOrdinals loaded = UninvertedOrdinals.load(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), TAGS_COUNTS)) {
                    assertNotNull("a valid multi-valued file must load", loaded);
                    assertTrue(loaded.isMultiValued());
                    assertTagsOrdinals(loaded);
                }
            }
        }
    }

    /** Asking about an earlier document restarts the presence bitmap, so any access order gets the right answer. */
    public void testMultiValuedCursorAnswersDocumentsInAnyOrder() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildTags(createTempDir(), leaf)) {
                    UninvertedOrdinals.MultiValuedOrdinalsCursor cursor = ordinals.newMultiValuedOrdinalsCursor();
                    assertOrdinals(cursor, 3, 1, 2);
                    assertOrdinals(cursor, 0, 0, 2);
                    assertOrdinals(cursor, 3, 1, 2);
                    assertOrdinals(cursor, 2);
                }
            }
        }
    }

    /** When every document has a value the file carries no presence bitmap, and document n is entry n. */
    public void testMultiValuedDenseFileServesEveryDocument() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addTagsDoc(writer, "red", "blue");
            addTagsDoc(writer, "blue");
            addTagsDoc(writer, "green");
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (
                    UninvertedOrdinals ordinals = UninvertedOrdinals.build(
                        createTempDir(),
                        "dense",
                        leaf.terms("f"),
                        leaf.maxDoc(),
                        new ColumnValueCounts(4, 3, true),
                        () -> false
                    )
                ) {
                    UninvertedOrdinals.MultiValuedOrdinalsCursor cursor = ordinals.newMultiValuedOrdinalsCursor();
                    assertOrdinals(cursor, 0, 0, 2);
                    assertOrdinals(cursor, 1, 0);
                    assertOrdinals(cursor, 2, 1);
                }
            }
        }
    }

    /** Each file kind refuses the other kind's cursor, naming the one to use. */
    public void testEachFileKindRefusesTheOtherKindsCursor() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals multiValued = buildTags(createTempDir(), leaf)) {
                    IllegalStateException e = expectThrows(IllegalStateException.class, multiValued::newOrdinalCursor);
                    assertTrue(e.getMessage(), e.getMessage().contains("newMultiValuedOrdinalsCursor"));
                }
            }
        }
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (
                    UninvertedOrdinals singleValued = UninvertedOrdinals.build(
                        createTempDir(),
                        "single",
                        leaf.terms("f"),
                        leaf.maxDoc(),
                        new ColumnValueCounts(2, 2, false),
                        () -> false
                    )
                ) {
                    IllegalStateException e = expectThrows(IllegalStateException.class, singleValued::newMultiValuedOrdinalsCursor);
                    assertTrue(e.getMessage(), e.getMessage().contains("newOrdinalCursor"));
                }
            }
        }
    }

    /** A file whose shape differs from the column's is deleted on load, in both directions. */
    public void testShapeMismatchIsRejectedOnLoad() throws Exception {
        Path ordsDir = createTempDir();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ignored = buildTags(ordsDir, leaf)) {
                    // built as multi-valued
                }
                assertNull(
                    "a multi-valued file must not load for a plain column",
                    UninvertedOrdinals.load(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), new ColumnValueCounts(6, 3, false))
                );
                assertFalse(Files.exists(ordsDir.resolve("parquet-ords-tags.ord")));
            }
        }
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            addDoc(writer, "beta");
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (
                    UninvertedOrdinals ignored = UninvertedOrdinals.build(
                        ordsDir,
                        "plain",
                        leaf.terms("f"),
                        leaf.maxDoc(),
                        new ColumnValueCounts(2, 2, false),
                        () -> false
                    )
                ) {
                    // built as single-valued
                }
                assertNull(
                    "a single-valued file must not load for a list column",
                    UninvertedOrdinals.load(ordsDir, "plain", leaf.terms("f"), leaf.maxDoc(), new ColumnValueCounts(2, 2, true))
                );
                assertFalse(Files.exists(ordsDir.resolve("parquet-ords-plain.ord")));
            }
        }
    }

    /** Loading refuses a file whose document count differs from Parquet's, or whose ordinals exceed Parquet's values. */
    public void testMultiValuedCountMismatchesAreRejectedOnLoad() throws Exception {
        Path ordsDir = createTempDir();
        Path ordFile = ordsDir.resolve("parquet-ords-tags.ord");
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ignored = buildTags(ordsDir, leaf)) {
                    assertTrue(Files.exists(ordFile));
                }
                assertNull(
                    "3 documents with values in the file, 2 in Parquet",
                    UninvertedOrdinals.load(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), new ColumnValueCounts(6, 2, true))
                );
                assertFalse(Files.exists(ordFile));

                try (UninvertedOrdinals ignored = buildTags(ordsDir, leaf)) {
                    assertTrue(Files.exists(ordFile));
                }
                assertNull(
                    "5 ordinals in the file, only 4 values in Parquet",
                    UninvertedOrdinals.load(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), new ColumnValueCounts(4, 3, true))
                );
                assertFalse(Files.exists(ordFile));
            }
        }
    }

    /** The build refuses to publish a multi-valued file whose document count differs from Parquet's. */
    public void testMultiValuedCoverageMismatchFailsBeforePublishingOrdFile() throws Exception {
        Path ordsDir = createTempDir();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinals.build(
                        ordsDir,
                        "tags",
                        leaf.terms("f"),
                        leaf.maxDoc(),
                        new ColumnValueCounts(6, 4, true),
                        () -> false
                    )
                );
                assertTrue(e.getMessage(), e.getMessage().contains("tags"));
                assertFalse(Files.exists(ordsDir.resolve("parquet-ords-tags.ord")));
            }
        }
    }

    /** A truncated multi-valued file fails the checksum and is deleted. */
    public void testTruncatedMultiValuedFileIsRejected() throws Exception {
        Path ordsDir = createTempDir();
        Path ordFile = ordsDir.resolve("parquet-ords-tags.ord");
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ignored = buildTags(ordsDir, leaf)) {
                    assertTrue(Files.exists(ordFile));
                }
                try (FileChannel channel = FileChannel.open(ordFile, StandardOpenOption.WRITE)) {
                    channel.truncate(channel.size() - 5);
                }
                assertNull(UninvertedOrdinals.load(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), TAGS_COUNTS));
                assertFalse(Files.exists(ordFile));
            }
        }
    }

    /** The build refuses a multi-valued file that would hold more ordinals than Parquet stores values. */
    public void testMultiValuedBuildWithMoreOrdinalsThanParquetValuesFails() throws Exception {
        Path ordsDir = createTempDir();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> UninvertedOrdinals.build(
                        ordsDir,
                        "tags",
                        leaf.terms("f"),
                        leaf.maxDoc(),
                        new ColumnValueCounts(4, 3, true),
                        () -> false
                    )
                );
                assertTrue(e.getMessage(), e.getMessage().contains("5 document and value pairs"));
                assertFalse(Files.exists(ordsDir.resolve("parquet-ords-tags.ord")));
            }
        }
    }

    /** A cancelled multi-valued build stops before writing anything. */
    public void testCancelledMultiValuedBuildWritesNothing() throws Exception {
        Path ordsDir = createTempDir();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                IOException e = expectThrows(
                    IOException.class,
                    () -> UninvertedOrdinals.build(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), TAGS_COUNTS, () -> true)
                );
                assertTrue(e.getMessage(), e.getMessage().contains("cancelled"));
                // Lucene's test file system may add stray "extra" files to temp dirs, so look only for ours.
                try (var listing = Files.list(ordsDir)) {
                    assertTrue(
                        "neither the .ord nor its .tmp may be left behind",
                        listing.noneMatch(f -> f.getFileName().toString().startsWith("parquet-ords-"))
                    );
                }
            }
        }
    }

    /** Terms and ranks resolve across several checkpoints in a multi-valued file. */
    public void testMultiValuedFileResolvesTermsAcrossCheckpoints() throws Exception {
        final int defaultInterval = UninvertedOrdinals.configuredCheckpointInterval();
        UninvertedOrdinals.setCheckpointInterval(2);
        try {
            Path ordsDir = createTempDir();
            try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
                // 7 words over 3 documents: term-0000 .. term-0006, so checkpoints at 0, 2, 4 and 6.
                addTagsDoc(writer, termValue(0), termValue(3), termValue(6));
                addTagsDoc(writer, termValue(1), termValue(4));
                addTagsDoc(writer, termValue(2), termValue(5), termValue(5));
                writer.forceMerge(1);
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    LeafReader leaf = reader.leaves().get(0).reader();
                    ColumnValueCounts counts = new ColumnValueCounts(8, 3, true);
                    try (
                        UninvertedOrdinals ignored = UninvertedOrdinals.build(
                            ordsDir,
                            "many",
                            leaf.terms("f"),
                            leaf.maxDoc(),
                            counts,
                            () -> false
                        )
                    ) {
                        // built with interval 2
                    }
                    try (UninvertedOrdinals loaded = UninvertedOrdinals.load(ordsDir, "many", leaf.terms("f"), leaf.maxDoc(), counts)) {
                        assertNotNull(loaded);
                        for (int ord = 0; ord < 7; ord++) {
                            assertEquals(termValue(ord), loaded.term(ord).utf8ToString());
                            assertEquals(ord, loaded.rank(new BytesRef(termValue(ord))));
                        }
                        UninvertedOrdinals.MultiValuedOrdinalsCursor cursor = loaded.newMultiValuedOrdinalsCursor();
                        assertOrdinals(cursor, 0, 0, 3, 6);
                        assertOrdinals(cursor, 1, 1, 4);
                        assertOrdinals(cursor, 2, 2, 5);
                    }
                }
            }
        } finally {
            UninvertedOrdinals.setCheckpointInterval(defaultInterval);
        }
    }

    /** A multi-valued flag byte other than 0 or 1 is rejected on load. */
    public void testInvalidMultiValuedFlagIsRejected() throws Exception {
        assertRejectedAfterHeaderEdit(MULTI_VALUED_FLAG_OFFSET, new byte[] { 2 }, false, TAGS_COUNTS);
    }

    /** An ordinal count that needs more bytes than the file holds is rejected, even with a valid checksum. */
    public void testOrdinalCountLargerThanTheFileIsRejected() throws Exception {
        assertRejectedAfterHeaderEdit(ORDINAL_COUNT_OFFSET, littleEndianLong(50), true, new ColumnValueCounts(100, 3, true));
    }

    /** Document starts that do not end at the header's ordinal count are rejected, even with a valid checksum. */
    public void testDocumentStartsNotEndingAtTheOrdinalCountAreRejected() throws Exception {
        assertRejectedAfterHeaderEdit(ORDINAL_COUNT_OFFSET, littleEndianLong(4), true, TAGS_COUNTS);
    }

    /** The single-valued cursor still answers an earlier document after a later one, through the shared bitmap helper. */
    public void testSingleValuedCursorAnswersDocumentsInAnyOrder() throws Exception {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            addDoc(writer, "alpha");
            writer.addDocument(new Document());
            addDoc(writer, "beta");
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (
                    UninvertedOrdinals ordinals = UninvertedOrdinals.build(
                        createTempDir(),
                        "sparse",
                        leaf.terms("f"),
                        leaf.maxDoc(),
                        new ColumnValueCounts(2, 2, false),
                        () -> false
                    )
                ) {
                    UninvertedOrdinals.OrdinalCursor cursor = ordinals.newOrdinalCursor();
                    assertEquals(1, cursor.ordinal(2));
                    assertEquals(0, cursor.ordinal(0));
                    assertEquals(-1, cursor.ordinal(1));
                    assertEquals(1, cursor.ordinal(2));
                }
            }
        }
    }

    /**
     * Builds the tags file, overwrites {@code bytes} at {@code offset}, optionally recomputes the checksum so only the
     * structural checks can catch the edit, and asserts the load rejects and deletes the file.
     */
    private void assertRejectedAfterHeaderEdit(long offset, byte[] bytes, boolean fixChecksum, ColumnValueCounts loadCounts)
        throws Exception {
        Path ordsDir = createTempDir();
        Path ordFile = ordsDir.resolve("parquet-ords-tags.ord");
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newDeterministicConfig())) {
            indexTagsExample(writer);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ignored = buildTags(ordsDir, leaf)) {
                    assertTrue(Files.exists(ordFile));
                }
                byte[] file = Files.readAllBytes(ordFile);
                System.arraycopy(bytes, 0, file, (int) offset, bytes.length);
                if (fixChecksum) {
                    CRC32 crc = new CRC32();
                    crc.update(file, 0, file.length - Long.BYTES);
                    byte[] stored = littleEndianLong(crc.getValue());
                    System.arraycopy(stored, 0, file, file.length - Long.BYTES, Long.BYTES);
                }
                Files.write(ordFile, file);
                assertNull(UninvertedOrdinals.load(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), loadCounts));
                assertFalse("the invalid file must be deleted so a rebuild can replace it", Files.exists(ordFile));
            }
        }
    }

    private static byte[] littleEndianLong(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    /** doc 0 [red, blue], doc 1 [blue], doc 2 nothing, doc 3 [red, red, green]; words 0 blue, 1 green, 2 red. */
    private static void indexTagsExample(IndexWriter writer) throws Exception {
        addTagsDoc(writer, "red", "blue");
        addTagsDoc(writer, "blue");
        addTagsDoc(writer);
        addTagsDoc(writer, "red", "red", "green");
        writer.forceMerge(1);
    }

    private static UninvertedOrdinals buildTags(Path ordsDir, LeafReader leaf) throws Exception {
        return UninvertedOrdinals.build(ordsDir, "tags", leaf.terms("f"), leaf.maxDoc(), TAGS_COUNTS, () -> false);
    }

    private static void assertTagsOrdinals(UninvertedOrdinals ordinals) {
        UninvertedOrdinals.MultiValuedOrdinalsCursor cursor = ordinals.newMultiValuedOrdinalsCursor();
        assertOrdinals(cursor, 0, 0, 2);
        assertOrdinals(cursor, 1, 0);
        assertOrdinals(cursor, 2);
        assertOrdinals(cursor, 3, 1, 2);
    }

    private static void assertOrdinals(UninvertedOrdinals.MultiValuedOrdinalsCursor cursor, int doc, long... expected) {
        assertEquals("ordinal count of doc " + doc, expected.length, cursor.advance(doc));
        for (long ordinal : expected) {
            assertEquals("ordinal of doc " + doc, ordinal, cursor.nextOrdinal());
        }
    }

    private static void addTagsDoc(IndexWriter writer, String... tags) throws Exception {
        Document doc = new Document();
        doc.add(new StringField("id", "x", Field.Store.NO));
        for (String tag : tags) {
            doc.add(new StringField("f", tag, Field.Store.NO));
        }
        writer.addDocument(doc);
    }

    private static Terms countingTerms(Terms delegate, AtomicInteger iteratorCalls) {
        return new FilterLeafReader.FilterTerms(delegate) {
            @Override
            public TermsEnum iterator() throws java.io.IOException {
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
        return String.format(java.util.Locale.ROOT, "term-%04d", ord);
    }
}
