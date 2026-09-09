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

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
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
        final int checkpointInterval = ParquetDocValuesProducer.checkpointInterval();

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
                try (
                    UninvertedOrdinals reloaded = UninvertedOrdinals.build(
                        ordsDir,
                        fileKey,
                        countingTerms,
                        leaf.maxDoc(),
                        termCount,
                        () -> false
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
        final int buildInterval = ParquetDocValuesProducer.checkpointInterval();
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
                ParquetDocValuesProducer.setCheckpointInterval(buildInterval * 2 + 7);
                try {
                    AtomicInteger iteratorCalls = new AtomicInteger();
                    Terms countingTerms = countingTerms(terms, iteratorCalls);
                    try (
                        UninvertedOrdinals reloaded = UninvertedOrdinals.build(
                            ordsDir,
                            fileKey,
                            countingTerms,
                            leaf.maxDoc(),
                            termCount,
                            () -> false
                        )
                    ) {
                        assertEquals("interval change must not force a re-uninvert", 0, iteratorCalls.get());
                        for (int ord : new int[] { 0, 1, buildInterval - 1, buildInterval, buildInterval + 5, termCount - 1 }) {
                            assertEquals("ord->term must survive the setting change", termValue(ord), reloaded.term(ord).utf8ToString());
                            assertEquals("term->ord must survive the setting change", ord, reloaded.rank(new BytesRef(termValue(ord))));
                        }
                    }
                } finally {
                    ParquetDocValuesProducer.setCheckpointInterval(buildInterval);
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

                try (UninvertedOrdinals ignored = UninvertedOrdinals.build(ordsDir, fileKey, baseTerms, leaf.maxDoc(), 3, () -> false)) {
                    assertTrue(Files.exists(ordFile));
                }

                try (RandomAccessFile raf = new RandomAccessFile(ordFile.toFile(), "rw")) {
                    raf.seek(20L); // magic, version, maxDoc, termCount
                    raf.writeLong(2L);
                }

                AtomicInteger iteratorCalls = new AtomicInteger();
                Terms countingTerms = countingTerms(baseTerms, iteratorCalls);
                try (
                    UninvertedOrdinals rebuilt = UninvertedOrdinals.build(ordsDir, fileKey, countingTerms, leaf.maxDoc(), 3, () -> false)
                ) {
                    assertTrue("corrupt assignedDocs metadata should force rebuild", iteratorCalls.get() > 0);
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
                    () -> UninvertedOrdinals.build(ordsDir, fileKey, terms, leaf.maxDoc(), 3, () -> false)
                );
                assertTrue(e.getMessage().contains("ordinal coverage mismatch"));
                assertFalse("coverage mismatch should fail before publishing the ord file", Files.exists(ordFile));
            }
        }
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
