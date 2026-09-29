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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetCodecBridge.ColumnValueCounts;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedSetDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetUninvertedSortedSetDocValues;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link ParquetUninvertedSortedSetDocValues} over a multi-valued ord file. The streaming reader
 * handed to it fails the test if it is read, which proves each branch under test is served by the ord file alone.
 */
public class ParquetUninvertedSortedSetDocValuesTests extends OpenSearchTestCase {

    private static final String FIELD = "tags";

    /** Per-document ordinals, term count and rank all come from the ord file, with no Parquet read. */
    public void testOrdinalsAndRankComeFromTheOrdFile() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    ParquetUninvertedSortedSetDocValues values = over(ordinals, leaf.maxDoc());

                    assertEquals("blue, green, red", 3, values.getValueCount());
                    assertTrue(values.advanceExact(3));
                    assertEquals("red is kept once for doc 3", 2, values.docValueCount());
                    assertEquals("green", 1, values.nextOrd());
                    assertEquals("red", 2, values.nextOrd());
                    assertEquals("rank is a term's sorted position", 2, values.lookupTerm(new BytesRef("red")));
                }
            }
        }
    }

    /** An ordinal that is not one of the current document's resolves through the term cursor, not Parquet. */
    public void testLookupOrdForAnotherDocumentsOrdinalUsesTheTermCursor() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    ParquetUninvertedSortedSetDocValues values = over(ordinals, leaf.maxDoc());

                    assertTrue("doc 1 holds only blue", values.advanceExact(1));
                    assertEquals(new BytesRef("red"), values.lookupOrd(2));
                }
            }
        }
    }

    /** A document with no value is skipped by iteration and refused by advanceExact. */
    public void testIterationSkipsDocumentsWithoutAValue() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    ParquetUninvertedSortedSetDocValues values = over(ordinals, leaf.maxDoc());

                    assertEquals(0, values.nextDoc());
                    assertEquals(1, values.nextDoc());
                    assertEquals("doc 2 has no tags, so iteration lands on 3", 3, values.nextDoc());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.nextDoc());
                    assertFalse("doc 2 carries no value", values.advanceExact(2));
                    assertEquals(0, values.docValueCount());
                }
            }
        }
    }

    public void testAdvanceExactBeyondMaxDocReportsNoMoreDocs() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    ParquetUninvertedSortedSetDocValues values = over(ordinals, leaf.maxDoc());

                    assertFalse(values.advanceExact(4));
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.docID());
                }
            }
        }
    }

    /** Before the first document and after the last, every ordinal resolves through the term cursor. */
    public void testLookupOrdOutsideAnyDocumentUsesTheTermCursor() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    ParquetUninvertedSortedSetDocValues values = over(ordinals, leaf.maxDoc());

                    assertEquals("no document yet", new BytesRef("green"), values.lookupOrd(1));
                    assertFalse(values.advanceExact(4));
                    assertEquals("past the last document", new BytesRef("blue"), values.lookupOrd(0));
                }
            }
        }
    }

    /** A document with more ordinals than any before it grows the per-document buffer without mixing documents. */
    public void testALargerDocumentAfterASmallerOneKeepsEachDocumentsOrdinals() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    ParquetUninvertedSortedSetDocValues values = over(ordinals, leaf.maxDoc());

                    assertOrdinals(values, 1, 0);
                    assertOrdinals(values, 3, 1, 2);
                    assertOrdinals(values, 0, 0, 2);
                }
            }
        }
    }

    /** The terms enum walks the segment's words in sorted order, from the ord file. */
    public void testTermsEnumWalksTheWordsInOrder() throws Exception {
        try (Directory dir = newDirectory()) {
            indexTagsExample(dir);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf)) {
                    TermsEnum terms = over(ordinals, leaf.maxDoc()).termsEnum();
                    assertEquals(new BytesRef("blue"), terms.next());
                    assertEquals(new BytesRef("green"), terms.next());
                    assertEquals(new BytesRef("red"), terms.next());
                    assertNull(terms.next());
                }
            }
        }
    }

    private static void assertOrdinals(ParquetUninvertedSortedSetDocValues values, int doc, long... expected) throws Exception {
        assertTrue(values.advanceExact(doc));
        assertEquals(expected.length, values.docValueCount());
        for (long ordinal : expected) {
            assertEquals(ordinal, values.nextOrd());
        }
    }

    private static ParquetUninvertedSortedSetDocValues over(UninvertedOrdinals ordinals, int maxDoc) {
        return new ParquetUninvertedSortedSetDocValues(ordinals, streamingThatMustNotBeRead(maxDoc), maxDoc);
    }

    /** 6 values stored (red twice in doc 3), 3 rows with values, a list column. */
    private UninvertedOrdinals buildOrdinals(LeafReader leaf) throws Exception {
        return UninvertedOrdinals.build(
            createTempDir(),
            FIELD,
            leaf.terms(FIELD),
            leaf.maxDoc(),
            new ColumnValueCounts(6, 3, true),
            () -> false
        );
    }

    /** A streaming reader whose cursor cannot be opened, so reading Parquet would fail the test. */
    private static ParquetSortedSetDocValues streamingThatMustNotBeRead(int maxDoc) {
        return new ParquetSortedSetDocValues(
            () -> { throw new AssertionError("the ord file must answer without reading Parquet"); },
            maxDoc
        );
    }

    /** doc 0 [red, blue], doc 1 [blue], doc 2 nothing, doc 3 [red, red, green]; words 0 blue, 1 green, 2 red. */
    private static void indexTagsExample(Directory dir) throws Exception {
        String[][] docs = { { "red", "blue" }, { "blue" }, {}, { "red", "red", "green" } };
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(new LogDocMergePolicy()))) {
            for (String[] tags : docs) {
                Document document = new Document();
                document.add(new StringField("id", "x", Field.Store.NO));
                for (String tag : tags) {
                    document.add(new StringField(FIELD, tag, Field.Store.NO));
                }
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            writer.commit();
        }
    }
}
