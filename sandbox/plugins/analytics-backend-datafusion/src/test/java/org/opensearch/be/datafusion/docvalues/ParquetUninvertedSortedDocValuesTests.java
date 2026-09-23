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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetUninvertedSortedDocValues;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link ParquetUninvertedSortedDocValues}, the tier that answers from a built ord file.
 * The streaming reader handed to it here fails the test if it is read, which proves each branch under
 * test is served by the ord file alone and never falls through to Parquet.
 */
public class ParquetUninvertedSortedDocValuesTests extends OpenSearchTestCase {

    private static final String FIELD = "city";

    /** Ordinal, term count and rank all come from the ord file, with no Parquet read. */
    public void testOrdinalsAndRankComeFromTheOrdFile() throws Exception {
        try (Directory dir = newDirectory()) {
            indexCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf, 3)) {
                    ParquetUninvertedSortedDocValues values = new ParquetUninvertedSortedDocValues(
                        ordinals,
                        streamingThatMustNotBeRead(leaf.maxDoc()),
                        leaf.maxDoc()
                    );

                    assertEquals("three distinct cities", 3, values.getValueCount());
                    assertTrue(values.advanceExact(1));
                    assertEquals("mumbai is the second term in sorted order", 1, values.ordValue());
                    assertEquals("rank is a term's sorted position", 2, values.lookupTerm(new BytesRef("pune")));
                }
            }
        }
    }

    /** An ordinal belonging to another document resolves through the term cursor, not the current document's page. */
    public void testLookupOrdForAnotherOrdinalUsesTheTermCursor() throws Exception {
        try (Directory dir = newDirectory()) {
            indexCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf, 3)) {
                    ParquetUninvertedSortedDocValues values = new ParquetUninvertedSortedDocValues(
                        ordinals,
                        streamingThatMustNotBeRead(leaf.maxDoc()),
                        leaf.maxDoc()
                    );

                    assertTrue(values.advanceExact(0));
                    assertEquals(new BytesRef("pune"), values.lookupOrd(2));
                }
            }
        }
    }

    public void testAdvanceExactBeyondMaxDocReportsNoMoreDocs() throws Exception {
        try (Directory dir = newDirectory()) {
            indexCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf, 3)) {
                    ParquetUninvertedSortedDocValues values = new ParquetUninvertedSortedDocValues(
                        ordinals,
                        streamingThatMustNotBeRead(leaf.maxDoc()),
                        leaf.maxDoc()
                    );

                    assertFalse(values.advanceExact(3));
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.docID());
                }
            }
        }
    }

    /** A document that carries no value for the field is skipped by iteration and refused by advanceExact. */
    public void testIterationSkipsDocumentsWithoutAValue() throws Exception {
        try (Directory dir = newDirectory()) {
            indexCities(dir, "delhi", null, "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = reader.leaves().get(0).reader();
                try (UninvertedOrdinals ordinals = buildOrdinals(leaf, 2)) {
                    ParquetUninvertedSortedDocValues values = new ParquetUninvertedSortedDocValues(
                        ordinals,
                        streamingThatMustNotBeRead(leaf.maxDoc()),
                        leaf.maxDoc()
                    );

                    assertEquals(0, values.nextDoc());
                    assertEquals("document 1 has no city, so iteration lands on 2", 2, values.nextDoc());
                    assertEquals(DocIdSetIterator.NO_MORE_DOCS, values.nextDoc());
                    assertFalse("document 1 carries no value", values.advanceExact(1));
                }
            }
        }
    }

    private UninvertedOrdinals buildOrdinals(LeafReader leaf, long expectedNonNullDocs) throws Exception {
        return UninvertedOrdinals.build(createTempDir(), "cities", leaf.terms(FIELD), leaf.maxDoc(), expectedNonNullDocs, () -> false);
    }

    /** A streaming reader whose cursor cannot be opened, so reading Parquet would fail the test. */
    private static ParquetSortedDocValues streamingThatMustNotBeRead(int maxDoc) {
        return new ParquetSortedDocValues(() -> { throw new AssertionError("the ord file must answer without reading Parquet"); }, maxDoc);
    }

    /** Indexes one document per entry; a null entry documents a row that carries no value for the field. */
    private static void indexCities(Directory dir, String... cities) throws Exception {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (String city : cities) {
                Document document = new Document();
                document.add(new StringField("id", "x", Field.Store.NO));
                if (city != null) {
                    document.add(new StringField(FIELD, city, Field.Store.NO));
                }
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            writer.commit();
        }
    }
}
