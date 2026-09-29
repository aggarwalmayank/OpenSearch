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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetUninvertedSortedDocValues;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ParquetDocValuesLeafReader}'s doc-values routing. The first group covers the
 * branches that decide a field's fate before any value is read, and uses a null producer deliberately:
 * reaching the producer there would surface as a {@link NullPointerException}. The second group covers
 * which tier serves a healthy field, and needs a real producer over a fixture Parquet file.
 */
public class ParquetDocValuesLeafReaderTests extends DataFusionBackedTestCase {

    private static final String CITY = "city";
    private static final String TAGS = "tags";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Another test in this JVM may have closed a plugin, which sets the ordinals cache's static
        // shuttingDown and makes every later build refuse.
        UninvertedOrdinalsCache.start();
    }

    /** Keyword is served as SORTED_SET, so the plain SORTED accessor never matches a Parquet field. */
    public void testSortedAccessorReturnsNullForAParquetField() throws Exception {
        ParquetSegmentResources resources = sortedSetResources("tags");

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());

            assertNull("SORTED accessor does not match a SORTED_SET Parquet field", parquetLeaf.getSortedDocValues("tags"));
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /**
     * Builds resources with a null producer holding one synthetic SORTED_SET {@link FieldInfo} for
     * {@code field}, the matching {@link FieldInfos}, the given multi-valued set, and a null segment info.
     */
    private static ParquetSegmentResources sortedSetResources(String field) {
        FieldInfo fi = sortedSetField(field);
        return new ParquetSegmentResources(null, Map.of(field, fi), new FieldInfos(new FieldInfo[] { fi }), null);
    }

    // ------------------------------------------------------------------------------------------------
    // Which tier serves a healthy field. These need a real producer, so the Parquet column and the
    // Lucene postings must agree: the ordinals build refuses a field whose coverage does not match.
    // ------------------------------------------------------------------------------------------------

    /** With no value in the whole segment, empty doc values ARE the correct ordinals view; nothing is built. */
    public void testZeroNonNullRowsServesEmptyOrdinals() throws Exception {
        List<String> values = Arrays.asList(null, null, null);
        Path parquetFile = createTempDir().resolve("allnull.parquet");
        StringColumnFixture.write(parquetFile, allocator, CITY, values);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedDocValues dv = cityDocValues(leaf, parquetFile, values.size());
                assertEquals("no row carries a value, so the ordinals view is empty", 0, dv.getValueCount());
            }
        }
    }

    /** A filesystem segment gets an ord file, and the ordinals tier serves the field. */
    public void testLeasedOrdFileProducesTheOrdinalsBackedIterator() throws Exception {
        List<String> values = List.of("delhi", "mumbai", "pune");
        Path parquetFile = createTempDir().resolve("cities.parquet");
        StringColumnFixture.write(parquetFile, allocator, CITY, values);

        Path shardDir = createTempDir();
        try (Directory dir = openFsIndex(shardDir, "delhi", "mumbai", "pune")) {
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedDocValues dv = cityDocValues(leaf, parquetFile, values.size());

                assertTrue("a leased ord file must serve the field", dv instanceof ParquetUninvertedSortedDocValues);
                assertEquals("three distinct cities", 3, dv.getValueCount());
                try (var listing = Files.list(shardDir.resolve("parquet-ords"))) {
                    assertTrue("the built ord file must exist under the shard", listing.anyMatch(f -> f.toString().endsWith(".ord")));
                }
            }
        }
    }

    /** No ord file can exist for a non-filesystem segment, so the streaming tier serves the field unchanged. */
    public void testNoLeaseFallsBackToTheStreamingIterator() throws Exception {
        List<String> values = List.of("delhi", "mumbai", "pune");
        Path parquetFile = createTempDir().resolve("cities-nofs.parquet");
        StringColumnFixture.write(parquetFile, allocator, CITY, values);

        try (Directory dir = new ByteBuffersDirectory()) {
            writeCities(dir, "delhi", "mumbai", "pune");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedDocValues dv = cityDocValues(leaf, parquetFile, values.size());
                assertTrue("without an ord file the streaming tier serves the field", dv instanceof ParquetSortedDocValues);
            }
        }
    }

    /** Source derivation serves a multi-valued field's list column, sorted and distinct per document. */
    public void testSourceDerivationServesAMultiValuedListColumn() throws Exception {
        Path parquetFile = createTempDir().resolve("tags-list.parquet");
        StringListColumnFixture.write(
            parquetFile,
            allocator,
            TAGS,
            Arrays.asList(List.of("red", "blue"), List.of("blue"), null, List.of("red", "red", "green"))
        );

        try (Directory dir = new ByteBuffersDirectory()) {
            writeCities(dir, "a", "b", "c", "d");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedSetDocValues values = sourceViewOfTags(leaf, parquetFile, 4);
                assertSetValues(values, 0, "blue", "red");
                assertSetValues(values, 1, "blue");
                assertFalse("a null row carries no value", values.advanceExact(2));
                assertSetValues(values, 3, "green", "red");
            }
        }
    }

    /** A plain column written before promotion still serves source derivation for a multi-valued field. */
    public void testSourceDerivationServesAPlainColumnOfAMultiValuedField() throws Exception {
        Path parquetFile = createTempDir().resolve("tags-plain.parquet");
        StringColumnFixture.write(parquetFile, allocator, TAGS, List.of("red", "blue", "green"));

        try (Directory dir = new ByteBuffersDirectory()) {
            writeCities(dir, "a", "b", "c");
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                SegmentReader leaf = (SegmentReader) reader.leaves().get(0).reader();
                SortedSetDocValues values = sourceViewOfTags(leaf, parquetFile, 3);
                assertSetValues(values, 0, "red");
                assertSetValues(values, 1, "blue");
                assertSetValues(values, 2, "green");
            }
        }
    }

    /** The multi-valued {@code tags} field as source derivation receives it, over a real producer. */
    private SortedSetDocValues sourceViewOfTags(SegmentReader leaf, Path parquetFile, int maxDoc) throws Exception {
        FieldInfo fi = sortedSetField(TAGS);
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
            parquetFile,
            ParquetColumnReader.LOCAL_STORE,
            Settings.EMPTY,
            maxDoc,
            null
        );
        ParquetSegmentResources resources = new ParquetSegmentResources(
            producer,
            Map.of(TAGS, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            leaf.getSegmentInfo().info
        );
        ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());
        return parquetLeaf.perDocumentValuesReader().getSortedSetDocValues(TAGS);
    }

    private static void assertSetValues(SortedSetDocValues values, int doc, String... expected) throws IOException {
        assertTrue("doc " + doc + " must have values", values.advanceExact(doc));
        assertEquals(expected.length, values.docValueCount());
        for (String word : expected) {
            assertEquals(new BytesRef(word), values.lookupOrd(values.nextOrd()));
        }
    }

    /** The field as an aggregation receives it: the leaf reader's SORTED_SET answer with its singleton unwrapped. */
    private SortedDocValues cityDocValues(SegmentReader leaf, Path parquetFile, int maxDoc) throws Exception {
        FieldInfo fi = sortedSetField(CITY);
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
            parquetFile,
            ParquetColumnReader.LOCAL_STORE,
            Settings.EMPTY,
            maxDoc,
            null
        );
        ParquetSegmentResources resources = new ParquetSegmentResources(
            producer,
            Map.of(CITY, fi),
            new FieldInfos(new FieldInfo[] { fi }),
            leaf.getSegmentInfo().info
        );
        ParquetDocValuesLeafReader parquetLeaf = new ParquetDocValuesLeafReader(leaf, resources, new CursorRegistry());
        return DocValues.unwrapSingleton(parquetLeaf.getSortedSetDocValues(CITY));
    }

    /** Opens a filesystem index at {@code <shardDir>/index}, so ord files resolve to {@code <shardDir>/parquet-ords}. */
    private static Directory openFsIndex(Path shardDir, String... cities) throws Exception {
        Path storeDir = shardDir.resolve("index");
        Files.createDirectories(storeDir);
        Directory directory = FSDirectory.open(storeDir);
        writeCities(directory, cities);
        return directory;
    }

    /** One document per city, carrying postings for the field the ordinals build reads. */
    private static void writeCities(Directory dir, String... cities) throws Exception {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (String city : cities) {
                Document document = new Document();
                document.add(new StringField(CITY, city, Field.Store.NO));
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            writer.commit();
        }
    }

    /** A synthetic SORTED_SET field info, matching what the resources builder synthesizes for keyword. */
    private static FieldInfo sortedSetField(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED_SET,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }

    private static IndexWriter singleDocWriter(Directory dir) throws Exception {
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        return writer;
    }
}
