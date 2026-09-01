/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class BinaryFieldMapperTests extends MapperTestCase {

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        final byte[] binaryValue = new byte[100];
        binaryValue[56] = 1;
        builder.value(binaryValue);
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "binary");
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck("doc_values", b -> b.field("doc_values", true));
        checker.registerConflictCheck("store", b -> b.field("store", true));
    }

    public void testExistsQueryDocValuesEnabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", true);
            if (randomBoolean()) {
                b.field("store", randomBoolean());
            }
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testExistsQueryStoreEnabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
            if (randomBoolean()) {
                b.field("doc_values", false);
            }
        }));
        assertExistsQuery(mapperService);
    }

    public void testExistsQueryStoreAndDocValuesDiabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", false);
            b.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
    }

    public void testDefaultMapping() throws Exception {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");

        assertThat(mapper, instanceOf(BinaryFieldMapper.class));
        assertThat(mapper.fieldType.stored(), equalTo(false));
    }

    public void testStoredValue() throws IOException {

        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", "true");
        }));

        // case 1: a simple binary value
        final byte[] binaryValue1 = new byte[100];
        binaryValue1[56] = 1;

        // case 2: a value that looks compressed: this used to fail in 1.x
        BytesStreamOutput out = new BytesStreamOutput();
        try (OutputStream compressed = CompressorRegistry.defaultCompressor().threadLocalOutputStream(out)) {
            new BytesArray(binaryValue1).writeTo(compressed);
        }
        final byte[] binaryValue2 = BytesReference.toBytes(out.bytes());
        assertTrue(CompressorRegistry.isCompressed(new BytesArray(binaryValue2)));

        for (byte[] value : Arrays.asList(binaryValue1, binaryValue2)) {
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", value)));
            BytesRef indexedValue = doc.rootDoc().getBinaryValue("field");
            assertEquals(new BytesRef(value), indexedValue);

            MappedFieldType fieldType = mapperService.fieldType("field");
            Object originalValue = fieldType.valueForDisplay(indexedValue);
            assertEquals(new BytesArray(value), originalValue);
        }
    }

    /**
     * A bare {@code {"type": "binary"}} keeps the upstream {@code doc_values: false} default on a plain
     * Lucene index. Guards the Lucene path against the pluggable data format default below.
     */
    public void testDocValuesDefaultOffOnLuceneIndex() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertFalse(mapperService.fieldType("field").hasDocValues());
    }

    /**
     * On a pluggable data format index the same bare mapping defaults doc values on, so the field can
     * back derived source without the mapping having to opt in.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDocValuesDefaultOnPluggableDataFormatIndex() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(this::minimalFieldMapping));
        assertTrue(mapperService.fieldType("field").hasDocValues());
    }

    /**
     * Defaulting, not forcing: an explicit {@code doc_values} in the mapping stays authoritative on both
     * paths, in both directions.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitDocValuesWinsOverDefault() throws IOException {
        // store: true so the mapping still passes the derived source check, which a pluggable data
        // format index runs at parse time.
        MapperService pluggableOff = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "binary").field("doc_values", false).field("store", true).endObject())
        );
        assertFalse(pluggableOff.fieldType("field").hasDocValues());

        MapperService luceneOn = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", true);
        }));
        assertTrue(luceneOn.fieldType("field").hasDocValues());
    }

    /**
     * The default is applied per index at mapping parse time, so it has to survive a mapping update:
     * {@link BinaryFieldMapper#getMergeBuilder()} builds a fresh Builder, which has to be seeded with the
     * default the mapper was originally built with rather than re-deriving it from index settings that a
     * merge does not have access to.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDocValuesDefaultSurvivesMappingUpdate() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(this::minimalFieldMapping));
        assertTrue(mapperService.fieldType("field").hasDocValues());

        merge(mapperService, mapping(b -> {
            minimalFieldMapping(b);
            b.startObject("other").field("type", "binary").endObject();
        }));
        assertTrue(mapperService.fieldType("field").hasDocValues());
        assertTrue(mapperService.fieldType("other").hasDocValues());
    }

    /**
     * Derived source is force-enabled on a pluggable data format index, so a bare binary mapping has to
     * pass {@code canDeriveSource()} at mapping parse time. Before the conditional default this threw
     * "Unable to derive source for [field] with stored and docValues disabled" and index creation failed.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testCanDeriveSourceFromDocValuesDefault() throws IOException {
        MapperService mapperService = createMapperService(pluggableSettings(), mapping(this::minimalFieldMapping));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        mapper.canDeriveSource();
        assertEquals(FieldValueType.DOC_VALUES, mapper.getDerivedFieldGenerator().getDerivedFieldPreference());
    }

    /**
     * With both doc values and store explicitly off there is nothing to rebuild the source from. Derived
     * source is force-enabled on a pluggable data format index and validated at mapping parse time, so
     * this is rejected at index creation rather than at read time.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testMappingRejectedWhenDocValuesAndStoreExplicitlyDisabled() {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(
                pluggableSettings(),
                mapping(b -> b.startObject("field").field("type", "binary").field("doc_values", false).endObject())
            )
        );
        assertThat(e.getMessage(), containsString("Unable to derive source for [field] with stored and docValues disabled"));
    }

    /** A stored field is read verbatim, so it stays the preferred source even when doc values are on. */
    public void testDerivedFieldPreferenceIsStoredWhenStored() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("store", true);
            b.field("doc_values", true);
        }));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        mapper.canDeriveSource();
        assertEquals(FieldValueType.STORED, mapper.getDerivedFieldGenerator().getDerivedFieldPreference());
    }

    /**
     * A defaulted {@code doc_values} is omitted from the serialized mapping on both paths, because
     * {@link BinaryFieldMapper#getMergeBuilder()} carries the default over and a parameter whose value
     * equals its default reads as unconfigured. What matters is that the omission round-trips: re-parsing
     * the serialized mapping on the same index re-applies the same default, which is what
     * {@code MapperService#assertSerialization} checks on every merge.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testDocValuesDefaultIsNotSerializedButRoundTrips() throws IOException {
        MapperService pluggable = createMapperService(pluggableSettings(), mapping(this::minimalFieldMapping));
        String serialized = serialize(pluggable);
        assertThat(serialized, not(containsString("doc_values")));
        // Re-parsing the serialized mapping on the same index gives the default back.
        assertTrue(createMapperService(pluggableSettings(), mapping(this::minimalFieldMapping)).fieldType("field").hasDocValues());

        // The Lucene path leaves it at the default too, so it stays omitted exactly as upstream.
        MapperService lucene = createMapperService(fieldMapping(this::minimalMapping));
        assertThat(serialize(lucene), not(containsString("doc_values")));
    }

    /**
     * An explicit {@code doc_values: false} on an index that defaults it on must survive serialization.
     * If the merge builder used the upstream {@code false} default the parameter would read as
     * unconfigured and be dropped, and re-parsing that output would silently flip the field back to on.
     */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testExplicitlyDisabledDocValuesSurvivesSerialization() throws IOException {
        MapperService mapperService = createMapperService(
            pluggableSettings(),
            mapping(b -> b.startObject("field").field("type", "binary").field("doc_values", false).field("store", true).endObject())
        );
        assertThat(serialize(mapperService), containsString("\"doc_values\":false"));
        assertFalse(mapperService.fieldType("field").hasDocValues());
    }

    /** An explicit {@code doc_values} is serialized on the Lucene path, as it was before this change. */
    public void testExplicitDocValuesIsSerialized() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", true);
        }));
        assertThat(serialize(mapperService), containsString("\"doc_values\":true"));
    }

    private String serialize(MapperService mapperService) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        mapperService.documentMapper().mapping().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder.toString();
    }

    /**
     * End to end derived source off binary doc values: parse a document through the mapper, index the
     * doc values field it produced, then rebuild the source from it. Exercises the framing written by
     * {@link BinaryFieldMapper.CustomBinaryDocValuesField#binaryValue()} against the decoding in
     * {@link BinaryDocValuesFetcher}.
     */
    public void testDerivedValueFetchingFromDocValues() throws IOException {
        byte[] value = new byte[] { 1, 2, 3, 4 };
        assertDerivedSource(
            fieldMapping(b -> {
                minimalMapping(b);
                b.field("doc_values", true);
            }),
            b -> b.field("field", value),
            "{\"field\":\"" + Base64.getEncoder().encodeToString(value) + "\"}"
        );
    }

    /**
     * Multi valued binary doc values are sorted and deduplicated on write, so the derived source holds
     * the values in unsigned byte order with duplicates dropped rather than in insertion order. Pinning
     * this because it is a real, observable difference from the stored field path.
     */
    public void testDerivedValueFetchingFromDocValuesMultiValued() throws IOException {
        byte[] high = new byte[] { (byte) 0xFF };
        byte[] low = new byte[] { 0x01 };
        assertDerivedSource(
            fieldMapping(b -> {
                minimalMapping(b);
                b.field("doc_values", true);
            }),
            b -> b.array("field", high, low, high),
            "{\"field\":["
                + "\""
                + Base64.getEncoder().encodeToString(low)
                + "\","
                + "\""
                + Base64.getEncoder().encodeToString(high)
                + "\"]}"
        );
    }

    /** An empty byte array round trips as an empty value rather than being dropped or throwing. */
    public void testDerivedValueFetchingFromDocValuesEmptyValue() throws IOException {
        assertDerivedSource(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", true);
        }), b -> b.field("field", new byte[0]), "{\"field\":\"\"}");
    }

    /** A document with no value for the field contributes nothing to the derived source. */
    public void testDerivedValueFetchingFromDocValuesMissingValue() throws IOException {
        assertDerivedSource(fieldMapping(b -> {
            minimalMapping(b);
            b.field("doc_values", true);
        }), b -> b.nullField("field"), "{}");
    }

    /**
     * Parses {@code source} through the mapping, indexes the resulting document, then derives the source
     * back off the reader and asserts it equals {@code expectedSource}.
     */
    private void assertDerivedSource(
        XContentBuilder mapping,
        CheckedConsumer<XContentBuilder, IOException> source,
        String expectedSource
    ) throws IOException {
        MapperService mapperService = createMapperService(mapping);
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        ParsedDocument doc = mapperService.documentMapper().parse(source(source));

        try (Directory directory = newDirectory()) {
            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                iw.addDocument(doc.rootDoc());
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                assertEquals(expectedSource, builder.toString());
            }
        }
    }

    private Settings pluggableSettings() {
        return Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
    }

    /** {@link #minimalMapping} wrapped in the field object, for the mapping(...) helper. */
    private void minimalFieldMapping(XContentBuilder b) throws IOException {
        b.startObject("field");
        minimalMapping(b);
        b.endObject();
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatBinaryValue() throws Exception {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "binary").field("store", true).endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        byte[] testValue = new byte[] { 1, 2, 3 };
        String base64Value = java.util.Base64.getEncoder().encodeToString(testValue);
        mapper.parse(source(b -> b.field("field", base64Value)), docInput);

        boolean found = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
        assertTrue("Expected binary field to be captured", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatBinaryNullSkipped() throws Exception {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();
        DocumentMapper mapper = createDocumentMapper(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "binary").field("store", true).endObject())
        );
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        mapper.parse(source(b -> b.nullField("field")), docInput);

        boolean found = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals("field"));
        assertFalse("Expected no binary field to be captured for null value", found);
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggablePathEquivalenceWithLucenePath() throws Exception {
        Settings pluggableSettings = Settings.builder().put(getIndexSettings()).put("index.pluggable.dataformat.enabled", true).build();

        // Scenario 1: binary value — both paths produce a field
        byte[] testValue = new byte[] { 1, 2, 3 };
        String base64Value = java.util.Base64.getEncoder().encodeToString(testValue);
        assertBinaryLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "binary").field("store", true).endObject()),
            b -> b.field("field", base64Value),
            "field",
            true
        );

        // Scenario 2: null value — no field produced
        assertBinaryLuceneAndPluggablePathsEquivalent(
            pluggableSettings,
            mapping(b -> b.startObject("field").field("type", "binary").field("store", true).endObject()),
            b -> b.nullField("field"),
            "field",
            false
        );
    }

    private void assertBinaryLuceneAndPluggablePathsEquivalent(
        Settings pluggableSettings,
        XContentBuilder mappingBuilder,
        CheckedConsumer<XContentBuilder, IOException> sourceBuilder,
        String fieldName,
        boolean expectField
    ) throws IOException {
        // Lucene path
        DocumentMapper luceneMapper = createDocumentMapper(mappingBuilder);
        ParsedDocument luceneDoc = luceneMapper.parse(source(sourceBuilder));
        IndexableField[] luceneFields = luceneDoc.rootDoc().getFields(fieldName);

        // Pluggable path
        DocumentMapper pluggableMapper = createDocumentMapper(pluggableSettings, mappingBuilder);
        CapturingDocumentInput docInput = new CapturingDocumentInput();
        pluggableMapper.parse(source(sourceBuilder), docInput);

        boolean pluggableHasField = docInput.getCapturedFields().stream().anyMatch(e -> e.getKey().name().equals(fieldName));

        if (!expectField) {
            assertEquals("Lucene path should produce no field for '" + fieldName + "'", 0, luceneFields.length);
            assertFalse("Pluggable path should produce no field for '" + fieldName + "'", pluggableHasField);
        } else {
            assertTrue("Lucene path should produce field '" + fieldName + "'", luceneFields.length > 0);
            assertTrue("Pluggable path should capture field '" + fieldName + "'", pluggableHasField);
        }
    }
}
