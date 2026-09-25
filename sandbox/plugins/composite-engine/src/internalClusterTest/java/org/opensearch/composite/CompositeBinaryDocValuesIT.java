/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Reads a {@code binary} field on a composite (Parquet-primary) index back through every {@code _search}
 * surface and checks each answer against a vanilla Lucene index holding the same documents:
 * {@code docvalue_fields}, {@code exists}, point lookups, {@code stored_fields}, and derived {@code _source}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class CompositeBinaryDocValuesIT extends AbstractCompositeEngineIT {

    private static final String COMPOSITE = "bin_dv";
    private static final String VANILLA = "bin_dv_vanilla";
    private static final int DOCS = 40;
    private static final int ABSENT_EVERY = 5;
    private static final int PRESENT = DOCS - (DOCS + ABSENT_EVERY - 1) / ABSENT_EVERY;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (indexExists(COMPOSITE) == false) {
            Settings.Builder composite = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", List.of("lucene"));
            Settings.Builder vanilla = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            for (var index : Map.of(COMPOSITE, composite, VANILLA, vanilla).entrySet()) {
                // store=true is what derived _source requires of a binary field; on composite it costs nothing
                // extra since the stored field and the doc values are two views over the one Parquet column.
                assertAcked(
                    client().admin()
                        .indices()
                        .prepareCreate(index.getKey())
                        .setSettings(index.getValue())
                        .setMapping("n", "type=long", "blob", "type=binary,store=true,doc_values=true")
                );
            }
            ensureGreen(COMPOSITE, VANILLA);
            indexDocs();
        }
    }

    /**
     * Two refreshes, so each index holds two segments and the read path crosses a segment boundary. Ids are
     * auto-generated: a pluggable-dataformat index is append-only by default and rejects a custom {@code _id}.
     */
    private void indexDocs() {
        for (int i = 0; i < DOCS; i++) {
            for (String index : List.of(COMPOSITE, VANILLA)) {
                var request = client().prepareIndex(index).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                if (isAbsent(i)) {
                    request.setSource("n", i);
                } else {
                    request.setSource("n", i, "blob", Base64.getEncoder().encodeToString(blobAt(i)));
                }
                request.get();
            }
            if (i == DOCS / 2) {
                refresh(COMPOSITE, VANILLA);
            }
        }
        refresh(COMPOSITE, VANILLA);
    }

    private static boolean isAbsent(int i) {
        return i % ABSENT_EVERY == 0;
    }

    /** Lengths cycle through empty, short, and one that needs a two-byte length vInt (300). */
    private static byte[] blobAt(int i) {
        int length = switch (i % 4) {
            case 1 -> 300;
            case 2 -> 0;
            default -> i % 7 + 1;
        };
        byte[] value = new byte[length];
        for (int k = 0; k < length; k++) {
            value[k] = (byte) (i * 17 + k);
        }
        return value;
    }

    public void testDocValueFieldsMatchVanilla() {
        Map<Long, byte[]> composite = blobsByN(COMPOSITE);
        Map<Long, byte[]> vanilla = blobsByN(VANILLA);
        assertEquals("both indices return every doc", DOCS, composite.size());
        assertEquals(DOCS, vanilla.size());
        for (int i = 0; i < DOCS; i++) {
            byte[] expected = isAbsent(i) ? null : blobAt(i);
            assertArrayEquals("composite doc " + i, expected, composite.get((long) i));
            assertArrayEquals("vanilla doc " + i, expected, vanilla.get((long) i));
        }
    }

    public void testExistsQueryMatchesVanilla() {
        for (String index : List.of(COMPOSITE, VANILLA)) {
            assertEquals(index + ": exists", PRESENT, count(index, QueryBuilders.existsQuery("blob")));
            assertEquals(
                index + ": not exists",
                DOCS - PRESENT,
                count(index, QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("blob")))
            );
        }
    }

    public void testPointLookupsReturnTheDocsBlob() {
        for (int i : new int[] { 1, 2, 3, 5, 17 }) { // 300 bytes, empty, short, absent, short
            SearchResponse response = docValueSearch(COMPOSITE).setQuery(QueryBuilders.termQuery("n", i)).get();
            assertEquals("one hit for n=" + i, 1, response.getHits().getTotalHits().value());
            byte[] blob = docValueBlob(response.getHits().getAt(0));
            if (isAbsent(i)) {
                assertNull("absent doc " + i + " has no blob doc value", blob);
            } else {
                assertArrayEquals("doc " + i, blobAt(i), blob);
            }
        }
    }

    /**
     * {@code stored_fields} and derived {@code _source} both come from the STORED_FIELDS surface, which Parquet
     * claims for binary; the composite index must return the bytes as written, exactly as the vanilla twin does.
     */
    public void testStoredFieldsAndSourceMatchVanilla() {
        for (int i = 0; i < DOCS; i++) {
            SearchHit composite = fetchHit(COMPOSITE, i);
            SearchHit vanilla = fetchHit(VANILLA, i);
            byte[] expected = isAbsent(i) ? null : blobAt(i);

            assertArrayEquals("composite stored_fields doc " + i, expected, storedBlob(composite));
            assertArrayEquals("vanilla stored_fields doc " + i, expected, storedBlob(vanilla));

            assertArrayEquals("composite _source doc " + i, expected, sourceBlob(composite));
            assertArrayEquals("vanilla _source doc " + i, expected, sourceBlob(vanilla));
            assertEquals("composite _source n doc " + i, (long) i, ((Number) composite.getSourceAsMap().get("n")).longValue());
        }
    }

    /** A fetch that asks for nothing from the Parquet column must not fail on an index that has one. */
    public void testFetchWithoutTheBinaryFieldStillWorks() {
        SearchResponse response = client().prepareSearch(COMPOSITE)
            .setQuery(QueryBuilders.termQuery("n", 7))
            .setFetchSource("n", null)
            .get();
        assertEquals(0, response.getFailedShards());
        assertEquals(1, response.getHits().getTotalHits().value());
        assertEquals(Map.of("n", 7), response.getHits().getAt(0).getSourceAsMap());
    }

    private SearchHit fetchHit(String index, int n) {
        SearchResponse response = client().prepareSearch(index)
            .setQuery(QueryBuilders.termQuery("n", n))
            .setFetchSource(true)
            .addStoredField("blob")
            .get();
        assertEquals(index + ": no shard failures", 0, response.getFailedShards());
        assertEquals(index + ": one hit for n=" + n, 1, response.getHits().getTotalHits().value());
        return response.getHits().getAt(0);
    }

    /** BinaryFieldMapper displays a stored value as a BytesReference; accept the raw and REST renderings too. */
    private static byte[] storedBlob(SearchHit hit) {
        DocumentField field = hit.getFields().get("blob");
        if (field == null) {
            return null;
        }
        Object value = field.getValue();
        if (value instanceof BytesReference bytes) {
            return BytesReference.toBytes(bytes);
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        return Base64.getDecoder().decode((String) value);
    }

    private static byte[] sourceBlob(SearchHit hit) {
        Object value = hit.getSourceAsMap().get("blob");
        return value == null ? null : Base64.getDecoder().decode((String) value);
    }

    private Map<Long, byte[]> blobsByN(String index) {
        SearchResponse response = docValueSearch(index).setQuery(QueryBuilders.matchAllQuery()).setSize(DOCS).get();
        assertEquals(index + ": no shard failures", 0, response.getFailedShards());
        Map<Long, byte[]> byN = new HashMap<>();
        for (SearchHit hit : response.getHits()) {
            long n = hit.getFields().get("n").<Long>getValue();
            byN.put(n, docValueBlob(hit));
        }
        return byN;
    }

    /**
     * The fetch phase registers every requested doc-value field on the hit even when the doc has no value
     * (REST rendering drops the empty field), so an absent value shows up here as an empty DocumentField.
     */
    private static byte[] docValueBlob(SearchHit hit) {
        DocumentField blob = hit.getFields().get("blob");
        if (blob == null || blob.getValues().isEmpty()) {
            return null;
        }
        return Base64.getDecoder().decode(blob.<String>getValue());
    }

    private SearchRequestBuilder docValueSearch(String index) {
        return client().prepareSearch(index).setFetchSource(false).addDocValueField("n").addDocValueField("blob");
    }

    private long count(String index, QueryBuilder query) {
        SearchResponse response = client().prepareSearch(index).setSize(0).setQuery(query).get();
        assertEquals(index + ": no shard failures", 0, response.getFailedShards());
        return response.getHits().getTotalHits().value();
    }
}
