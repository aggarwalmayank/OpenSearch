/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts flat {@code Iterable<Object[]>} execution results into {@link SearchHits}.
 * Each row becomes a {@link SearchHit} with field names mapped to {@code _source}.
 */
public final class HitsResponseBuilder {

    private HitsResponseBuilder() {}

    /**
     * Builds SearchHits from an execution result.
     *
     * @param result the HITS execution result with rows and field names
     * @return the constructed SearchHits
     */
    public static SearchHits build(ExecutionResult result) {
        List<String> fieldNames = result.getFieldNames();
        List<SearchHit> hitList = new ArrayList<>();
        int docId = 0;

        for (Object[] row : result.getRows()) {
            hitList.add(buildHit(docId++, row, fieldNames));
        }

        SearchHit[] hits = hitList.toArray(new SearchHit[0]);
        return new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), Float.NaN);
    }

    private static SearchHit buildHit(int docId, Object[] row, List<String> fieldNames) {
        Map<String, Object> sourceMap = new LinkedHashMap<>();
        Map<String, DocumentField> fields = new LinkedHashMap<>();

        for (int col = 0; col < fieldNames.size(); col++) {
            Object value = col < row.length ? row[col] : null;
            if (value == null) continue;
            String name = fieldNames.get(col);
            sourceMap.put(name, value);
            fields.put(name, new DocumentField(name, List.of(value)));
        }

        SearchHit hit = new SearchHit(docId, String.valueOf(docId), fields, Map.of());
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().map(sourceMap);
            hit.sourceRef(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize _source for hit " + docId, e);
        }
        return hit;
    }
}
