/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.index.engine.dataformat.DocumentInput;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Segment-core-scoped resources serving Parquet-resident doc values: the shared
 * {@link ParquetDocValuesProducer}, the synthetic {@link FieldInfo}s for the Parquet-only fields, and
 * the combined {@link FieldInfos} presented to readers of the segment, and which of those fields may hold
 * more than one value per document.
 *
 * <p>One instance is built per segment core by {@link ParquetSegmentResourceCache} and reused across
 * requests and across {@code openIfChanged}. The {@link #ABSENT} sentinel marks a core that serves no
 * Parquet doc values, so a negative resolve is recorded once per core rather than repeated per
 * request.
 */
final class ParquetSegmentResources {

    /** Sentinel for a segment core that serves no Parquet doc values. */
    static final ParquetSegmentResources ABSENT = new ParquetSegmentResources(null, Map.of(), null, Set.of(), null);

    final ParquetDocValuesProducer producer;
    final Map<String, FieldInfo> parquetFields;
    final FieldInfos combinedFieldInfos;
    /**
     * Parquet-resident fields whose mapping allows arrays. The synthetic FieldInfo is SORTED_SET for
     * either cardinality, so this set is the only record of which fields may hold more than one value.
     */
    final Set<String> multiValuedFields;
    /** Identifies the segment to {@link UninvertedOrdinalsCache}, which keys .ord files by segment and field. */
    final SegmentInfo segmentInfo;
    /** Parquet-resident fields served as stored fields, keyed by name; a subset of {@link #parquetFields}. */
    final Map<String, StoredFieldMapping.Kind> storedFields;

    /** Memoized result of the assertions-only row-id identity check; computed once per segment core. */
    private boolean rowIdsChecked;
    private boolean rowIdsAreIdentity;

    ParquetSegmentResources(
        ParquetDocValuesProducer producer,
        Map<String, FieldInfo> parquetFields,
        FieldInfos combinedFieldInfos,
        Set<String> multiValuedFields,
        SegmentInfo segmentInfo
    ) {
        this(producer, parquetFields, combinedFieldInfos, multiValuedFields, segmentInfo, Map.of());
    }

    ParquetSegmentResources(
        ParquetDocValuesProducer producer,
        Map<String, FieldInfo> parquetFields,
        FieldInfos combinedFieldInfos,
        Set<String> multiValuedFields,
        SegmentInfo segmentInfo,
        Map<String, StoredFieldMapping.Kind> storedFields
    ) {
        this.producer = producer;
        this.parquetFields = parquetFields;
        this.combinedFieldInfos = combinedFieldInfos;
        this.multiValuedFields = multiValuedFields;
        this.segmentInfo = segmentInfo;
        this.storedFields = storedFields;
    }

    /** Whether this core serves no Parquet doc values. */
    boolean isAbsent() {
        return this == ABSENT;
    }

    /** The synthetic {@link FieldInfo} for {@code field}, or {@code null} when the field is not Parquet-resident. */
    FieldInfo parquetFieldInfo(String field) {
        return parquetFields.get(field);
    }

    /** Whether {@code field}'s mapping allows arrays, so no single-valued read path can serve it. */
    boolean isMultiValued(String field) {
        return multiValuedFields.contains(field);
    }

    /**
     * Confirms the write path's guarantee that docId == Parquet row for this segment core. Enabled only
     * with assertions on; a mismatch means the identity read is unsafe.
     *
     * <p>Scans the whole segment, so the result is memoized per core: the property is per-segment, and
     * every doc-values request over this core would otherwise repeat the scan.
     */
    synchronized boolean assertRowIdsAreIdentity(LeafReader in) throws IOException {
        if (rowIdsChecked) {
            return rowIdsAreIdentity;
        }
        rowIdsChecked = true;
        rowIdsAreIdentity = computeRowIdsAreIdentity(in);
        return rowIdsAreIdentity;
    }

    private static boolean computeRowIdsAreIdentity(LeafReader in) throws IOException {
        SortedNumericDocValues rowId = in.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        if (rowId == null) {
            return true; // no row-id field => identity by definition
        }
        for (int docId = 0; docId < in.maxDoc(); docId++) {
            if (rowId.advanceExact(docId) == false || rowId.nextValue() != docId) {
                return false;
            }
        }
        return true;
    }
}
