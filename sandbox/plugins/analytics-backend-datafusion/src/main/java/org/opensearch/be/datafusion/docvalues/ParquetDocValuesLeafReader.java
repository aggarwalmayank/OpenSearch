/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IOContext;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetUninvertedSortedDocValues;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.PerDocumentValuesProvider;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SequentialStoredFieldsLeafReader} that serves numeric and keyword/ip doc values for
 * Parquet-resident fields from a {@link ParquetDocValuesProducer}, delegating everything else to the
 * underlying leaf.
 *
 * <p>A Parquet-only field has no {@link FieldInfo} in the Lucene segment, so Lucene's
 * {@code PerFieldDocValuesFormat} cannot route to it. This reader closes that gap by synthesizing a
 * {@code FieldInfo} (with the DV type from {@link FieldTypeMapping}) for every mapped, codec-supported
 * field the delegate serves no doc values for, and overriding the DV accessors to serve those fields
 * from a per-segment producer. All other fields pass through unchanged.
 *
 * <p>It extends {@link SequentialStoredFieldsLeafReader} (not plain {@code FilterLeafReader}) so the
 * fetch phase can still retrieve stored fields: the derived-source layer above unwraps to this reader,
 * which passes the underlying segment's stored-fields reader straight through.
 *
 * <p>One producer is built lazily per segment and closed when this reader closes; a wrapper retained
 * by a cache past its request is rerouted to the segment-lifetime shared producer (see
 * {@link SharedProducerRegistry}).
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader implements PerDocumentValuesProvider {

    private final MapperService mapperService;
    private final SegmentReadState segmentReadState;
    private final Map<String, FieldInfo> parquetFields;
    private final FieldInfos combinedFieldInfos;

    private ParquetDocValuesProducer producer;
    private boolean producerInitialized;

    /** Memoized result of the assertions-only row-id identity check; see {@link #assertRowIdsAreIdentity}. */
    private boolean rowIdsChecked;
    private boolean rowIdsAreIdentity;

    private ParquetDocValuesLeafReader(
        LeafReader in,
        MapperService mapperService,
        SegmentReadState segmentReadState,
        Map<String, FieldInfo> parquetFields,
        FieldInfos combinedFieldInfos
    ) {
        super(in);
        this.mapperService = mapperService;
        this.segmentReadState = segmentReadState;
        this.parquetFields = parquetFields;
        this.combinedFieldInfos = combinedFieldInfos;
    }

    /**
     * Wraps {@code in} if a Parquet file resolves for its segment and the mapping declares at least one
     * codec-supported field the Lucene segment does not know about. Otherwise returns {@code in}.
     */
    public static LeafReader wrapIfApplicable(LeafReader in, MapperService mapperService) throws IOException {
        SegmentReader segmentReader;
        try {
            segmentReader = Lucene.segmentReader(in);
        } catch (RuntimeException e) {
            // Not a segment-backed leaf (e.g. an in-memory test reader) - nothing to wrap.
            return in;
        }

        SegmentReadState state = new SegmentReadState(
            segmentReader.directory(),
            segmentReader.getSegmentInfo().info,
            segmentReader.getFieldInfos(),
            IOContext.DEFAULT
        );

        if (ParquetSegmentLayout.resolve(state) == null) {
            return in;
        }

        FieldInfos existing = in.getFieldInfos();
        Map<String, FieldInfo> parquetFields = new LinkedHashMap<>();
        List<FieldInfo> combined = new ArrayList<>();
        int maxNumber = -1;
        for (FieldInfo fi : existing) {
            combined.add(fi);
            maxNumber = Math.max(maxNumber, fi.number);
        }

        // Synthesize a FieldInfo carrying the mapped DV type for each codec-supported field the Lucene
        // segment serves no doc values for; a DV-less FieldInfo is promoted (see below).
        for (MappedFieldType mft : mapperService.fieldTypes()) {
            String name = mft.name();
            if (mapperService.isMetadataField(name)) {
                continue;
            }
            if (FieldTypeMapping.isSupported(mft.typeName()) == false) {
                continue;
            }
            FieldInfo realFi = existing.fieldInfo(name);
            if (realFi != null && realFi.getDocValuesType() != DocValuesType.NONE) {
                continue;
            }
            FieldTypeMapping.Mapping mapping = FieldTypeMapping.forType(mft.typeName());
            ValueCardinality cardinality = ValueCardinality.of(mft);
            // LIST-promoted fields synthesize SORTED_SET so the segment-ordinals gate (SORTED-only)
            // turns them away; single-valued keeps SORTED.
            DocValuesType dvType = cardinality.isSingleValued() ? mapping.singleValued() : mapping.multiValued();
            FieldInfo synthetic = newDocValuesFieldInfo(name, ++maxNumber, dvType);
            parquetFields.put(name, synthetic);
            // Composite keyword/ip carry a real FieldInfo (term postings) with docValues NONE: replace
            // it with the synthetic one. Postings still serve through the delegate reader.
            if (realFi != null) {
                combined.removeIf(fi -> fi.name.equals(name));
            }
            combined.add(synthetic);
        }

        if (parquetFields.isEmpty()) {
            return in;
        }

        FieldInfos combinedFieldInfos = new FieldInfos(combined.toArray(new FieldInfo[0]));
        return new ParquetDocValuesLeafReader(in, mapperService, state, parquetFields, combinedFieldInfos);
    }

    /** Builds a synthetic doc-values {@link FieldInfo}. Skip index is NONE: the codec serves no skipper. */
    private static FieldInfo newDocValuesFieldInfo(String name, int number, DocValuesType dvType) {
        return new FieldInfo(
            name,
            number,
            false,                       // storeTermVector
            true,                        // omitNorms
            false,                       // storePayloads
            IndexOptions.NONE,           // not indexed via this reader
            dvType,
            DocValuesSkipIndexType.NONE,
            -1,                          // dvGen
            new HashMap<>(),             // attributes (mutable, per FieldInfo contract)
            0,                           // pointDimensionCount
            0,                           // pointIndexDimensionCount
            0,                           // pointNumBytes
            0,                           // vectorDimension
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,                       // softDeletes
            false                        // isParentField
        );
    }

    private synchronized ParquetDocValuesProducer producer() throws IOException {
        if (producerInitialized == false) {
            producer = new ParquetDocValuesProducer(segmentReadState, mapperService);
            producerInitialized = true;
        }
        if (producer != null && producer.isClosed()) {
            // A cache (fielddata, global ordinals) retained this wrapper past its request and is
            // calling back after the request producer closed: serve through the segment-lifetime
            // shared producer instead.
            ParquetDocValuesProducer shared = SharedProducerRegistry.get(in.getCoreCacheHelper(), segmentReadState, mapperService);
            if (shared == null) {
                throw new IllegalStateException("doc values requested after the search closed and the segment has no core cache identity");
            }
            return shared;
        }
        return producer;
    }

    private FieldInfo parquetFieldInfo(String field) {
        return parquetFields.get(field);
    }

    /**
     * Confirms the write path's guarantee that docId == Parquet row for this segment. Enabled only
     * with assertions on; a mismatch would mean the identity read is unsafe.
     *
     * <p>Scans the whole segment, so the result is memoized: the property is per-segment, and every
     * doc-values request on this leaf would otherwise repeat the scan.
     */
    private synchronized boolean assertRowIdsAreIdentity() throws IOException {
        if (rowIdsChecked) {
            return rowIdsAreIdentity;
        }
        rowIdsChecked = true;
        rowIdsAreIdentity = computeRowIdsAreIdentity();
        return rowIdsAreIdentity;
    }

    private boolean computeRowIdsAreIdentity() throws IOException {
        SortedNumericDocValues rowId = in.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
        if (rowId == null) {
            return true; // no row-id field => identity by definition
        }
        for (int docId = 0; docId < maxDoc(); docId++) {
            if (rowId.advanceExact(docId) == false || rowId.nextValue() != docId) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return combinedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.NUMERIC) {
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return producer().getNumeric(fi);
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.NUMERIC) {
            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...). The producer serves this as a singleton over
            // the single-valued numeric iterator (docId == Parquet row, asserted above).
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return producer().getSortedNumeric(fi);
        }
        return in.getSortedNumericDocValues(field);
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return sortedDocValues(field, true);
    }

    private SortedDocValues sortedDocValues(String field, boolean buildOrdinals) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.SORTED) {
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            SortedDocValues plain = producer().getSorted(fi);
            return buildOrdinals ? withSegmentOrdinals(field, plain) : plain;
        }
        return in.getSortedDocValues(field);
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return sortedSetDocValues(field, true);
    }

    private SortedSetDocValues sortedSetDocValues(String field, boolean buildOrdinals) throws IOException {
        FieldInfo fi = parquetFieldInfo(field);
        if (fi != null && fi.getDocValuesType() == DocValuesType.SORTED) {
            // OpenSearch keyword/ip accessors request SORTED_SET even for single-valued fields;
            // serve the single-valued iterator singleton-wrapped, mirroring the numeric path.
            assert assertRowIdsAreIdentity() : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            SortedDocValues plain = producer().getSorted(fi);
            SortedDocValues sorted = buildOrdinals ? withSegmentOrdinals(field, plain) : plain;
            return DocValues.singleton(sorted);
        }
        return in.getSortedSetDocValues(field);
    }

    /**
     * Eligible for segment ordinals only when the synthetic FieldInfo is {@code SORTED} — keyword/ip,
     * whose sidecar terms are byte-identical to the Parquet values, so ranking terms == ranking values.
     */
    private boolean supportsSegmentOrdinals(String field) {
        FieldInfo fi = parquetFieldInfo(field);
        return fi != null && fi.getDocValuesType() == DocValuesType.SORTED;
    }

    /**
     * Layers cached segment ordinals over the streaming Parquet reader when available; falls back to
     * the streaming reader unchanged when no lease can be acquired.
     */
    private SortedDocValues withSegmentOrdinals(String field, SortedDocValues sorted) throws IOException {
        if (supportsSegmentOrdinals(field) == false) {
            return sorted;
        }
        if (sorted instanceof ParquetSortedDocValues streaming) {
            long expectedNonNull = producer().nonNullRowCount(parquetFieldInfo(field));
            if (expectedNonNull == 0) {
                // No document in this segment has a value: empty doc values ARE the correct ordinals
                // view (zero terms) — nothing to uninvert, and the streaming tier would fail the
                // aggregation at getValueCount.
                return DocValues.emptySorted();
            }
            UninvertedOrdinalsCache.Lease lease = UninvertedOrdinalsCache.acquire(in, segmentReadState.segmentInfo, field, expectedNonNull);
            if (lease != null) {
                ParquetUninvertedSortedDocValues withOrdinals = new ParquetUninvertedSortedDocValues(lease.ordinals(), streaming, maxDoc());
                UninvertedOrdinalsCache.releaseWhenUnreachable(withOrdinals, lease);
                return withOrdinals;
            }
        }
        return sorted;
    }

    @Override
    public LeafReader perDocumentValuesReader() {
        return new FilterLeafReader(this) {
            @Override
            public SortedDocValues getSortedDocValues(String field) throws IOException {
                return sortedDocValues(field, false);
            }

            @Override
            public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
                return sortedSetDocValues(field, false);
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return ParquetDocValuesLeafReader.this.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return ParquetDocValuesLeafReader.this.getReaderCacheHelper();
            }
        };
    }

    @Override
    protected void doClose() throws IOException {
        closeParquetResources();
        super.doClose();
    }

    /**
     * Releases the producer owned by this wrapper without closing the underlying Lucene leaf. The
     * request-scoped directory reader calls this explicitly before closing its non-closing delegate.
     */
    synchronized void closeParquetResources() throws IOException {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        // This reader overlays doc values only; the underlying segment holds the real stored fields.
        return reader;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
