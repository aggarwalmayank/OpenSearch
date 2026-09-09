/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterSortedSetDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.util.function.Supplier;

/**
 * A {@code terms} aggregation without an {@code execution_hint} defaults to
 * {@code global_ordinals}, whose setup calls {@code getValueCount()}. Some doc-values
 * implementations (e.g. the pluggable Parquet data format's streaming tier) cannot materialize
 * segment-global ordinals and throw {@link UnsupportedOperationException} there. The factory must
 * fall back to {@code map} execution instead of failing the search request.
 */
public class StringTermsUnsupportedOrdinalsFallbackTests extends AggregatorTestCase {

    public void testFallsBackToMapWhenGlobalOrdinalsUnsupported() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (String value : new String[] { "delhi", "mumbai", "delhi", "pune", "mumbai", "delhi" }) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("city", new BytesRef(value)));
                    indexWriter.addDocument(document);
                }
            }
            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                MappedFieldType fieldType = new GlobalOrdinalsUnsupportedKeywordFieldType("city");
                TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("cities").userValueTypeHint(ValueType.STRING)
                    .field("city");
                // Without the factory fallback this throws UnsupportedOperationException from
                // getValueCount() during aggregator creation.
                StringTerms result = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(3, result.getBuckets().size());
                assertEquals("delhi", result.getBuckets().get(0).getKeyAsString());
                assertEquals(3, result.getBuckets().get(0).getDocCount());
                assertEquals("mumbai", result.getBuckets().get(1).getKeyAsString());
                assertEquals(2, result.getBuckets().get(1).getDocCount());
                assertEquals("pune", result.getBuckets().get(2).getKeyAsString());
                assertEquals(1, result.getBuckets().get(2).getDocCount());
            }
        }
    }

    /**
     * Keyword field type whose fielddata serves per-document ordinal access but throws
     * {@link UnsupportedOperationException} for segment-global operations, mimicking a
     * streaming-tier segment.
     */
    private static final class GlobalOrdinalsUnsupportedKeywordFieldType extends KeywordFieldMapper.KeywordFieldType {
        GlobalOrdinalsUnsupportedKeywordFieldType(String name) {
            super(name);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            IndexFieldData.Builder delegateBuilder = super.fielddataBuilder(fullyQualifiedIndexName, searchLookup);
            return (IndexFieldDataCache cache, CircuitBreakerService breakerService) -> new GlobalOrdinalsUnsupportedFieldData(
                (IndexOrdinalsFieldData) delegateBuilder.build(cache, breakerService)
            );
        }
    }

    private static final class GlobalOrdinalsUnsupportedFieldData implements IndexOrdinalsFieldData {
        private final IndexOrdinalsFieldData delegate;

        GlobalOrdinalsUnsupportedFieldData(IndexOrdinalsFieldData delegate) {
            this.delegate = delegate;
        }

        private static LeafOrdinalsFieldData wrapLeaf(LeafOrdinalsFieldData leaf) {
            return new LeafOrdinalsFieldData() {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    return new FilterSortedSetDocValues(leaf.getOrdinalsValues()) {
                        @Override
                        public long getValueCount() {
                            throw new UnsupportedOperationException("segment-global ordinals unavailable (test streaming tier)");
                        }

                        @Override
                        public long lookupTerm(BytesRef key) {
                            throw new UnsupportedOperationException("segment-global ordinals unavailable (test streaming tier)");
                        }
                    };
                }

                @Override
                public SortedBinaryDocValues getBytesValues() {
                    return org.opensearch.index.fielddata.FieldData.toString(getOrdinalsValues());
                }

                @Override
                public ScriptDocValues<?> getScriptValues() {
                    return leaf.getScriptValues();
                }

                @Override
                public long ramBytesUsed() {
                    return leaf.ramBytesUsed();
                }

                @Override
                public void close() {
                    leaf.close();
                }
            };
        }

        @Override
        public LeafOrdinalsFieldData load(LeafReaderContext context) {
            return wrapLeaf(delegate.load(context));
        }

        @Override
        public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) throws Exception {
            return wrapLeaf(delegate.loadDirect(context));
        }

        @Override
        public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
            return this;
        }

        @Override
        public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) {
            return this;
        }

        @Override
        public org.apache.lucene.index.OrdinalMap getOrdinalMap() {
            throw new UnsupportedOperationException("segment-global ordinals unavailable (test streaming tier)");
        }

        @Override
        public boolean supportsGlobalOrdinalsMapping() {
            return false;
        }

        @Override
        public String getFieldName() {
            return delegate.getFieldName();
        }

        @Override
        public org.opensearch.search.aggregations.support.ValuesSourceType getValuesSourceType() {
            return delegate.getValuesSourceType();
        }

        @Override
        public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
            return new SortField(getFieldName(), new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested), reverse);
        }

        @Override
        public BucketedSort newBucketedSort(
            BigArrays bigArrays,
            Object missingValue,
            MultiValueMode sortMode,
            XFieldComparatorSource.Nested nested,
            SortOrder sortOrder,
            DocValueFormat format,
            int bucketSize,
            BucketedSort.ExtraData extra
        ) {
            throw new UnsupportedOperationException("not needed for this test");
        }
    }
}
