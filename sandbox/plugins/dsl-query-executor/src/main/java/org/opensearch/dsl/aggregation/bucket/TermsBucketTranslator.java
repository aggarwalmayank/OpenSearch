/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.lucene.util.BytesRef;
import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Translates a {@link TermsAggregationBuilder} — single-field GROUP BY.
 * {@code {"aggs": {"by_brand": {"terms": {"field": "brand"}}}}} becomes {@code GROUP BY brand}.
 */
public class TermsBucketTranslator implements BucketTranslator<TermsAggregationBuilder> {

    /** Creates a terms bucket translator. */
    public TermsBucketTranslator() {}

    @Override
    public Class<TermsAggregationBuilder> getAggregationType() {
        return TermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(TermsAggregationBuilder agg) {
        return new FieldGrouping(List.of(agg.field()));
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(TermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(TermsAggregationBuilder agg) {
        return agg.order();
    }

    @Override
    public InternalAggregation toBucketAggregation(TermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> bucketList = new ArrayList<>();
        buckets.forEach(bucketList::add);

        if (bucketList.isEmpty()) {
            return buildEmptyStringTerms(agg);
        }

        Object firstKey = bucketList.get(0).keys().get(0);
        if (firstKey instanceof Long || firstKey instanceof Integer || firstKey instanceof Short) {
            return buildLongTerms(agg, bucketList);
        }
        return buildStringTerms(agg, bucketList);
    }

    private static TermsAggregator.BucketCountThresholds thresholds(TermsAggregationBuilder agg) {
        return new TermsAggregator.BucketCountThresholds(
            agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize()
        );
    }

    private StringTerms buildStringTerms(TermsAggregationBuilder agg, List<BucketEntry> buckets) {
        List<StringTerms.Bucket> termBuckets = new ArrayList<>(buckets.size());
        for (BucketEntry entry : buckets) {
            termBuckets.add(new StringTerms.Bucket(
                new BytesRef(String.valueOf(entry.keys().get(0))),
                entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW
            ));
        }
        return new StringTerms(
            agg.getName(), agg.order(), agg.order(), Map.of(), DocValueFormat.RAW,
            agg.shardSize(), false, 0, termBuckets, 0, thresholds(agg)
        );
    }

    private LongTerms buildLongTerms(TermsAggregationBuilder agg, List<BucketEntry> buckets) {
        List<LongTerms.Bucket> termBuckets = new ArrayList<>(buckets.size());
        for (BucketEntry entry : buckets) {
            termBuckets.add(new LongTerms.Bucket(
                ((Number) entry.keys().get(0)).longValue(),
                entry.docCount(), entry.subAggs(), false, 0, DocValueFormat.RAW
            ));
        }
        return new LongTerms(
            agg.getName(), agg.order(), agg.order(), Map.of(), DocValueFormat.RAW,
            agg.shardSize(), false, 0, termBuckets, 0, thresholds(agg)
        );
    }

    private StringTerms buildEmptyStringTerms(TermsAggregationBuilder agg) {
        return new StringTerms(
            agg.getName(), agg.order(), agg.order(), Map.of(), DocValueFormat.RAW,
            agg.shardSize(), false, 0, List.of(), 0, thresholds(agg)
        );
    }
}
