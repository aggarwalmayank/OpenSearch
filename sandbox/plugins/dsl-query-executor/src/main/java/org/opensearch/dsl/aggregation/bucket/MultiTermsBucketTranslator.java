/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.FieldGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Translates a {@link MultiTermsAggregationBuilder} — multi-field GROUP BY.
 * {@code {"aggs": {"by_a_b": {"multi_terms": {"terms": [{"field":"a"},{"field":"b"}]}}}}}
 * becomes {@code GROUP BY a, b}.
 */
public class MultiTermsBucketTranslator implements BucketTranslator<MultiTermsAggregationBuilder> {

    /** Creates a multi-terms bucket translator. */
    public MultiTermsBucketTranslator() {}

    @Override
    public Class<MultiTermsAggregationBuilder> getAggregationType() {
        return MultiTermsAggregationBuilder.class;
    }

    @Override
    public GroupingInfo getGrouping(MultiTermsAggregationBuilder agg) {
        List<String> fields = new ArrayList<>();
        for (MultiTermsValuesSourceConfig config : agg.termsConfig()) {
            fields.add(config.getFieldName());
        }
        return new FieldGrouping(fields);
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(MultiTermsAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    @Override
    public BucketOrder getBucketOrder(MultiTermsAggregationBuilder agg) {
        return agg.order();
    }

    @Override
    public InternalAggregation toBucketAggregation(MultiTermsAggregationBuilder agg, Iterable<BucketEntry> buckets) {
        List<BucketEntry> bucketList = new ArrayList<>();
        buckets.forEach(bucketList::add);

        int keyCount = agg.termsConfig().size();
        List<DocValueFormat> formats = Collections.nCopies(keyCount, DocValueFormat.RAW);

        List<InternalMultiTerms.Bucket> termBuckets = new ArrayList<>(bucketList.size());
        for (BucketEntry entry : bucketList) {
            termBuckets.add(new InternalMultiTerms.Bucket(
                entry.keys(), entry.docCount(), entry.subAggs(), false, 0, formats
            ));
        }

        return new InternalMultiTerms(
            agg.getName(), agg.order(), agg.order(), Map.of(),
            agg.shardSize(), false, 0, 0, formats, termBuckets,
            new TermsAggregator.BucketCountThresholds(
                agg.minDocCount(), agg.shardMinDocCount(), agg.size(), agg.shardSize()
            )
        );
    }
}
