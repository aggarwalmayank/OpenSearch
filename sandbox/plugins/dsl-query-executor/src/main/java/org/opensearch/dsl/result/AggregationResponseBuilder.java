/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts flat {@code Object[][]} execution results from multiple granularity levels
 * into a single nested {@link InternalAggregations} matching the original DSL aggregation tree.
 *
 * Each granularity level (distinct GROUP BY key set) produces a separate {@link ExecutionResult}.
 * This builder walks the original aggregation tree, correlates each node to the correct
 * granularity's result, and reconstructs the nested bucket structure.
 */
public final class AggregationResponseBuilder {

    private final AggregationRegistry registry;
    private final Map<String, ExecutionResult> granularityMap;

    /**
     * Creates a builder from aggregation results.
     *
     * @param registry   the aggregation registry for finding handlers
     * @param allResults all execution results (this builder filters for AGGREGATION type)
     */
    public AggregationResponseBuilder(AggregationRegistry registry, List<ExecutionResult> allResults) {
        this.registry = registry;
        this.granularityMap = new HashMap<>();
        for (ExecutionResult result : allResults) {
            if (result.getType() == QueryPlans.Type.AGGREGATION) {
                String key = granularityKey(result);
                granularityMap.put(key, result);
            }
        }
    }

    /**
     * Builds the merged InternalAggregations from the original aggregation tree.
     *
     * @param originalAggs the top-level aggregation builders from SearchSourceBuilder
     * @return the nested InternalAggregations, or null if no aggregation results
     */
    public InternalAggregations build(Collection<AggregationBuilder> originalAggs) {
        if (granularityMap.isEmpty()) {
            return null;
        }
        List<InternalAggregation> aggs = buildLevel(originalAggs, new ArrayList<>(), Map.of());
        return InternalAggregations.from(aggs);
    }

    @SuppressWarnings("unchecked")
    private List<InternalAggregation> buildLevel(
            Collection<AggregationBuilder> aggs,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) {

        List<InternalAggregation> result = new ArrayList<>();

        for (AggregationBuilder agg : aggs) {
            AggregationTranslator<?> translator = registry.get(agg.getClass());
            if (translator == null) continue;

            if (translator instanceof MetricTranslator) {
                result.add(buildMetric((MetricTranslator<AggregationBuilder>) translator, agg,
                    accumulatedGroupFields, parentKeyFilter));
            } else if (translator instanceof BucketTranslator) {
                result.add(buildBucket((BucketTranslator<AggregationBuilder>) translator, agg,
                    accumulatedGroupFields, parentKeyFilter));
            }
        }
        return result;
    }

    private InternalAggregation buildMetric(
            MetricTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) {

        String granularityKey = String.join(",", accumulatedGroupFields);
        ExecutionResult result = granularityMap.get(granularityKey);
        if (result == null) {
            return translator.toInternalAggregation(agg.getName(), null);
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        String metricFieldName = agg.getName();
        Integer colIdx = colIndex.get(metricFieldName);
        if (colIdx == null) {
            return translator.toInternalAggregation(agg.getName(), null);
        }

        if (accumulatedGroupFields.isEmpty()) {
            Object[] firstRow = firstRow(result);
            Object value = firstRow != null ? firstRow[colIdx] : null;
            return translator.toInternalAggregation(agg.getName(), value);
        }

        Object[] matchingRow = findMatchingRow(result, colIndex, parentKeyFilter);
        Object value = matchingRow != null ? matchingRow[colIdx] : null;
        return translator.toInternalAggregation(agg.getName(), value);
    }

    private InternalAggregation buildBucket(
            BucketTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) {

        List<String> bucketFieldNames = translator.getGrouping(agg).getFieldNames();
        List<String> newAccumulatedFields = new ArrayList<>(accumulatedGroupFields);
        newAccumulatedFields.addAll(bucketFieldNames);

        String granularityKey = String.join(",", newAccumulatedFields);
        ExecutionResult result = granularityMap.get(granularityKey);
        if (result == null) {
            return translator.toBucketAggregation(agg, List.of());
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        List<Object[]> filteredRows = filterRows(result, colIndex, parentKeyFilter);
        Map<List<Object>, List<Object[]>> groups = groupByKeys(filteredRows, colIndex, bucketFieldNames);

        Integer countCol = colIndex.get("_count");
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(agg);

        List<BucketEntry> bucketEntries = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Object[]>> group : groups.entrySet()) {
            List<Object> keys = group.getKey();
            List<Object[]> groupRows = group.getValue();

            long docCount = 1;
            if (countCol != null && !groupRows.isEmpty()) {
                Object countVal = groupRows.get(0)[countCol];
                if (countVal instanceof Number) {
                    docCount = ((Number) countVal).longValue();
                }
            }

            Map<String, Object> childKeyFilter = new HashMap<>(parentKeyFilter);
            for (int i = 0; i < bucketFieldNames.size(); i++) {
                childKeyFilter.put(bucketFieldNames.get(i), keys.get(i));
            }

            InternalAggregations subAggResults;
            if (subAggs != null && !subAggs.isEmpty()) {
                List<InternalAggregation> subAggList = buildLevel(subAggs, newAccumulatedFields, childKeyFilter);
                subAggResults = InternalAggregations.from(subAggList);
            } else {
                subAggResults = InternalAggregations.EMPTY;
            }

            bucketEntries.add(new BucketEntry(keys, docCount, subAggResults));
        }

        return translator.toBucketAggregation(agg, bucketEntries);
    }

    private static Map<String, Integer> buildColumnIndex(ExecutionResult result) {
        Map<String, Integer> index = new HashMap<>();
        List<String> fieldNames = result.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            index.put(fieldNames.get(i), i);
        }
        return index;
    }

    private static Object[] firstRow(ExecutionResult result) {
        for (Object[] row : result.getRows()) {
            return row;
        }
        return null;
    }

    private static Object[] findMatchingRow(ExecutionResult result, Map<String, Integer> colIndex,
            Map<String, Object> keyFilter) {
        for (Object[] row : result.getRows()) {
            if (rowMatchesFilter(row, colIndex, keyFilter)) {
                return row;
            }
        }
        return null;
    }

    private static List<Object[]> filterRows(ExecutionResult result, Map<String, Integer> colIndex,
            Map<String, Object> keyFilter) {
        if (keyFilter.isEmpty()) {
            List<Object[]> all = new ArrayList<>();
            for (Object[] row : result.getRows()) {
                all.add(row);
            }
            return all;
        }
        List<Object[]> filtered = new ArrayList<>();
        for (Object[] row : result.getRows()) {
            if (rowMatchesFilter(row, colIndex, keyFilter)) {
                filtered.add(row);
            }
        }
        return filtered;
    }

    private static boolean rowMatchesFilter(Object[] row, Map<String, Integer> colIndex,
            Map<String, Object> keyFilter) {
        for (Map.Entry<String, Object> entry : keyFilter.entrySet()) {
            Integer col = colIndex.get(entry.getKey());
            if (col == null) return false;
            Object rowVal = row[col];
            Object filterVal = entry.getValue();
            if (!valuesEqual(rowVal, filterVal)) return false;
        }
        return true;
    }

    private static boolean valuesEqual(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        return a.equals(b);
    }

    private static Map<List<Object>, List<Object[]>> groupByKeys(
            List<Object[]> rows, Map<String, Integer> colIndex, List<String> keyFieldNames) {
        Map<List<Object>, List<Object[]>> groups = new LinkedHashMap<>();
        for (Object[] row : rows) {
            List<Object> key = new ArrayList<>(keyFieldNames.size());
            for (String fieldName : keyFieldNames) {
                Integer col = colIndex.get(fieldName);
                key.add(col != null ? row[col] : null);
            }
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return groups;
    }

    private static String granularityKey(ExecutionResult result) {
        if (result.getAggregationMetadata() == null) return "";
        List<String> groupByFields = result.getAggregationMetadata().getGroupByFieldNames();
        if (groupByFields.isEmpty()) return "";
        return String.join(",", groupByFields);
    }
}
