/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.opensearch.index.mapper.MappedFieldType;

/**
 * Whether a Parquet-backed field stores one value per document or many — the <b>single</b> place
 * the codec's read path makes that decision.
 *
 * <h2>Why this exists</h2>
 * Every read-path behaviour that differs between single- and multi-valued fields is derived from
 * {@link #of(MappedFieldType)}: which Lucene {@code DocValuesType} the synthetic
 * {@link org.apache.lucene.index.FieldInfo} declares, whether the Parquet column reader is opened
 * in repeated mode, whether a {@code DocValuesSkipper} may be exposed, and which iterator serves
 * the field. Concentrating it here means enabling multi-valued fields is a change to
 * {@link #of(MappedFieldType)} plus the already-written {@code MULTI_VALUED} arms at each call
 * site — not an audit of the whole codec.
 *
 * <h2>Current state: mapping-level detection, streaming multi-value</h2>
 * {@link #of(MappedFieldType)} reads the keyword {@code multi_value} mapping state (adaptive
 * keyword promotion, upstream #22883). {@code MULTI_VALUED} fields are served through the
 * retained repeated decoders — {@link org.opensearch.parquet.codec.iter.ParquetSortedSetDocValues}
 * in repeated mode, {@link org.opensearch.parquet.codec.iter.ParquetSortedNumericDocValues}, and
 * {@code DataFusionColumnReader}'s repeated batch loaders — with <em>no uninverted ordinals</em>:
 * {@link UninvertedOrdinals} stores one slot per document, so multi-valued fields are excluded
 * from the ordinals tier and aggregations on them run in {@code map} execution (the terms
 * aggregator falls back automatically when segment-global ordinals are unavailable). Two
 * backstops make a LIST column on the single-valued path impossible to serve silently wrong:
 * this gate, and the ordinals coverage check ({@code sum(docFreq) == nonNullRowCount}), which a
 * multi-valued segment always fails.
 *
 * <h2>MULTI_VALUE_TODO — remaining for full multi-value ordinal support</h2>
 * <ol>
 *   <li>Per-segment cardinality: expose the segment's physical column shape (scalar vs LIST)
 *       through the bridge so promoted fields' pre-promotion scalar segments keep the
 *       single-valued path (today the mapping-level answer routes them through the repeated arm;
 *       mixed-generation search is unsupported upstream too).</li>
 *   <li>Ordinals: give {@link UninvertedOrdinals} a per-document ordinal <em>list</em> (offsets
 *       array + flat ord array, as Lucene's own {@code SortedSetDocValues} writer does) and
 *       change the coverage check to compare against total values, not documents — this upgrades
 *       multi-valued aggregations from {@code map} execution to real ordinals.</li>
 *   <li>Nothing else: the call sites in {@link ParquetDocValuesLeafReader} and
 *       {@link ParquetDocValuesProducer} already branch on this enum.</li>
 * </ol>
 *
 * @opensearch.experimental
 */
public enum ValueCardinality {

    /** At most one value per document. Served by {@code NUMERIC} / {@code SORTED} iterators. */
    SINGLE_VALUED,

    /** Zero or more values per document. Served by {@code SORTED_NUMERIC} / {@code SORTED_SET}. */
    MULTI_VALUED;

    /**
     * The cardinality of {@code fieldType}'s Parquet column, from the keyword {@code multi_value}
     * mapping state (adaptive keyword promotion, upstream #22883):
     *
     * <ul>
     *   <li>{@code SCALAR} ({@code "multi_value": false}) — locked: ingest rejects arrays, every
     *       segment is provably scalar → {@link #SINGLE_VALUED}.</li>
     *   <li>{@code AUTO} (parameter omitted) — the field has <em>never promoted</em>: promotion
     *       publishes a one-way mapping update to {@code LIST}, so a field still in {@code AUTO}
     *       has only scalar segments → {@link #SINGLE_VALUED}.</li>
     *   <li>{@code LIST} — arrays are (or became) allowed → {@link #MULTI_VALUED}: serve through
     *       the repeated decoders and never build single-slot ordinals.</li>
     * </ul>
     *
     * <p><b>Known coarseness on promoted fields:</b> a field that promoted {@code AUTO → LIST}
     * mid-life still has scalar columns in its pre-promotion segments, which this mapping-level
     * answer routes through the repeated arm. Refining that requires a per-segment check of the
     * Parquet footer's column shape, which the Java side has no bridge API for yet; note that
     * search over such mixed scalar/LIST generations is explicitly unsupported by upstream #22883
     * as well. The safety property this method does guarantee: a LIST column is never served
     * through the single-valued path, so silently wrong scalar reads and corrupt single-slot
     * ordinals are impossible.
     */
    public static ValueCardinality of(MappedFieldType fieldType) {
        assert fieldType != null : "cardinality requested for a null field type";
        return fieldType.multiValueState() == MappedFieldType.MultiValueState.LIST ? MULTI_VALUED : SINGLE_VALUED;
    }

    /** True when this field stores at most one value per document. */
    public boolean isSingleValued() {
        return this == SINGLE_VALUED;
    }
}
