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
 * <h2>Current state: single-valued only</h2>
 * {@link #of(MappedFieldType)} returns {@link #SINGLE_VALUED} unconditionally. This is not an
 * approximation the read path papers over — it is enforced end to end:
 * <ol>
 *   <li><b>Write path.</b> {@code ParquetDocumentInput} rejects a second value for the same field
 *       with {@code "Cannot accept multiple values for field: [x] of type: [y]"}, so no segment
 *       reachable by this reader contains a repeated column for a user field.</li>
 *   <li><b>Ordinals.</b> {@link UninvertedOrdinals} stores exactly one ordinal slot per document
 *       and verifies coverage as {@code sum(docFreq) == nonNullRowCount}. A multi-valued field
 *       would both overwrite slots and fail that check — it refuses rather than undercounts.</li>
 * </ol>
 * The multi-valued machinery on the decode side already exists and is deliberately retained:
 * {@link org.opensearch.parquet.codec.iter.ParquetSortedSetDocValues} in repeated mode,
 * {@link org.opensearch.parquet.codec.iter.ParquetSortedNumericDocValues}, and
 * {@code DataFusionColumnReader}'s repeated batch loaders. See {@code MULTI_VALUE_TODO} below for
 * the ordered checklist.
 *
 * <h2>MULTI_VALUE_TODO — what enabling multi-value requires</h2>
 * <ol>
 *   <li>Write path: emit {@code REPEATED} Parquet columns and drop the single-value rejection.</li>
 *   <li>Mapping: expose per-field cardinality (mapping-declared, or the column's Parquet
 *       repetition level) and return {@link #MULTI_VALUED} from {@link #of(MappedFieldType)}.</li>
 *   <li>Ordinals: give {@link UninvertedOrdinals} a per-document ordinal <em>list</em> (offsets
 *       array + flat ord array, as Lucene's own {@code SortedSetDocValues} writer does) and
 *       change the coverage check to compare against total values, not documents.</li>
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
     * The cardinality of {@code fieldType}'s Parquet column.
     *
     * <p>Always {@link #SINGLE_VALUED} today — see the class javadoc for why that is safe and what
     * changing it entails. This is the only method to change when multi-value support lands.
     */
    public static ValueCardinality of(MappedFieldType fieldType) {
        // MULTI_VALUE_TODO(step 2): return MULTI_VALUED for fields whose Parquet column is
        // REPEATED. Until the write path can produce one, claiming MULTI_VALUED would only route
        // reads through the repeated decoders for columns that are not repeated.
        assert fieldType != null : "cardinality requested for a null field type";
        return SINGLE_VALUED;
    }

    /** True when this field stores at most one value per document. */
    public boolean isSingleValued() {
        return this == SINGLE_VALUED;
    }
}
