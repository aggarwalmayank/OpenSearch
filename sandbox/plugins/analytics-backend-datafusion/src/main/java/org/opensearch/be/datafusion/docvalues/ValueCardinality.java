/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.index.mapper.MappedFieldType;

/**
 * Whether a Parquet-backed field stores one value per document or many. Every single- vs
 * multi-valued branch in the read path derives from {@link #of(MappedFieldType)}.
 *
 * @opensearch.experimental
 */
public enum ValueCardinality {

    /** At most one value per document.*/
    SINGLE_VALUED,

    /** Zero or more values per document.*/
    MULTI_VALUED;

    /**
     * Decides from the field's {@code multi_value} mapping setting (upstream #22883): a field
     * marked {@code LIST} (arrays allowed) is {@link #MULTI_VALUED}; anything else is
     * {@link #SINGLE_VALUED}.
     *
     * <p>Why that is safe: the first array ever ingested permanently flips the mapping to LIST,
     * so a field not marked LIST has never stored an array. The opposite direction is merely
     * wasteful, not wrong: a LIST field's older segments that happen to hold only single values
     * are also served as multi-valued.
     */
    public static ValueCardinality of(MappedFieldType fieldType) {
        assert fieldType != null : "cardinality requested for a null field type";
        return fieldType.isMultiValued() ? MULTI_VALUED : SINGLE_VALUED;
    }

    public boolean isSingleValued() {
        return this == SINGLE_VALUED;
    }
}
