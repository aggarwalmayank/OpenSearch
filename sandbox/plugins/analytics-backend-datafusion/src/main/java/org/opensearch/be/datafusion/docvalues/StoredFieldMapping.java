/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import java.util.Map;

/**
 * Mapping types whose Parquet column the codec serves as a Lucene stored field, and how a row's bytes
 * reach the {@code StoredFieldVisitor}. Parquet claims {@code STORED_FIELDS} for these types, so the
 * Lucene segment holds no copy: the column is the stored field.
 */
final class StoredFieldMapping {

    /** How a row's bytes are handed to the visitor. */
    enum Kind {
        /** {@code visitor.binaryField}: the raw column bytes. */
        BINARY
    }

    private static final Map<String, Kind> MAPPINGS = Map.of("binary", Kind.BINARY);

    private StoredFieldMapping() {}

    /** The stored-field kind for a mapping type, or {@code null} when the codec does not serve it as stored. */
    static Kind forType(String mappingType) {
        return MAPPINGS.get(mappingType);
    }
}
