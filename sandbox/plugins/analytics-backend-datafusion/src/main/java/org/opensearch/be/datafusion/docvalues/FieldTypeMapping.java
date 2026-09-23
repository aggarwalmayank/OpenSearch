/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesType;

import java.util.Locale;
import java.util.Map;

/**
 * Maps an OpenSearch field mapping type to the Lucene DocValues type the codec serves for it.
 *
 * <p>One entry per supported mapping type, naming the DV type OpenSearch's own mapper indexes for
 * that type, so a single-valued field arrives as a singleton callers recover via
 * {@code DocValues.unwrapSingleton}. Supporting a new mapping type is one entry here plus the
 * matching accessor on {@link ParquetDocValuesProducer}. The Parquet physical type is not modeled
 * here: the native cursor reads it from the Parquet schema at open time.
 */
public final class FieldTypeMapping {

    // token_count resolves through the "integer" entry (TokenCountFieldType extends NumberFieldType).

    private static final Map<String, DocValuesType> BY_TYPE = Map.ofEntries(
        Map.entry("byte", DocValuesType.SORTED_NUMERIC),
        Map.entry("short", DocValuesType.SORTED_NUMERIC),
        Map.entry("integer", DocValuesType.SORTED_NUMERIC),
        Map.entry("long", DocValuesType.SORTED_NUMERIC),
        Map.entry("float", DocValuesType.SORTED_NUMERIC),
        Map.entry("double", DocValuesType.SORTED_NUMERIC),
        Map.entry("date", DocValuesType.SORTED_NUMERIC),
        Map.entry("date_nanos", DocValuesType.SORTED_NUMERIC),
        // BooleanFieldMapper stores doc values as a SortedNumericDocValuesField holding 0 or 1, so a
        // boolean resolves like the numerics; the codec reads it bit-packed.
        Map.entry("boolean", DocValuesType.SORTED_NUMERIC),
        // Arrow UInt64. NumberType.UNSIGNED_LONG stores doc values as BigInteger.longValue(), the same
        // raw 64-bit pattern the column holds, so the bits pass through unchanged.
        Map.entry("unsigned_long", DocValuesType.SORTED_NUMERIC),
        // Written as a plain long holding the already-scaled value, which is exactly what
        // ScaledFloatFieldMapper puts in doc values; ScaledFloatLeafFieldData divides by the scaling
        // factor above this codec, so the raw scaled long is the correct thing to return.
        Map.entry("scaled_float", DocValuesType.SORTED_NUMERIC),
        // Arrow Float16, re-encoded to Lucene's sortable short by DecodedBatch.KIND_HALF_FLOAT.
        Map.entry("half_float", DocValuesType.SORTED_NUMERIC),
        // KeywordFieldMapper and IpFieldMapper index SortedSetDocValuesField, so both resolve as
        // SORTED_SET even where the column holds one value per document.
        Map.entry("keyword", DocValuesType.SORTED_SET),
        Map.entry("ip", DocValuesType.SORTED_SET)
    );

    private FieldTypeMapping() {}

    /** True if the codec has a Parquet DocValues mapping for the given OpenSearch mapping type. */
    public static boolean isSupported(String mappingType) {
        return BY_TYPE.containsKey(mappingType);
    }

    /**
     * Returns the DV type the codec serves for {@code mappingType}.
     *
     * @throws IllegalArgumentException if the mapping type has no Parquet DocValues mapping
     */
    public static DocValuesType forType(String mappingType) {
        DocValuesType dvType = BY_TYPE.get(mappingType);
        if (dvType == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Parquet DocValues codec has no mapping for OpenSearch type '%s'", mappingType)
            );
        }
        return dvType;
    }

    /**
     * Validates that the field's mapping type supports the requested Lucene DV type, throwing
     * {@link IllegalArgumentException} naming the field and mapping type when incompatible.
     */
    public static void validate(String field, String mappingType, DocValuesType requested) {
        DocValuesType supported = BY_TYPE.get(mappingType);
        if (supported == null) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "field '%s' has mapping type '%s', which the Parquet DocValues codec does not support",
                    field,
                    mappingType
                )
            );
        }
        if (requested != supported) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "field '%s' (mapping type '%s') supports DocValues type %s but %s was requested",
                    field,
                    mappingType,
                    supported,
                    requested
                )
            );
        }
    }
}
