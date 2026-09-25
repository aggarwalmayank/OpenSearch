/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.HashMap;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for the multi-valued skipper gate: {@link ParquetSegmentResourceCache#skipTypeFor}
 * stamps NONE for multi-valued (LIST) and non-skippable types and RANGE only for single-valued
 * skippable types, and {@link ParquetDocValuesProducer#getSkipper} honours that stamp by returning no
 * skipper for a NONE {@link FieldInfo}.
 */
public class ParquetDocValuesSkipGateTests extends OpenSearchTestCase {

    /**
     * A NONE-stamped FieldInfo yields no skipper even with a mapper present. The producer trusts the
     * open-time stamp and short-circuits: without the NONE gate, execution would fall through to the
     * type lookup instead of returning null here.
     */
    public void testGetSkipperReturnsNullForNoneStampedFieldEvenWithMapperPresent() throws Exception {
        // Non-null mapper so the low-level "no mapper" bypass does not fire; the NONE gate is what
        // returns null here, not the null-mapper check.
        MapperService mapperService = mock(MapperService.class);
        Path file = createTempDir().resolve("gate.parquet");
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, 0L, Settings.EMPTY, 100, mapperService);

        FieldInfo noneStamped = sortedNumericField("f", DocValuesSkipIndexType.NONE);
        assertNull("a NONE-stamped field must get no skipper", producer.getSkipper(noneStamped));
    }

    /** A single-valued, range-skippable type stamps RANGE. */
    public void testSkipTypeForSingleValuedSkippableIsRange() {
        MappedFieldType longType = new NumberFieldMapper.NumberFieldType("f", NumberType.LONG);
        assertFalse("a fresh field is single-valued (AUTO)", longType.isMultiValued());
        assertEquals(DocValuesSkipIndexType.RANGE, ParquetSegmentResourceCache.skipTypeFor(longType));
    }

    /** The same skippable type stamps NONE once it has flipped to multi-valued (LIST). */
    public void testSkipTypeForMultiValuedSkippableIsNone() {
        MappedFieldType longType = new NumberFieldMapper.NumberFieldType("f", NumberType.LONG);
        longType.setMultiValued(true);
        assertTrue(longType.isMultiValued());
        assertEquals(DocValuesSkipIndexType.NONE, ParquetSegmentResourceCache.skipTypeFor(longType));
    }

    /** A non-skippable type (double) stamps NONE regardless of multi-valued state. */
    public void testSkipTypeForNonSkippableIsNoneEvenWhenSingleValued() {
        MappedFieldType doubleType = new NumberFieldMapper.NumberFieldType("f", NumberType.DOUBLE);
        assertFalse(doubleType.isMultiValued());
        assertEquals(DocValuesSkipIndexType.NONE, ParquetSegmentResourceCache.skipTypeFor(doubleType));
    }

    /** A synthetic SORTED_NUMERIC field info with the given skip-index type, as the resources builder produces. */
    private static FieldInfo sortedNumericField(String name, DocValuesSkipIndexType skipType) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED_NUMERIC,
            skipType,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
