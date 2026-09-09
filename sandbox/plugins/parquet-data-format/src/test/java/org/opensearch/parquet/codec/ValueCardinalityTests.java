/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link ValueCardinality#of} must reflect the keyword {@code multi_value} mapping state
 * (adaptive keyword promotion, upstream #22883): only {@code LIST} is multi-valued. {@code AUTO}
 * is single-valued because promotion is a one-way flip to {@code LIST} — a field still in
 * {@code AUTO} has never accepted an array, so every segment is scalar.
 */
public class ValueCardinalityTests extends OpenSearchTestCase {

    public void testScalarLockedIsSingleValued() {
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("city");
        fieldType.setMultiValueState(MappedFieldType.MultiValueState.SCALAR);
        assertEquals(ValueCardinality.SINGLE_VALUED, ValueCardinality.of(fieldType));
        assertTrue(ValueCardinality.of(fieldType).isSingleValued());
    }

    public void testAutoNeverPromotedIsSingleValued() {
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("city");
        fieldType.setMultiValueState(MappedFieldType.MultiValueState.AUTO);
        assertEquals(ValueCardinality.SINGLE_VALUED, ValueCardinality.of(fieldType));
    }

    public void testListIsMultiValued() {
        KeywordFieldMapper.KeywordFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("tags");
        fieldType.setMultiValueState(MappedFieldType.MultiValueState.LIST);
        assertEquals(ValueCardinality.MULTI_VALUED, ValueCardinality.of(fieldType));
        assertFalse(ValueCardinality.of(fieldType).isSingleValued());
    }
}
