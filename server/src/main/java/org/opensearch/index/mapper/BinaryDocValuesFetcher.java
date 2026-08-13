/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.BytesStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldValueFetcher for binary doc values. Unlike sorted numeric and sorted set doc values, plain binary doc values
 * hold a single {@link BytesRef} per document, so a multi valued field packs all of its values into that one slot.
 * The framing written by {@link BinaryFieldMapper.CustomBinaryDocValuesField#binaryValue()} is a vInt holding the
 * number of values, followed by a vInt length and the raw bytes for each value. This fetcher decodes that framing,
 * mirroring {@link org.opensearch.index.fielddata.plain.AbstractBinaryDVLeafFieldData}.
 * <p>
 * For a doc, values will be deduplicated and sorted, since they are sorted and deduplicated while stored in lucene.
 *
 * @opensearch.internal
 */
public class BinaryDocValuesFetcher extends FieldValueFetcher {

    public BinaryDocValuesFetcher(MappedFieldType mappedFieldType, String simpleName) {
        super(simpleName);
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    public List<Object> fetch(LeafReader reader, int docId) throws IOException {
        List<Object> values = new ArrayList<>();
        try {
            final BinaryDocValues binaryDocValues = reader.getBinaryDocValues(mappedFieldType.name());
            if (binaryDocValues == null || binaryDocValues.advanceExact(docId) == false) {
                return values;
            }
            final BytesRef bytes = binaryDocValues.binaryValue();
            if (bytes.length == 0) {
                return values;
            }
            final BytesStreamInput in = new BytesStreamInput();
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            final int count = in.readVInt();
            for (int i = 0; i < count; i++) {
                final int length = in.readVInt();
                final int offset = in.getPosition();
                in.setPosition(offset + length);
                // deep copy, the backing array is owned by the doc values iterator and is reused across documents
                final byte[] value = new byte[length];
                System.arraycopy(bytes.bytes, offset, value, 0, length);
                values.add(new BytesRef(value));
            }
        } catch (IOException e) {
            throw new IOException("Failed to read doc values for document " + docId + " in field " + mappedFieldType.name(), e);
        }
        return values;
    }

    @Override
    public Object convert(Object value) {
        return mappedFieldType.valueForDisplay(value);
    }
}
