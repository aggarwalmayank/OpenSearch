/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.datafusion.docvalues.bridge.DecodedBatch;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Serves Parquet-claimed stored fields to a {@link StoredFieldVisitor} from the field's column, after
 * the Lucene segment's own stored fields. The column is the only copy of a stored value Parquet claimed
 * ({@code STORED_FIELDS}), so this is the read side of that claim: {@code docId} is the Parquet row, and
 * an absent row yields no stored field, as in Lucene.
 *
 * <p>One instance per request-scoped leaf reader. Cursors open on first use and are recorded on the
 * request's {@link CursorRegistry}; the wrappers returned by {@link #wrap} share them, so a request
 * opens at most one cursor per stored field per segment however many documents it fetches.
 */
final class ParquetStoredFields {

    private final ParquetDocValuesProducer producer;
    private final CursorRegistry cursors;
    private final List<Column> columns = new ArrayList<>();
    private final BytesRef scratch = new BytesRef(BytesRef.EMPTY_BYTES);

    ParquetStoredFields(ParquetSegmentResources resources, CursorRegistry cursors) {
        this.producer = resources.producer;
        this.cursors = cursors;
        for (Map.Entry<String, StoredFieldMapping.Kind> stored : resources.storedFields.entrySet()) {
            columns.add(new Column(resources.parquetFieldInfo(stored.getKey()), stored.getValue()));
        }
    }

    /** Offers each Parquet stored field to the visitor, until it answers {@code STOP}. */
    synchronized void visit(int docId, StoredFieldVisitor visitor) throws IOException {
        for (Column column : columns) {
            StoredFieldVisitor.Status status = visitor.needsField(column.fieldInfo);
            if (status == StoredFieldVisitor.Status.STOP) {
                return;
            }
            if (status != StoredFieldVisitor.Status.YES) {
                continue;
            }
            DecodedBatch batch = column.batchContaining(docId);
            if (batch.isPresent(docId) == false) {
                continue;
            }
            batch.bytesAt(docId, scratch);
            switch (column.kind) {
                case BINARY -> visitor.binaryField(
                    column.fieldInfo,
                    ArrayUtil.copyOfSubArray(scratch.bytes, scratch.offset, scratch.offset + scratch.length)
                );
            }
        }
    }

    StoredFields wrap(StoredFields in) {
        return new StoredFields() {
            @Override
            public void document(int docId, StoredFieldVisitor visitor) throws IOException {
                in.document(docId, visitor);
                visit(docId, visitor);
            }
        };
    }

    StoredFieldsReader wrap(StoredFieldsReader in) {
        return new Reader(in);
    }

    /** Lazily opened cursor over one stored column; the visitor's {@code FieldInfo} is the synthesized one. */
    private final class Column {
        final FieldInfo fieldInfo;
        final StoredFieldMapping.Kind kind;
        private ParquetColumnReader cursor;

        Column(FieldInfo fieldInfo, StoredFieldMapping.Kind kind) {
            this.fieldInfo = fieldInfo;
            this.kind = kind;
        }

        DecodedBatch batchContaining(int docId) throws IOException {
            if (cursor == null) {
                cursor = producer.openCursor(fieldInfo.name, cursors);
            }
            DecodedBatch batch = cursor.decodedBatch();
            if (batch == null || batch.contains(docId) == false) {
                cursor.loadBatchContaining(docId);
                batch = cursor.decodedBatch();
            }
            return batch;
        }
    }

    /** The sequential-access form: same overlay, over the segment's {@link StoredFieldsReader}. */
    private final class Reader extends StoredFieldsReader {
        private final StoredFieldsReader in;

        Reader(StoredFieldsReader in) {
            this.in = in;
        }

        @Override
        public void document(int docId, StoredFieldVisitor visitor) throws IOException {
            in.document(docId, visitor);
            visit(docId, visitor);
        }

        @Override
        public StoredFieldsReader clone() {
            return new Reader(in.clone());
        }

        @Override
        public void checkIntegrity() throws IOException {
            in.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            in.close(); // cursors belong to the request registry, not to this reader
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            return new Reader(in.getMergeInstance());
        }
    }
}
