/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Test helper that writes a real single-column UTF-8 Parquet file via {@link NativeParquetWriter}, so
 * codec tests can read a keyword column back through the native binary cursor. A {@code null} element
 * writes a null row.
 */
public final class StringColumnFixture {

    private StringColumnFixture() {}

    public static void write(Path file, BufferAllocator allocator, String column, List<String> values) throws Exception {
        // List.of rejects a null argument to contains, so the nullability check scans instead.
        boolean nullable = values.stream().anyMatch(value -> value == null);
        FieldType fieldType = nullable ? FieldType.nullable(new ArrowType.Utf8()) : FieldType.notNullable(new ArrowType.Utf8());
        Schema schema = new Schema(List.of(new Field(column, fieldType, null)));

        NativeParquetWriter writer = new NativeParquetWriter(file.toString());
        ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
        Data.exportSchema(allocator, schema, null, schemaExport);
        try (ArrowExport export = new ArrowExport(null, schemaExport)) {
            writer.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
        }

        int rowCount = values.size();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarCharVector vector = (VarCharVector) root.getVector(column);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                String value = values.get(i);
                if (value == null) {
                    vector.setNull(i);
                } else {
                    vector.setSafe(i, value.getBytes(StandardCharsets.UTF_8));
                }
            }
            vector.setValueCount(rowCount);
            root.setRowCount(rowCount);

            ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
            ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
            Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
            try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                writer.write(export.getArrayAddress(), export.getSchemaAddress());
            }
        }
        writer.flush();
    }
}
