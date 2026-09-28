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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.nativebridge.spi.ArrowExport;
import org.opensearch.parquet.bridge.NativeParquetWriter;
import org.opensearch.parquet.bridge.ParquetSortConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Test helper that writes a real single-column {@code list<utf8>} Parquet file via {@link NativeParquetWriter},
 * the shape a multi-valued keyword column has on disk. A {@code null} row writes a null list.
 */
public final class StringListColumnFixture {

    private StringListColumnFixture() {}

    public static void write(Path file, BufferAllocator allocator, String column, List<List<String>> rows) throws Exception {
        try (ListVector listVector = ListVector.empty(column, allocator)) {
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Utf8()));
            VarCharVector data = (VarCharVector) listVector.getDataVector();
            listVector.allocateNew();
            data.allocateNew();
            int child = 0;
            for (int r = 0; r < rows.size(); r++) {
                List<String> row = rows.get(r);
                if (row == null) {
                    listVector.setNull(r);
                    continue;
                }
                listVector.startNewValue(r);
                for (String value : row) {
                    data.setSafe(child++, value.getBytes(StandardCharsets.UTF_8));
                }
                listVector.endValue(r, row.size());
            }
            data.setValueCount(child);
            listVector.setValueCount(rows.size());
            writeListColumn(file, allocator, listVector, rows.size());
        }
    }

    private static void writeListColumn(Path file, BufferAllocator allocator, ListVector listVector, int rowCount) throws Exception {
        Schema schema = new Schema(List.of(listVector.getField()));
        // Not AutoCloseable: flush() releases the native entry on success, cleanup() on any failure path.
        NativeParquetWriter parquetWriter = new NativeParquetWriter(file.toString());
        try {
            ArrowSchema schemaExport = ArrowSchema.allocateNew(allocator);
            Data.exportSchema(allocator, schema, null, schemaExport);
            try (ArrowExport export = new ArrowExport(null, schemaExport)) {
                parquetWriter.initialize("test-index", export.getSchemaAddress(), ParquetSortConfig.empty(), 0L);
            }

            try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), List.of(listVector), rowCount)) {
                ArrowArray arrayExport = ArrowArray.allocateNew(allocator);
                ArrowSchema dataSchema = ArrowSchema.allocateNew(allocator);
                Data.exportVectorSchemaRoot(allocator, root, null, arrayExport, dataSchema);
                try (ArrowExport export = new ArrowExport(arrayExport, dataSchema)) {
                    parquetWriter.write(export.getArrayAddress(), export.getSchemaAddress());
                }
            }
            parquetWriter.flush();
        } finally {
            parquetWriter.cleanup();
        }
    }
}
