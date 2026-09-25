/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

/**
 * A {@link SequentialStoredFieldsLeafReader} that serves numeric doc values for Parquet-resident
 * fields from the segment core's shared {@link ParquetDocValuesProducer}, delegating everything else
 * to the underlying leaf.
 *
 * <p>A Parquet-only field has no {@link FieldInfo} in the Lucene segment, so Lucene's
 * {@code PerFieldDocValuesFormat} cannot route to it. The {@link ParquetSegmentResources} for the core
 * carries a synthesized {@code FieldInfo} (with the DV type from {@link FieldTypeMapping}) for every
 * such field and the combined {@link FieldInfos} this reader presents; the numeric DV accessors serve
 * those fields from the shared producer. All other fields pass through unchanged.
 *
 * <p>It extends {@link SequentialStoredFieldsLeafReader} (not plain {@code FilterLeafReader}) so the
 * fetch phase can still retrieve stored fields: the derived-source layer above unwraps to this reader,
 * which passes the underlying segment's stored-fields reader straight through.
 *
 * <p>The resources are shared across requests over the same segment core. This wrapper is request-scoped:
 * the cursors it hands out are recorded on the request-wide {@link CursorRegistry}, which the wrapping
 * directory reader closes when the request ends.
 */
public final class ParquetDocValuesLeafReader extends SequentialStoredFieldsLeafReader {

    private final ParquetSegmentResources resources;
    private final CursorRegistry cursors;

    ParquetDocValuesLeafReader(LeafReader in, ParquetSegmentResources resources, CursorRegistry cursors) {
        super(in);
        this.resources = resources;
        this.cursors = cursors;
    }

    @Override
    public FieldInfos getFieldInfos() {
        return resources.combinedFieldInfos;
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        if (resources.parquetFieldInfo(field) != null) {
            // Synthesized Parquet fields are SORTED_NUMERIC; like CodecReader, an accessor whose DV
            // type does not match the FieldInfo returns null rather than serving the field.
            return null;
        }
        return in.getNumericDocValues(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        FieldInfo fi = resources.parquetFieldInfo(field);
        if (fi != null) {
            // OpenSearch numeric value sources request SORTED_NUMERIC even for single-valued fields,
            // then call DocValues.unwrapSingleton(...). The producer serves this as a singleton over
            // the single-valued numeric iterator (docId == Parquet row, asserted here). The cursor is
            // recorded on this request's registry and closed when the request ends.
            assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet doc-values read path";
            return resources.producer.getSortedNumeric(fi, cursors);
        }
        return in.getSortedNumericDocValues(field);
    }

    /**
     * Serves a {@link DocValuesSkipper} for a Parquet-resident field when its synthetic
     * {@link FieldInfo} declares a skip index (RANGE), and delegates to the underlying leaf
     * otherwise. The RANGE declaration and the producer's skipper gate share
     * {@link FieldTypeMapping#isRangeSkippable}, so a Parquet field declaring NONE here is exactly
     * one the producer would decline; short-circuiting on it avoids a needless native page-index
     * load.
     */
    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        FieldInfo fi = resources.parquetFieldInfo(field);
        if (fi != null) {
            if (fi.docValuesSkipIndexType() == DocValuesSkipIndexType.NONE) {
                return null;
            }
            // Doc IDs and Parquet rows coincide (identity __row_id__), so page row ranges are
            // directly valid as skipper doc ID intervals.
            assert resources.assertRowIdsAreIdentity(in) : "non-identity __row_id__ segment reached the Parquet doc-values skipper path";
            return resources.producer.getSkipper(fi);
        }
        return in.getDocValuesSkipper(field);
    }

    @Override
    protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
        // This reader overlays doc values only; the underlying segment holds the real stored fields.
        return reader;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }
}
