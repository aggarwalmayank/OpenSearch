/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetCodecBridge;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.be.datafusion.docvalues.iter.ParquetNumericDocValues;
import org.opensearch.be.datafusion.docvalues.iter.ParquetSortedDocValues;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Read-only {@link DocValuesProducer} that serves single-valued numeric and keyword/ip sorted doc
 * values from a Parquet file through Lucene's DocValues iterator API.
 *
 * <p>The constructor resolves the backing file and sanity-checks its row count against the segment's
 * {@code maxDoc}, but opens no cursor. It also captures the store those bytes come from: a hot shard's
 * Parquet files are on local disk, while a shard tiered to warm keeps them only in the remote object
 * store, reachable through the native store the engine stamped on the segment.
 *
 * <p>Each {@code getNumeric}/{@code getSortedNumeric}/{@code getSorted} opens its own
 * dedicated {@link ParquetColumnReader}: a native cursor is forward-only, so one shared across
 * concurrent segment-search slices would be driven backwards by one slice while another advances it.
 * A sorted iterator opens its cursor lazily on the first value request. {@link #close()} releases
 * every reader and is idempotent; a shared-registry producer tracks readers weakly instead of
 * pinning them, with close() as the backstop.
 */
public final class ParquetDocValuesProducer extends DocValuesProducer {

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesProducer.class);

    private static volatile int checkpointInterval = 128;

    /** For newly built .ord files only; existing files keep the interval recorded in them. */
    public static void setCheckpointInterval(int interval) {
        checkpointInterval = interval;
    }

    /** Checkpoint interval stamped into newly built .ord files. */
    static int checkpointInterval() {
        return checkpointInterval;
    }

    /** Oldest stamped format version this codec can decode, long-encoded as {@code major*1_000_000 + minor*1_000 + patch}. */
    static final long MIN_SUPPORTED_FORMAT_VERSION = 1_000_000L; // 1.0.0

    /**
     * Newest stamped format version this codec can decode. Deliberately a literal rather than a
     * reference to {@code ParquetDataFormatPlugin.PARQUET_FORMAT_VERSION}: tracking the writer
     * automatically would let a writer bump silently admit a file this decode logic has never seen.
     * {@code ParquetDocValuesProducerTests} asserts the two are equal, so a writer bump fails the build
     * until someone confirms the new version is readable and bumps this too.
     */
    static final long MAX_SUPPORTED_FORMAT_VERSION = 1_000_000L; // 1.0.0

    private final Path parquetFile;
    /**
     * Native object store every cursor reads {@link #parquetFile} through, or
     * {@link ParquetColumnReader#LOCAL_STORE} when the file is on local disk. Captured once from the
     * segment's stamp so a cursor opened later in the segment's life cannot disagree with the row count
     * validated here.
     */
    private final long storePointer;
    private final MapperService mapperService;
    /**
     * Index settings the decode-window sizes are resolved from, captured once so every cursor this
     * producer opens agrees. {@link Settings#EMPTY} when there is no mapper service, which only
     * happens in low-level tests.
     */
    private final Settings indexSettings;
    private final int maxDoc;
    private final long parquetRowCount;

    private final List<ParquetColumnReader> dedicatedReaders = Collections.synchronizedList(new ArrayList<>());

    /** True for the shared-registry producer: readers are tracked weakly, not pinned. */
    private final boolean reusedAcrossRequests;

    /** Weakly-held readers of a shared producer; {@link #close()} is the backstop for any still live. */
    private final Set<ParquetColumnReader> weakDedicatedReaders = Collections.newSetFromMap(new WeakHashMap<>());

    private volatile boolean closed;

    /**
     * @param mapperService resolves OpenSearch mapping types for DV-type validation (may be
     *                      {@code null} only in low-level tests that bypass type validation)
     * @throws IOException if the backing Parquet file for the segment cannot be resolved
     * @throws IllegalStateException if the Parquet row count does not match the segment's {@code maxDoc}
     */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService) throws IOException {
        this(state, mapperService, false);
    }

    /** @param reusedAcrossRequests true for the shared-registry producer; tracks readers weakly. */
    public ParquetDocValuesProducer(SegmentReadState state, MapperService mapperService, boolean reusedAcrossRequests) throws IOException {
        this.reusedAcrossRequests = reusedAcrossRequests;
        this.mapperService = mapperService;
        this.indexSettings = mapperService == null ? Settings.EMPTY : mapperService.getIndexSettings().getSettings();
        this.maxDoc = state.segmentInfo.maxDoc();

        ParquetSegmentLayout.ParquetSource resolved = ParquetSegmentLayout.resolve(state);
        if (resolved == null) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "no Parquet file bound to segment '%s' (maxDoc=%d); cannot serve Parquet doc values",
                    state.segmentInfo.name,
                    maxDoc
                )
            );
        }
        this.parquetFile = resolved.file();
        this.storePointer = resolved.storePointer();

        ParquetCodecBridge.FileMetadata metadata = ParquetCodecBridge.fileMetadata(parquetFile.toString(), storePointer);
        checkFormatVersion(metadata.opensearchFormatVersion(), parquetFile);
        this.parquetRowCount = metadata.numRows();
        if (parquetRowCount != maxDoc) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Parquet/Lucene row-count mismatch for segment '%s': Lucene maxDoc=%d but Parquet numRows=%d (file=%s)",
                    state.segmentInfo.name,
                    maxDoc,
                    parquetRowCount,
                    parquetFile
                )
            );
        }
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.NUMERIC);
        return new ParquetNumericDocValues(dedicatedReaderFor(field), maxDoc);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED_NUMERIC);
        // Ingest rejects multi-valued numerics (ParquetDocumentInput), so every numeric column on disk
        // is single-valued and this singleton wrap is exact; OpenSearch value sources recover the inner
        // iterator via DocValues.unwrapSingleton.
        // TODO(multi-value): needs a repeated read path once the write path emits arrays.
        return DocValues.singleton(new ParquetNumericDocValues(dedicatedReaderFor(field), maxDoc));
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        throw unsupported("binary", field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        ensureOpen();
        validate(field, DocValuesType.SORTED);
        // The reader recipe runs on the iterator's first value request, so an ordinal-only
        // consumer never opens a native cursor.
        return new ParquetSortedDocValues(() -> binaryDedicatedReaderFor(field), maxDoc);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        // Single-valued keyword/ip is served through getSorted and singleton-wrapped by the leaf
        // reader; SORTED_SET would reach here only for repeated columns, which have no read path.
        throw unsupported("sorted-set", field);
    }

    /** No DocValues skip index is served; the synthetic {@code FieldInfo}s advertise skip type NONE. */
    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        return null;
    }

    /**
     * Verifies the backing Parquet file is still accessible and its row count matches the value
     * cached at construction.
     *
     * <p>Not currently invoked: this producer is a search-time overlay, not a registered
     * {@code DocValuesFormat}, so codec-driven integrity checks (CheckIndex, merge-time verification)
     * do not reach it.
     */
    @Override
    public void checkIntegrity() throws IOException {
        ParquetCodecBridge.FileMetadata metadata = ParquetCodecBridge.fileMetadata(parquetFile.toString(), storePointer);
        if (metadata.numRows() != parquetRowCount) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "checkIntegrity: Parquet numRows changed for %s: expected %d, found %d",
                    parquetFile,
                    parquetRowCount,
                    metadata.numRows()
                )
            );
        }
    }

    /**
     * Number of rows with a non-null value in this column, from the Parquet footer's per-row-group
     * column-chunk statistics; {@code -1} when any row group lacks the null count. Used to verify
     * that postings-derived ordinal tables cover every stored value.
     */
    long nonNullRowCount(FieldInfo field) throws IOException {
        return ParquetCodecBridge.columnNonNullCount(parquetFile.toString(), field.getName(), storePointer);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        synchronized (dedicatedReaders) {
            for (ParquetColumnReader reader : dedicatedReaders) {
                try {
                    // A teardown failure is logged by the reader itself; this only guards the loop
                    // so one bad reader cannot leave the rest open.
                    reader.close();
                } catch (RuntimeException e) {
                    logger.warn("Failed to close Parquet column reader for [{}]", parquetFile, e);
                }
            }
            dedicatedReaders.clear();
        }
        if (reusedAcrossRequests) {
            // Backstop: close any weakly-tracked reader still live at segment close.
            List<ParquetColumnReader> liveReaders;
            synchronized (weakDedicatedReaders) {
                liveReaders = new ArrayList<>(weakDedicatedReaders);
                weakDedicatedReaders.clear();
            }
            for (ParquetColumnReader reader : liveReaders) {
                try {
                    reader.close();
                } catch (RuntimeException e) {
                    logger.warn("Failed to close Parquet column reader for [{}]", parquetFile, e);
                }
            }
        }
    }

    /**
     * Rejects a file this codec cannot decode: unstamped, older than {@link #MIN_SUPPORTED_FORMAT_VERSION},
     * or newer than {@link #MAX_SUPPORTED_FORMAT_VERSION}, failing on an out-of-range file rather than reading it
     * with assumptions that may not hold.
     */
    static void checkFormatVersion(long formatVersion, Path file) throws IOException {
        if (formatVersion == ParquetCodecBridge.FORMAT_VERSION_UNKNOWN) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s carries no parseable opensearch.format_version; this doc-values codec requires a stamped version in %s",
                    file,
                    supportedRange()
                )
            );
        }
        if (formatVersion < MIN_SUPPORTED_FORMAT_VERSION || formatVersion > MAX_SUPPORTED_FORMAT_VERSION) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Parquet file %s has OpenSearch format version %s, outside this doc-values codec's supported range %s",
                    file,
                    describeFormatVersion(formatVersion),
                    supportedRange()
                )
            );
        }
    }

    /** Scales of the long-encoded {@code major.minor.patch} version: {@code major*1_000_000 + minor*1_000 + patch}. */
    private static final long MAJOR_SCALE = 1_000_000L;
    private static final long MINOR_SCALE = 1_000L;

    /** Renders the inclusive supported version range for an error message. */
    private static String supportedRange() {
        return "[" + describeFormatVersion(MIN_SUPPORTED_FORMAT_VERSION) + ", " + describeFormatVersion(MAX_SUPPORTED_FORMAT_VERSION) + "]";
    }

    /** Renders a long-encoded format version as {@code major.minor.patch} for an error message. */
    private static String describeFormatVersion(long formatVersion) {
        long major = formatVersion / MAJOR_SCALE;
        long minor = formatVersion / MINOR_SCALE % MINOR_SCALE;
        long patch = formatVersion % MINOR_SCALE;
        return major + "." + minor + "." + patch;
    }

    /** Validates the field's mapping type supports the requested DV type, when a mapper is present. */
    private void validate(FieldInfo field, DocValuesType requested) {
        if (mapperService == null) {
            return; // low-level tests may bypass mapping validation
        }
        FieldTypeMapping.validate(field.getName(), mappingType(field), requested);
    }

    private String mappingType(FieldInfo field) {
        MappedFieldType mft = mapperService.fieldType(field.getName());
        if (mft == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "field '%s' has no mapping; cannot resolve Parquet column type", field.getName())
            );
        }
        return mft.typeName();
    }

    /** Opens a dedicated forward-only cursor for one iterator, registered for close with this producer. */
    private ParquetColumnReader dedicatedReaderFor(FieldInfo field) throws IOException {
        return registerForClose(ParquetColumnReader.open(parquetFile, field.getName(), indexSettings, storePointer));
    }

    /** Opens a dedicated binary cursor for one iterator, registered for close with this producer. */
    private ParquetColumnReader binaryDedicatedReaderFor(FieldInfo field) throws IOException {
        return registerForClose(ParquetColumnReader.openBinary(parquetFile, field.getName(), indexSettings, storePointer));
    }

    /**
     * Tracks a just-opened reader for release: pinned in {@link #dedicatedReaders} for a
     * request-scoped producer, weakly tracked for a shared one (the reader's Cleaner frees the
     * cursor once its iterator is unreachable). Registration happens under the same lock close()
     * drains under, so an open racing a concurrent close cannot leak the cursor.
     */
    private ParquetColumnReader registerForClose(ParquetColumnReader reader) throws IOException {
        Object lock = reusedAcrossRequests ? weakDedicatedReaders : dedicatedReaders;
        synchronized (lock) {
            if (closed) {
                reader.close();
                throw new IllegalStateException("producer for " + parquetFile + " is closed");
            }
            if (reusedAcrossRequests) {
                weakDedicatedReaders.add(reader);
            } else {
                dedicatedReaders.add(reader);
            }
        }
        return reader;
    }

    private UnsupportedOperationException unsupported(String kind, FieldInfo field) {
        return new UnsupportedOperationException(
            String.format(Locale.ROOT, "Parquet DocValues codec does not serve %s doc values (field '%s')", kind, field.getName())
        );
    }

    /** Whether {@link #close()} has run. */
    boolean isClosed() {
        return closed;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ParquetDocValuesProducer is closed");
        }
    }
}
