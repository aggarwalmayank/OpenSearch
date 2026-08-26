/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Segment-global ordinals for a high-cardinality keyword field, uninverted once from the Lucene
 * sidecar's postings and spilled to a memory-mapped node-local file — read-side only, with
 * Lucene's own storage economics: the packed doc→ord array lives on disk and only touched pages
 * are resident.
 *
 * <h2>Build</h2>
 * One sequential sweep of the field's terms (already sorted on disk) and their postings assigns
 * each document its term's rank. The transient in-heap packed buffer is released after the spill;
 * builds are serialized node-wide and check the cancellation flag between terms. Deleted
 * documents keep their ordinals (collectors never visit them), matching Lucene's own doc-values
 * semantics until merge.
 *
 * <h2>Read</h2>
 * {@code ordinal(doc)} is one packed read from the mapped file (0 = missing; stored values are
 * ord + 1). {@code lookupOrd} uses sparse in-heap checkpoints (every {@code checkpointInterval}
 * terms, set via {@code parquet.docvalues.checkpoint.interval}) plus a bounded {@code TermsEnum}
 * advance — only final buckets and sort bounds resolve terms, never per-document access. The
 * interval is written into each file and read back, so an instance always uses the interval its
 * own {@code .ord} was built with.
 */
public final class UninvertedOrdinals implements Closeable {

    private static final String CODEC_PREFIX = "parquet-ords";
    // PORD = parquet ord file header magic.
    private static final int ORD_FILE_MAGIC = 0x504F5244; // "PORD"
    private static final int ORD_FILE_VERSION = 1;
    // PORF = parquet ord file footer magic.
    private static final int ORD_FILE_FOOTER_MAGIC = 0x504F5246; // "PORF"

    private record OrdFileMetadata(
        int maxDoc, // Number of document slots encoded in the payload.
        long termCount, // Distinct term count for this segment field.
        long assignedDocs, // Number of docs that actually received a non-missing ord during build.
        int checkpointInterval // Checkpoint spacing persisted with the file for layout validation.
    ) {}

    private record LoadedOrdFile(IndexInput input, IndexInput payloadInput, LongValues ords, BytesRef[] checkpoints, long sizeInBytes) {}

    private static final class InvalidOrdFileException extends IOException {
        private InvalidOrdFileException(String message) {
            super(message);
        }

        private InvalidOrdFileException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final Directory directory;
    private final IndexInput input;
    private final IndexInput payloadInput;
    private final LongValues ords;
    private final BytesRef[] checkpoints;
    private final Terms terms;
    private final int valueCount;
    private final long sizeInBytes;
    private final String fileName;
    private final AtomicBoolean closed = new AtomicBoolean();

    private UninvertedOrdinals(
        Directory directory,
        IndexInput input,
        IndexInput payloadInput,
        LongValues ords,
        BytesRef[] checkpoints,
        Terms terms,
        int valueCount,
        long sizeInBytes,
        String fileName
    ) {
        this.directory = directory;
        this.input = input;
        this.payloadInput = payloadInput;
        this.ords = ords;
        this.checkpoints = checkpoints;
        this.terms = terms;
        this.valueCount = valueCount;
        this.sizeInBytes = sizeInBytes;
        this.fileName = fileName;
    }

    /**
     * Current checkpoint interval from the dynamic node setting {@code parquet.docvalues.checkpoint.interval}.
     * A {@code .ord} is rebuilt on load if its stored interval differs, so read-side use of the live value
     * is safe as long as the setting is only changed with the ordinal files wiped/rebuilt.
     */
    private static int checkpointInterval() {
        return ParquetDocValuesProducer.checkpointInterval();
    }

    /**
     * Builds (or maps an existing) ordinal file for the field. {@code cancelled} is polled
     * between terms during the sweep so runaway builds die with their task.
     */
    static UninvertedOrdinals build(
        Path ordsDir,
        String fileKey,
        Terms terms,
        int maxDoc,
        long expectedNonNullDocs,
        java.util.function.BooleanSupplier cancelled
    ) throws IOException {
        if (expectedNonNullDocs < 0) {
            throw new IllegalStateException(
                "cannot verify ordinal coverage (column null statistics unavailable); refusing to "
                    + "serve postings-derived ordinals that may silently drop unindexed values"
            );
        }
        long termCount = terms.size();
        if (termCount < 0) {
            throw new IllegalStateException("terms index reports unknown size; cannot uninvert");
        }

        Files.createDirectories(ordsDir);
        Directory directory = new MMapDirectory(ordsDir);
        String fileName = CODEC_PREFIX + "-" + fileKey + ".ord";
        try {
            int bits = DirectWriter.bitsRequired(termCount + 1);
            LoadedOrdFile loaded = null;
            boolean exists;
            try {
                directory.fileLength(fileName);
                exists = true;
            } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                exists = false;
            }
            if (exists) {
                try {
                    loaded = loadOrdFile(directory, fileName, termCount, maxDoc, expectedNonNullDocs);
                } catch (InvalidOrdFileException e) {
                    deleteInvalidOrdFileIfPresent(directory, fileName);
                    exists = false;
                } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                    exists = false;
                }
            }

            if (exists == false) {
                int interval = checkpointInterval();
                PackedInts.Mutable building = PackedInts.getMutable(maxDoc, bits, PackedInts.COMPACT);
                List<BytesRef> checkpoints = new ArrayList<>((int) (termCount / interval) + 1);
                TermsEnum termsEnum = terms.iterator();
                PostingsEnum postings = null;
                long ord = 0;
                long assignedDocs = 0;
                for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next(), ord++) {
                    if ((ord % interval) == 0 && cancelled.getAsBoolean()) {
                        throw new IOException("ordinal build cancelled for " + fileKey);
                    }
                    if ((ord % interval) == 0) {
                        checkpoints.add(BytesRef.deepCopyOf(term));
                    }
                    assignedDocs += termsEnum.docFreq();
                    postings = termsEnum.postings(postings, PostingsEnum.NONE);
                    for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                        building.set(doc, ord + 1);
                    }
                }
                if (assignedDocs != expectedNonNullDocs) {
                    throw new IllegalStateException(coverageMismatchMessage(fileKey, assignedDocs, expectedNonNullDocs));
                }

                String tempName = fileName + ".tmp";
                deleteFileIfPresent(directory, tempName);
                try (IndexOutput out = directory.createOutput(tempName, IOContext.DEFAULT)) {
                    writeOrdFileHeader(out, maxDoc, termCount, assignedDocs, interval);
                    DirectWriter writer = DirectWriter.getInstance(out, maxDoc, bits);
                    for (int doc = 0; doc < maxDoc; doc++) {
                        writer.add(building.get(doc));
                    }
                    writer.finish();
                    writeOrdFileCheckpoints(out, checkpoints);
                    out.writeInt(ORD_FILE_FOOTER_MAGIC);
                }

                directory.rename(tempName, fileName);
                loaded = loadOrdFile(directory, fileName, termCount, maxDoc, expectedNonNullDocs);
            }

            return new UninvertedOrdinals(
                directory,
                loaded.input(),
                loaded.payloadInput(),
                loaded.ords(),
                loaded.checkpoints(),
                terms,
                (int) termCount,
                loaded.sizeInBytes(),
                fileName
            );
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
    }

    static long estimatedDiskBytes(long termCount, int maxDoc) {
        long bits = DirectWriter.bitsRequired(termCount + 1);
        int interval = checkpointInterval();
        long checkpointCount = termCount == 0 ? 0 : (termCount + interval - 1) / interval;
        long checkpointEstimate = checkpointCount * 64L;
        return DirectWriter.bytesRequired(maxDoc, (int) bits) + 1024L + checkpointEstimate;
    }


    private static String coverageMismatchMessage(String fileKey, long assignedDocs, long expectedNonNullDocs) {
        return "ordinal coverage mismatch for "
            + fileKey
            + ": postings assign "
            + assignedDocs
            + " documents but the column stores "
            + expectedNonNullDocs
            + " non-null values — some stored values are not indexed (ignore_above?); "
            + "refusing uninverted ordinals to avoid silent undercounts";
    }

    private static long packedPayloadLength(int maxDoc, int bits) {
        return DirectWriter.bytesRequired(maxDoc, bits);
    }


    private static void writeOrdFileHeader(IndexOutput out, int maxDoc, long termCount, long assignedDocs, int checkpointInterval)
        throws IOException {
        out.writeInt(ORD_FILE_MAGIC);
        out.writeInt(ORD_FILE_VERSION);
        out.writeInt(maxDoc);
        out.writeLong(termCount);
        out.writeLong(assignedDocs);
        out.writeInt(checkpointInterval);
    }

    private static void writeOrdFileCheckpoints(IndexOutput out, List<BytesRef> checkpoints) throws IOException {
        for (BytesRef checkpoint : checkpoints) {
            out.writeInt(checkpoint.length);
            out.writeBytes(checkpoint.bytes, checkpoint.offset, checkpoint.length);
        }
    }

    private static OrdFileMetadata readOrdFileMetadata(IndexInput input) throws IOException {
        final int magic;
        final int version;
        final int maxDoc;
        final long termCount;
        final long assignedDocs;
        final int checkpointInterval;
        try {
            magic = input.readInt();
            version = input.readInt();
            maxDoc = input.readInt();
            termCount = input.readLong();
            assignedDocs = input.readLong();
            checkpointInterval = input.readInt();
        } catch (IOException e) {
            throw new InvalidOrdFileException("ord file header is truncated", e);
        }
        if (magic != ORD_FILE_MAGIC) {
            throw new InvalidOrdFileException("legacy or invalid ord file magic: " + Integer.toHexString(magic));
        }
        if (version != ORD_FILE_VERSION) {
            throw new InvalidOrdFileException("unsupported ord file version: " + version);
        }
        return new OrdFileMetadata(maxDoc, termCount, assignedDocs, checkpointInterval);
    }

    private static LoadedOrdFile loadOrdFile(
        Directory directory,
        String fileName,
        long expectedTermCount,
        int expectedMaxDoc,
        long expectedNonNullDocs
    ) throws IOException {
        IndexInput input = directory.openInput(fileName, IOContext.DEFAULT);
        IndexInput payloadInput = null;
        try {
            OrdFileMetadata metadata = readOrdFileMetadata(input);
            if (metadata.maxDoc() != expectedMaxDoc) {
                throw new InvalidOrdFileException("ord file maxDoc mismatch");
            }
            if (metadata.termCount() != expectedTermCount) {
                throw new InvalidOrdFileException("ord file termCount mismatch");
            }
            if (metadata.assignedDocs() != expectedNonNullDocs) {
                throw new InvalidOrdFileException(coverageMismatchMessage(fileName, metadata.assignedDocs(), expectedNonNullDocs));
            }
            if (metadata.checkpointInterval() != checkpointInterval()) {
                throw new InvalidOrdFileException("ord file checkpoint interval mismatch");
            }
            int interval = metadata.checkpointInterval();
            int expectedCheckpointCount = expectedTermCount == 0 ? 0 : (int) ((expectedTermCount + interval - 1) / interval);

            int bits = DirectWriter.bitsRequired(metadata.termCount() + 1);
            long payloadOffset = input.getFilePointer();
            long payloadLength = packedPayloadLength(expectedMaxDoc, bits);
            long checkpointStart = payloadOffset + payloadLength;
            if (checkpointStart > input.length()) {
                throw new InvalidOrdFileException("ord file payload length mismatch");
            }

            BytesRef[] checkpoints = readOrdFileCheckpoints(input, checkpointStart, expectedCheckpointCount);
            verifyOrdFileFooter(input);
            verifyOrdFileEnd(input);
            payloadInput = input.slice("ord-payload", payloadOffset, payloadLength);
            LongValues ords = DirectReader.getInstance(payloadInput.randomAccessSlice(0, payloadLength), bits);
            return new LoadedOrdFile(input, payloadInput, ords, checkpoints, directory.fileLength(fileName));
        } catch (IOException | RuntimeException e) {
            if (payloadInput != null) {
                payloadInput.close();
            }
            input.close();
            throw e;
        }
    }

    private static BytesRef[] readOrdFileCheckpoints(IndexInput input, long checkpointStart, int checkpointCount) throws IOException {
        input.seek(checkpointStart);
        BytesRef[] checkpoints = new BytesRef[checkpointCount];
        for (int i = 0; i < checkpointCount; i++) {
            final int length;
            try {
                length = input.readInt();
            } catch (IOException e) {
                throw new InvalidOrdFileException("ord file checkpoint section truncated", e);
            }
            if (length < 0) {
                throw new InvalidOrdFileException("ord file checkpoint length is invalid");
            }
            byte[] bytes = new byte[length];
            try {
                input.readBytes(bytes, 0, length);
            } catch (IOException e) {
                throw new InvalidOrdFileException("ord file checkpoint section truncated", e);
            }
            checkpoints[i] = new BytesRef(bytes);
        }
        return checkpoints;
    }

    private static void verifyOrdFileFooter(IndexInput input) throws IOException {
        final int footerMagic;
        try {
            footerMagic = input.readInt();
        } catch (IOException e) {
            throw new InvalidOrdFileException("ord file footer is truncated", e);
        }
        if (footerMagic != ORD_FILE_FOOTER_MAGIC) {
            throw new InvalidOrdFileException("ord file footer mismatch");
        }
    }

    private static void verifyOrdFileEnd(IndexInput input) throws IOException {
        if (input.getFilePointer() != input.length()) {
            throw new InvalidOrdFileException("ord file has trailing bytes");
        }
    }

    private static void deleteFileIfPresent(Directory directory, String fileName) {
        try {
            directory.deleteFile(fileName);
        } catch (IOException e) {
            // ignore stale/missing artifacts
        }
    }

    private static void deleteInvalidOrdFileIfPresent(Directory directory, String fileName) {
        deleteFileIfPresent(directory, fileName);
    }

    private static void closeLoadedOrdFile(LoadedOrdFile loaded) {
        if (loaded == null) {
            return;
        }
        try {
            loaded.payloadInput().close();
        } catch (IOException e) {
            // ignore
        }
        try {
            loaded.input().close();
        } catch (IOException e) {
            // ignore
        }
    }

    /** The segment ordinal for {@code doc}, or -1 when the document has no value. */
    public int ordinal(int doc) {
        return (int) ords.get(doc) - 1;
    }

    /** Number of distinct terms. */
    public int valueCount() {
        return valueCount;
    }

    /** On-disk footprint (cache accounting). */
    public long sizeInBytes() {
        return sizeInBytes;
    }

    /** The ord file's name within the ords directory (disk-budget pinning). */
    public String fileName() {
        return fileName;
    }

    /**
     * The field's real terms enumeration — the exact sorted term space these ordinals rank —
     * wrapped with ordinal tracking, because consumers like {@code OrdinalMap} require
     * {@link TermsEnum#ord()} which BlockTree does not implement. Ord seeks use the sparse
     * checkpoints; byte seeks re-derive the position via {@link #rank}.
     */
    public TermsEnum termsEnum() throws IOException {
        return new OrdTrackingTermsEnum(terms.iterator());
    }

    private final class OrdTrackingTermsEnum extends org.apache.lucene.index.FilterLeafReader.FilterTermsEnum {
        private long position = -1;

        OrdTrackingTermsEnum(TermsEnum in) {
            super(in);
        }

        @Override
        public BytesRef next() throws IOException {
            BytesRef term = in.next();
            if (term != null) {
                position++;
            } else {
                position = valueCount;
            }
            return term;
        }

        @Override
        public long ord() {
            return position;
        }

        @Override
        public void seekExact(long ord) throws IOException {
            int interval = checkpointInterval();
            int checkpoint = (int) (ord / interval);
            in.seekCeil(checkpoints[checkpoint]);
            position = (long) checkpoint * interval;
            while (position < ord) {
                in.next();
                position++;
            }
        }

        @Override
        public boolean seekExact(BytesRef text) throws IOException {
            boolean found = in.seekExact(text);
            position = found ? rank(text) : -1;
            return found;
        }

        @Override
        public SeekStatus seekCeil(BytesRef text) throws IOException {
            SeekStatus status = in.seekCeil(text);
            if (status == SeekStatus.END) {
                position = valueCount;
            } else {
                int r = rank(in.term());
                position = r >= 0 ? r : -(r + 1);
            }
            return status;
        }
    }

    /** Resolves an ordinal to its term: checkpoint seek plus a bounded enum advance. */
    public BytesRef term(int ord) {
        try {
            int interval = checkpointInterval();
            TermsEnum termsEnum = terms.iterator();
            int checkpoint = ord / interval;
            termsEnum.seekCeil(checkpoints[checkpoint]);
            for (int i = checkpoint * interval; i < ord; i++) {
                termsEnum.next();
            }
            return BytesRef.deepCopyOf(termsEnum.term());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A single-consumer stateful term resolver: ascending ordinal walks cost one enum pass. */
    public TermCursor newTermCursor() {
        return new TermCursor();
    }

    /**
     * Stateful ord→term resolution for one consumer (not thread-safe, like doc-values
     * iterators). A stateless resolver pays a checkpoint seek plus up to a checkpoint interval of
     * enum steps on every call; this cursor advances forward from
     * its last position when the requested ordinal is ahead, so monotonic access amortizes to a
     * single sequential pass over the terms file.
     */
    public final class TermCursor {
        private TermsEnum cursorEnum;
        private long cursorOrd = -1;

        public BytesRef term(int ord) {
            try {
                int interval = checkpointInterval();
                long delta = cursorEnum == null ? Long.MAX_VALUE : ord - cursorOrd;
                if (delta < 0 || delta > interval) {
                    cursorEnum = terms.iterator();
                    int checkpoint = ord / interval;
                    cursorEnum.seekCeil(checkpoints[checkpoint]);
                    cursorOrd = (long) checkpoint * interval;
                }
                while (cursorOrd < ord) {
                    cursorEnum.next();
                    cursorOrd++;
                }
                return cursorEnum.term();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** The ordinal of {@code key}, or {@code -insertionPoint - 1} (the lookupTerm contract). */
    public int rank(BytesRef key) {
        try {
            int interval = checkpointInterval();
            if (checkpoints.length == 0) {
                return -1;
            }
            int low = 0;
            int high = checkpoints.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = checkpoints[mid].compareTo(key);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid * interval;
                }
            }
            int checkpoint = Math.max(low - 1, 0);
            TermsEnum termsEnum = terms.iterator();
            termsEnum.seekCeil(checkpoints[checkpoint]);
            int ord = checkpoint * interval;
            BytesRef term = termsEnum.term();
            while (term != null) {
                int cmp = term.compareTo(key);
                if (cmp == 0) {
                    return ord;
                }
                if (cmp > 0) {
                    return -(ord + 1);
                }
                term = termsEnum.next();
                ord++;
            }
            return -(ord + 1);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        // Idempotent so eviction and later segment cleanup can safely race on the same entry.
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        IOException first = null;
        try {
            payloadInput.close();
        } catch (IOException e) {
            first = e;
        }
        try {
            input.close();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        try {
            directory.close();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
