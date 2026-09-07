/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.codec;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
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
 * Segment-global ordinals for a keyword field, uninverted once from the Lucene sidecar's postings
 * and spilled to a memory-mapped node-local file — read-side only.
 *
 * <h2>On-disk format (v3 = Phase 1 + Phase 2)</h2>
 * <ul>
 *   <li><b>Phase 1 — per-block base + bit-width.</b> The ordinal sequence is split into fixed
 *       {@value #BLOCK_SIZE}-entry blocks; each block stores {@code (value - base)} bit-packed at
 *       its own minimal width, or is a <b>constant</b> block ({@code bits == 0}, base only, no
 *       payload) when all its values are equal. Decode-free on read.</li>
 *   <li><b>Phase 2 — presence via IndexedDISI.</b> A <b>dense</b> field ({@code numPresent ==
 *       maxDoc}) indexes the sequence by doc id directly. A <b>sparse</b> field writes an
 *       {@link IndexedDISI} presence bitmap and blocks the present docs only, so missing docs cost
 *       ~1 bit rather than a full slot.</li>
 * </ul>
 * Layout: {@code [header][DISI (sparse only)][block data][block index][checkpoints][trailer]}.
 * Stored values are {@code ord + 1} (so 0 would mean missing, though the dense path has none);
 * the read side returns {@code value - 1}.
 */
public final class UninvertedOrdinals implements Closeable {

    private static final String CODEC_PREFIX = "parquet-ords";
    private static final int ORD_FILE_MAGIC = 0x504F5244; // "PORD"
    private static final int ORD_FILE_VERSION = 3;
    private static final int ORD_FILE_FOOTER_MAGIC = 0x504F5246; // "PORF"

    private static final int BLOCK_SHIFT = 16;
    private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT; // 65536 entries per block
    private static final byte DENSE_RANK_POWER = 9; // IndexedDISI dense-rank granularity (Lucene default)
    // Trailer: blockIndexStart, checkpointStart, disiStart, disiLength (4 longs) + jumpTableEntryCount, footerMagic (2 ints).
    private static final int TRAILER_BYTES = 8 * 4 + 4 * 2;

    private record OrdFileMetadata(int maxDoc, long termCount, long assignedDocs, // == numPresent
        int checkpointInterval, int blockShift, boolean dense) {
    }

    private record LoadedOrdFile(IndexInput input, IndexInput payloadInput, IndexInput disiInput, // null when dense
        BytesRef[] checkpoints, int checkpointInterval, long sizeInBytes, int blockShift, int maxDoc, boolean dense, int numPresent,
        int jumpTableEntryCount, long[] blockRelOffset, long[] blockBase, byte[] blockBits) {
    }

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
    private final IndexInput disiInput; // null when dense
    private final BytesRef[] checkpoints;
    /**
     * The checkpoint spacing this file was actually built with, read from its header. Every
     * ord&harr;term lookup uses this, never the current cluster setting, so a dynamic setting
     * change can never desynchronise a live reader from its on-disk checkpoints.
     */
    private final int checkpointInterval;
    private final Terms terms;
    private final int valueCount;
    private final long sizeInBytes;
    private final String fileName;

    private final int blockShift;
    private final int blockSize;
    private final int maxDoc;
    private final boolean dense;
    private final int numPresent;
    private final int jumpTableEntryCount;
    private final long[] blockRelOffset;
    private final long[] blockBase;
    private final byte[] blockBits;

    private final AtomicBoolean closed = new AtomicBoolean();

    private UninvertedOrdinals(
        Directory directory,
        IndexInput input,
        IndexInput payloadInput,
        IndexInput disiInput,
        BytesRef[] checkpoints,
        int checkpointInterval,
        Terms terms,
        int valueCount,
        long sizeInBytes,
        String fileName,
        int blockShift,
        int maxDoc,
        boolean dense,
        int numPresent,
        int jumpTableEntryCount,
        long[] blockRelOffset,
        long[] blockBase,
        byte[] blockBits
    ) {
        this.directory = directory;
        this.input = input;
        this.payloadInput = payloadInput;
        this.disiInput = disiInput;
        this.checkpoints = checkpoints;
        this.checkpointInterval = checkpointInterval;
        this.terms = terms;
        this.valueCount = valueCount;
        this.sizeInBytes = sizeInBytes;
        this.fileName = fileName;
        this.blockShift = blockShift;
        this.blockSize = 1 << blockShift;
        this.maxDoc = maxDoc;
        this.dense = dense;
        this.numPresent = numPresent;
        this.jumpTableEntryCount = jumpTableEntryCount;
        this.blockRelOffset = blockRelOffset;
        this.blockBase = blockBase;
        this.blockBits = blockBits;
    }

    /**
     * The interval to stamp into a <em>newly built</em> ord file (and to use when estimating its
     * size). Never used on a read path: an already-built file carries its own interval in its
     * header, and {@link #checkpointInterval} is what every lookup honours. Changing the setting
     * therefore only affects files built afterwards; files already on disk (or already mapped by a
     * live reader) stay self-consistent and keep serving correct results.
     */
    private static int configuredCheckpointInterval() {
        return ParquetDocValuesProducer.checkpointInterval();
    }

    private static int blockCount(int len) {
        return len == 0 ? 0 : (len + BLOCK_SIZE - 1) / BLOCK_SIZE;
    }

    /** Value of the i-th sequence entry ({@code ord + 1}). Dense: by doc id; sparse: by present index. */
    private static long seqValue(PackedInts.Mutable building, int[] seqDocs, boolean dense, int i) {
        return dense ? building.get(i) : building.get(seqDocs[i]);
    }

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
            int buildBits = DirectWriter.bitsRequired(termCount + 1); // holds ord+1 (0 == missing)
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
                int interval = configuredCheckpointInterval();
                PackedInts.Mutable building = PackedInts.getMutable(maxDoc, buildBits, PackedInts.COMPACT);
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
                    writeOrdFile(out, building, maxDoc, termCount, (int) assignedDocs, interval, checkpoints);
                }

                directory.rename(tempName, fileName);
                loaded = loadOrdFile(directory, fileName, termCount, maxDoc, expectedNonNullDocs);
            }

            return new UninvertedOrdinals(
                directory,
                loaded.input(),
                loaded.payloadInput(),
                loaded.disiInput(),
                loaded.checkpoints(),
                loaded.checkpointInterval(),
                terms,
                (int) termCount,
                loaded.sizeInBytes(),
                fileName,
                loaded.blockShift(),
                loaded.maxDoc(),
                loaded.dense(),
                loaded.numPresent(),
                loaded.jumpTableEntryCount(),
                loaded.blockRelOffset(),
                loaded.blockBase(),
                loaded.blockBits()
            );
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
    }

    static long estimatedDiskBytes(long termCount, int maxDoc) {
        long bits = DirectWriter.bitsRequired(termCount + 1);
        int interval = configuredCheckpointInterval();
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

    private static void writeOrdFile(
        IndexOutput out,
        PackedInts.Mutable building,
        int maxDoc,
        long termCount,
        int numPresent,
        int interval,
        List<BytesRef> checkpoints
    ) throws IOException {
        boolean dense = numPresent == maxDoc;

        // present docs (sparse only) — also the DISI source.
        FixedBitSet present = null;
        int[] seqDocs = null;
        if (dense == false) {
            present = new FixedBitSet(maxDoc);
            seqDocs = new int[numPresent];
            int k = 0;
            for (int d = 0; d < maxDoc; d++) {
                if (building.get(d) != 0) {
                    present.set(d);
                    seqDocs[k++] = d;
                }
            }
        }
        int seqLen = dense ? maxDoc : numPresent;
        int nBlocks = blockCount(seqLen);

        // Header.
        out.writeInt(ORD_FILE_MAGIC);
        out.writeInt(ORD_FILE_VERSION);
        out.writeInt(maxDoc);
        out.writeLong(termCount);
        out.writeLong(numPresent);
        out.writeInt(interval);
        out.writeInt(BLOCK_SHIFT);
        out.writeByte((byte) (dense ? 1 : 0));

        // Presence bitmap (sparse only), right after the header.
        long disiStart = -1;
        long disiLength = 0;
        int jumpTableEntryCount = 0;
        if (dense == false) {
            disiStart = out.getFilePointer();
            jumpTableEntryCount = IndexedDISI.writeBitSet(new BitSetIterator(present, numPresent), out, DENSE_RANK_POWER);
            disiLength = out.getFilePointer() - disiStart;
        }

        // Block data: per-block base + bit width (constant when bits == 0).
        long[] base = new long[nBlocks];
        byte[] bits = new byte[nBlocks];
        for (int b = 0; b < nBlocks; b++) {
            int start = b << BLOCK_SHIFT;
            int len = Math.min(BLOCK_SIZE, seqLen - start);
            long mn = Long.MAX_VALUE;
            long mx = Long.MIN_VALUE;
            for (int i = 0; i < len; i++) {
                long v = seqValue(building, seqDocs, dense, start + i);
                if (v < mn) {
                    mn = v;
                }
                if (v > mx) {
                    mx = v;
                }
            }
            base[b] = mn;
            long range = mx - mn;
            if (range == 0) {
                bits[b] = 0; // constant block, no payload
                continue;
            }
            int w = DirectWriter.bitsRequired(range);
            bits[b] = (byte) w;
            DirectWriter writer = DirectWriter.getInstance(out, len, w);
            for (int i = 0; i < len; i++) {
                writer.add(seqValue(building, seqDocs, dense, start + i) - mn);
            }
            writer.finish();
        }

        long blockIndexStart = out.getFilePointer();
        out.writeVInt(nBlocks);
        for (int b = 0; b < nBlocks; b++) {
            out.writeVLong(base[b]);
            out.writeByte(bits[b]);
        }

        long checkpointStart = out.getFilePointer();
        for (BytesRef checkpoint : checkpoints) {
            out.writeInt(checkpoint.length);
            out.writeBytes(checkpoint.bytes, checkpoint.offset, checkpoint.length);
        }

        out.writeLong(blockIndexStart);
        out.writeLong(checkpointStart);
        out.writeLong(disiStart);
        out.writeLong(disiLength);
        out.writeInt(jumpTableEntryCount);
        out.writeInt(ORD_FILE_FOOTER_MAGIC);
    }

    private static OrdFileMetadata readOrdFileMetadata(IndexInput input) throws IOException {
        final int magic;
        final int version;
        final int maxDoc;
        final long termCount;
        final long assignedDocs;
        final int checkpointInterval;
        final int blockShift;
        final byte denseByte;
        try {
            magic = input.readInt();
            version = input.readInt();
            maxDoc = input.readInt();
            termCount = input.readLong();
            assignedDocs = input.readLong();
            checkpointInterval = input.readInt();
            blockShift = input.readInt();
            denseByte = input.readByte();
        } catch (IOException e) {
            throw new InvalidOrdFileException("ord file header is truncated", e);
        }
        if (magic != ORD_FILE_MAGIC) {
            throw new InvalidOrdFileException("legacy or invalid ord file magic: " + Integer.toHexString(magic));
        }
        if (version != ORD_FILE_VERSION) {
            throw new InvalidOrdFileException("unsupported ord file version: " + version);
        }
        if (denseByte != 0 && denseByte != 1) {
            throw new InvalidOrdFileException("ord file dense flag invalid: " + denseByte);
        }
        return new OrdFileMetadata(maxDoc, termCount, assignedDocs, checkpointInterval, blockShift, denseByte == 1);
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
        IndexInput disiInput = null;
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
            // NOTE: deliberately no check against the *current* checkpoint-interval setting. The
            // file's own interval (metadata.checkpointInterval()) is authoritative and is what the
            // returned instance uses for every lookup, so a file built under a different setting
            // stays perfectly valid. Rejecting it here would have forced a full re-uninvert of
            // every segment on any setting change.
            if (metadata.blockShift() != BLOCK_SHIFT) {
                throw new InvalidOrdFileException("ord file block layout mismatch");
            }
            boolean dense = metadata.dense();
            int numPresent = (int) metadata.assignedDocs();
            long headerEnd = input.getFilePointer();
            long fileLen = input.length();
            if (fileLen < headerEnd + TRAILER_BYTES) {
                throw new InvalidOrdFileException("ord file too short");
            }

            input.seek(fileLen - TRAILER_BYTES);
            long blockIndexStart = input.readLong();
            long checkpointStart = input.readLong();
            long disiStart = input.readLong();
            long disiLength = input.readLong();
            int jumpTableEntryCount = input.readInt();
            int footerMagic = input.readInt();
            if (footerMagic != ORD_FILE_FOOTER_MAGIC) {
                throw new InvalidOrdFileException("ord file footer mismatch");
            }

            long blockDataStart = dense ? headerEnd : (disiStart + disiLength);
            if (dense == false && disiStart != headerEnd) {
                throw new InvalidOrdFileException("ord file disi offset invalid");
            }
            if (blockIndexStart < blockDataStart || checkpointStart < blockIndexStart || checkpointStart > fileLen - TRAILER_BYTES) {
                throw new InvalidOrdFileException("ord file section offsets invalid");
            }

            int seqLen = dense ? expectedMaxDoc : numPresent;
            int expectedBlocks = blockCount(seqLen);
            input.seek(blockIndexStart);
            int nBlocks = input.readVInt();
            if (nBlocks != expectedBlocks) {
                throw new InvalidOrdFileException("ord file block count mismatch");
            }
            long[] relOffset = new long[nBlocks];
            long[] base = new long[nBlocks];
            byte[] bits = new byte[nBlocks];
            long acc = 0;
            for (int b = 0; b < nBlocks; b++) {
                base[b] = input.readVLong();
                bits[b] = input.readByte();
                relOffset[b] = acc;
                int len = Math.min(BLOCK_SIZE, seqLen - (b << BLOCK_SHIFT));
                if (bits[b] != 0) {
                    acc += DirectWriter.bytesRequired(len, bits[b]);
                }
            }
            if (acc != blockIndexStart - blockDataStart) {
                throw new InvalidOrdFileException("ord file block data length mismatch");
            }

            int interval = metadata.checkpointInterval();
            int expectedCheckpointCount = expectedTermCount == 0 ? 0 : (int) ((expectedTermCount + interval - 1) / interval);
            BytesRef[] checkpoints = readOrdFileCheckpoints(input, checkpointStart, expectedCheckpointCount);
            if (input.getFilePointer() != fileLen - TRAILER_BYTES) {
                throw new InvalidOrdFileException("ord file checkpoint section length mismatch");
            }

            payloadInput = input.slice("ord-payload", blockDataStart, blockIndexStart - blockDataStart);
            if (dense == false) {
                disiInput = input.slice("ord-disi", disiStart, disiLength);
            }
            return new LoadedOrdFile(
                input,
                payloadInput,
                disiInput,
                checkpoints,
                interval,
                directory.fileLength(fileName),
                metadata.blockShift(),
                expectedMaxDoc,
                dense,
                numPresent,
                jumpTableEntryCount,
                relOffset,
                base,
                bits
            );
        } catch (IOException | RuntimeException e) {
            if (disiInput != null) {
                disiInput.close();
            }
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

    /** True when every doc has a value (dense index-by-doc path); false when IndexedDISI-backed. */
    public boolean isDense() {
        return dense;
    }

    /** A single-consumer forward-only cursor. Dense: index by doc id; sparse: via IndexedDISI. */
    public OrdinalCursor newOrdinalCursor() {
        return new OrdinalCursor();
    }

    public final class OrdinalCursor {
        private int cachedBlock = -1;
        private long base;
        private int bits;
        private LongValues reader;
        private IndexedDISI disi;
        private int disiDoc = -1;

        private OrdinalCursor() {
            if (dense == false) {
                resetDisi();
            }
        }

        private void resetDisi() {
            try {
                disi = new IndexedDISI(disiInput.clone(), 0L, disiInput.length(), jumpTableEntryCount, DENSE_RANK_POWER, numPresent);
                disiDoc = -1;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /** The segment ordinal for {@code doc}, or -1 when the document has no value. */
        public int ordinal(int doc) {
            int index;
            if (dense) {
                index = doc;
            } else {
                try {
                    if (doc <= disiDoc) {
                        resetDisi(); // IndexedDISI is forward-only; restart on backward/repeat access
                    }
                    boolean present = disi.advanceExact(doc);
                    disiDoc = doc;
                    if (present == false) {
                        return -1;
                    }
                    index = disi.index();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            int block = index >>> blockShift;
            int within = index - (block << blockShift);
            if (block != cachedBlock) {
                loadBlock(block);
                cachedBlock = block;
            }
            return (int) (bits == 0 ? base : base + reader.get(within)) - 1;
        }

        private void loadBlock(int block) {
            base = blockBase[block];
            bits = blockBits[block];
            if (bits == 0) {
                reader = null;
                return;
            }
            int len = Math.min(blockSize, (dense ? maxDoc : numPresent) - (block << blockShift));
            long bytes = DirectWriter.bytesRequired(len, bits);
            try {
                reader = DirectReader.getInstance(payloadInput.randomAccessSlice(blockRelOffset[block], bytes), bits);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
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
            int interval = checkpointInterval;
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
            int interval = checkpointInterval;
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
                int interval = checkpointInterval;
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
            int interval = checkpointInterval;
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
        if (disiInput != null) {
            try {
                disiInput.close();
            } catch (IOException e) {
                first = e;
            }
        }
        try {
            payloadInput.close();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
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
