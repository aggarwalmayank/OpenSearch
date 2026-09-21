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
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Segment-global ordinals for a keyword field, uninverted once from the Lucene sidecar's postings
 * into a memory-mapped node-local file (v3).
 *
 * <p>Layout: {@code [header][DISI (sparse only)][block data][block index][checkpoints][section offsets]}.
 * Ordinals are stored as {@code ord + 1} in fixed {@value #BLOCK_SIZE}-entry blocks, each
 * bit-packed against its own base (constant blocks carry no payload). Sparse fields index only
 * present docs, with an {@link IndexedDISI} presence bitmap.
 */
public final class UninvertedOrdinals implements Closeable {

    private static final Logger logger = LogManager.getLogger(UninvertedOrdinals.class);

    private static final int ORD_FILE_MAGIC = 0x504F5244; // "PORD"
    // Version 2 appended a CRC32 of the whole file after the footer magic.
    private static final int ORD_FILE_VERSION = 2;
    private static final int ORD_FILE_FOOTER_MAGIC = 0x504F5246; // "PORF"
    private static final int CHECKSUM_BYTES = Long.BYTES;

    private static final int BLOCK_SHIFT = 16;
    private static final int BLOCK_SIZE = 1 << BLOCK_SHIFT; // 65536 entries per block (Lucene default)
    private static final byte DENSE_RANK_POWER = 9; // IndexedDISI dense-rank granularity (Lucene default)
    // Section-offsets block: blockIndexStart, checkpointStart, disiStart, disiLength (4 longs) + jumpTableEntryCount, footerMagic (2 ints)
    // + CRC32 (1 long).
    private static final int SECTION_OFFSETS_BYTES = 8 * 4 + 4 * 2 + CHECKSUM_BYTES;

    private final Directory directory;
    private final IndexInput input;
    private final IndexInput payloadInput;
    private final IndexInput disiInput; // null when dense
    private final BytesRef[] checkpoints;
    /** Checkpoint spacing from this file's own header — never the live cluster setting. */
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
     * Loads the field's existing ord file, or returns {@code null} when no usable file exists —
     * missing, or invalid for this segment (wrong doc count, term count, coverage, or layout), in
     * which case the invalid file is deleted so a rebuild can replace it.
     */
    static UninvertedOrdinals load(Path ordsDir, String fileKey, Terms terms, int maxDoc, long expectedNonNullDocs) throws IOException {
        long termCount = verifiableTermCount(terms, expectedNonNullDocs);
        Directory directory = new MMapDirectory(ordsDir); // the file-IO handle for the ords folder; memory-maps reads
        String fileName = OrdFilePaths.ordFileName(fileKey);
        try {
            LoadedOrdFile loaded;
            try {
                loaded = loadOrdFile(directory, fileName, termCount, maxDoc, expectedNonNullDocs);
            } catch (FileNotFoundException | NoSuchFileException e) {
                directory.close();
                return null;
            } catch (InvalidOrdFileException e) {
                // The rebuild self-heals, but without this log repeated corruption (a failing
                // disk, a misbehaving backup tool) would stay invisible.
                logger.warn("deleting invalid ord file [{}] so it can be rebuilt: {}", fileName, e.getMessage());
                deleteInvalidOrdFileIfPresent(directory, fileName);
                directory.close();
                return null;
            }
            return openOrdinals(directory, loaded, terms, termCount, fileName);
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
    }

    /**
     * Builds the field's ord file from postings — walking every term's postings into a packed
     * doc-to-ordinal array, verifying coverage against the column's non-null count, and
     * publishing via temp-file-then-rename — then loads and returns it. Callers wanting an
     * existing file should {@link #load} first; a leftover file here is simply overwritten.
     */
    static UninvertedOrdinals build(
        Path ordsDir,
        String fileKey,
        Terms terms,
        int maxDoc,
        long expectedNonNullDocs,
        BooleanSupplier cancelled
    ) throws IOException {
        long termCount = verifiableTermCount(terms, expectedNonNullDocs);
        Directory directory = new MMapDirectory(ordsDir);
        String fileName = OrdFilePaths.ordFileName(fileKey);
        try {
            int buildBits = DirectWriter.bitsRequired(termCount + 1);
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

            deleteFileIfPresent(directory, fileName);
            directory.rename(tempName, fileName);
            LoadedOrdFile loaded = loadOrdFile(directory, fileName, termCount, maxDoc, expectedNonNullDocs);
            return openOrdinals(directory, loaded, terms, termCount, fileName);
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

    /** True when every doc has a value (dense index-by-doc path); false when IndexedDISI-backed. */
    public boolean isDense() {
        return dense;
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

    /** A single-consumer forward-only cursor. Dense: index by doc id; sparse: via IndexedDISI. */
    public OrdinalCursor newOrdinalCursor() {
        return new OrdinalCursor();
    }

    /**
     * The field's terms enum wrapped with ordinal tracking: consumers like {@code OrdinalMap}
     * need {@link TermsEnum#ord()}, which BlockTree does not implement.
     */
    public TermsEnum termsEnum() throws IOException {
        return new OrdTrackingTermsEnum(terms.iterator());
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

    /** The terms count, after checking that ordinal coverage can be verified at all. */
    private static long verifiableTermCount(Terms terms, long expectedNonNullDocs) throws IOException {
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
        return termCount;
    }

    /** Assembles the reader over a loaded ord file; takes ownership of {@code directory}. */
    private static UninvertedOrdinals openOrdinals(
        Directory directory,
        LoadedOrdFile loaded,
        Terms terms,
        long termCount,
        String fileName
    ) {
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
    }

    /**
     * Interval stamped into newly built files (and used for size estimates). Read paths always
     * honour the file's own header instead, so setting changes never invalidate existing files.
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
            long smallest = Long.MAX_VALUE;
            long largest = Long.MIN_VALUE;
            for (int i = 0; i < len; i++) {
                long value = seqValue(building, seqDocs, dense, start + i);
                if (value < smallest) {
                    smallest = value;
                }
                if (value > largest) {
                    largest = value;
                }
            }
            base[b] = smallest;
            long range = largest - smallest;
            if (range == 0) {
                bits[b] = 0; // constant block, no payload
                continue;
            }
            int bitsPerValue = DirectWriter.bitsRequired(range);
            bits[b] = (byte) bitsPerValue;
            DirectWriter writer = DirectWriter.getInstance(out, len, bitsPerValue);
            for (int i = 0; i < len; i++) {
                writer.add(seqValue(building, seqDocs, dense, start + i) - smallest);
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
        out.writeLong(out.getChecksum());
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
            validateMetadata(metadata, fileName, expectedMaxDoc, expectedTermCount, expectedNonNullDocs);
            verifyChecksum(directory, fileName);

            boolean dense = metadata.dense();
            int numPresent = (int) metadata.assignedDocs();
            long headerEnd = input.getFilePointer();
            long fileLen = input.length();
            if (fileLen < headerEnd + SECTION_OFFSETS_BYTES) {
                throw new InvalidOrdFileException("ord file too short");
            }

            OrdFileSectionOffsets sectionOffsets = readSectionOffsets(input, fileLen);
            long blockDataStart = dense ? headerEnd : (sectionOffsets.disiStart() + sectionOffsets.disiLength());
            if (dense == false && sectionOffsets.disiStart() != headerEnd) {
                throw new InvalidOrdFileException("ord file disi offset invalid");
            }
            if (sectionOffsets.blockIndexStart() < blockDataStart
                || sectionOffsets.checkpointStart() < sectionOffsets.blockIndexStart()
                || sectionOffsets.checkpointStart() > fileLen - SECTION_OFFSETS_BYTES) {
                throw new InvalidOrdFileException("ord file section offsets invalid");
            }

            int seqLen = dense ? expectedMaxDoc : numPresent;
            BlockIndex blocks = readBlockIndex(
                input,
                sectionOffsets.blockIndexStart(),
                seqLen,
                sectionOffsets.blockIndexStart() - blockDataStart
            );

            int interval = metadata.checkpointInterval();
            int expectedCheckpointCount = expectedTermCount == 0 ? 0 : (int) ((expectedTermCount + interval - 1) / interval);
            BytesRef[] checkpoints = readOrdFileCheckpoints(input, sectionOffsets.checkpointStart(), expectedCheckpointCount);
            if (input.getFilePointer() != fileLen - SECTION_OFFSETS_BYTES) {
                throw new InvalidOrdFileException("ord file checkpoint section length mismatch");
            }

            payloadInput = input.slice("ord-payload", blockDataStart, sectionOffsets.blockIndexStart() - blockDataStart);
            if (dense == false) {
                disiInput = input.slice("ord-disi", sectionOffsets.disiStart(), sectionOffsets.disiLength());
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
                sectionOffsets.jumpTableEntryCount(),
                blocks.relOffset(),
                blocks.base(),
                blocks.bits()
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

    /** Rejects a file whose header does not match the live segment (stale) or this code's block layout. */
    private static void validateMetadata(
        OrdFileMetadata metadata,
        String fileName,
        int expectedMaxDoc,
        long expectedTermCount,
        long expectedNonNullDocs
    ) throws InvalidOrdFileException {
        if (metadata.maxDoc() != expectedMaxDoc) {
            throw new InvalidOrdFileException("ord file maxDoc mismatch");
        }
        if (metadata.termCount() != expectedTermCount) {
            throw new InvalidOrdFileException("ord file termCount mismatch");
        }
        if (metadata.assignedDocs() != expectedNonNullDocs) {
            throw new InvalidOrdFileException(coverageMismatchMessage(fileName, metadata.assignedDocs(), expectedNonNullDocs));
        }
        if (metadata.blockShift() != BLOCK_SHIFT) {
            throw new InvalidOrdFileException("ord file block layout mismatch");
        }
    }

    /**
     * Verifies the CRC32 stored as the file's final 8 bytes against every byte before it. Runs
     * once per load; the sequential read doubles as page-cache warm-up for the mmap that follows.
     */
    private static void verifyChecksum(Directory directory, String fileName) throws IOException {
        try (ChecksumIndexInput in = directory.openChecksumInput(fileName)) {
            long checksummedBytes = in.length() - CHECKSUM_BYTES;
            if (checksummedBytes < 0) {
                throw new InvalidOrdFileException("ord file too short for a checksum");
            }
            in.skipBytes(checksummedBytes);
            long actual = in.getChecksum();
            long stored = in.readLong();
            if (actual != stored) {
                throw new InvalidOrdFileException(
                    "ord file checksum mismatch: stored=" + Long.toHexString(stored) + " actual=" + Long.toHexString(actual)
                );
            }
        }
    }

    private static OrdFileSectionOffsets readSectionOffsets(IndexInput input, long fileLen) throws IOException {
        input.seek(fileLen - SECTION_OFFSETS_BYTES);
        long blockIndexStart = input.readLong();
        long checkpointStart = input.readLong();
        long disiStart = input.readLong();
        long disiLength = input.readLong();
        int jumpTableEntryCount = input.readInt();
        int footerMagic = input.readInt();
        if (footerMagic != ORD_FILE_FOOTER_MAGIC) {
            throw new InvalidOrdFileException("ord file footer mismatch");
        }
        return new OrdFileSectionOffsets(blockIndexStart, checkpointStart, disiStart, disiLength, jumpTableEntryCount);
    }

    private static BlockIndex readBlockIndex(IndexInput input, long blockIndexStart, int seqLen, long expectedBlockDataLength)
        throws IOException {
        input.seek(blockIndexStart);
        int nBlocks = input.readVInt();
        if (nBlocks != blockCount(seqLen)) {
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
        if (acc != expectedBlockDataLength) {
            throw new InvalidOrdFileException("ord file block data length mismatch");
        }
        return new BlockIndex(relOffset, base, bits);
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

    /**
     * Stateful ord→term resolver for one consumer keeps its enum position so
     * ascending ordinal walks amortize to a single sequential pass.
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

    private final class OrdTrackingTermsEnum extends FilterLeafReader.FilterTermsEnum {
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

    private record OrdFileMetadata(int maxDoc, long termCount, long assignedDocs, // == numPresent
        int checkpointInterval, int blockShift, boolean dense) {
    }

    private record LoadedOrdFile(IndexInput input, IndexInput payloadInput, IndexInput disiInput, // null when dense
        BytesRef[] checkpoints, int checkpointInterval, long sizeInBytes, int blockShift, int maxDoc, boolean dense, int numPresent,
        int jumpTableEntryCount, long[] blockRelOffset, long[] blockBase, byte[] blockBits) {
    }

    /** The fixed-size block at the file's end: section offsets and the jump-table entry count. */
    private record OrdFileSectionOffsets(long blockIndexStart, long checkpointStart, long disiStart, long disiLength,
        int jumpTableEntryCount) {
    }

    /** Per-block index: cumulative data offset, first-value base, and bit width of each block. */
    private record BlockIndex(long[] relOffset, long[] base, byte[] bits) {
    }

    private static final class InvalidOrdFileException extends IOException {
        private InvalidOrdFileException(String message) {
            super(message);
        }

        private InvalidOrdFileException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
