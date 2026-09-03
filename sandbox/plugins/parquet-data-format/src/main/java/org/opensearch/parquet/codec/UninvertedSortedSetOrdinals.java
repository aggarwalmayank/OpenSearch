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
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
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
 * Segment-global ordinals for a <b>multi-valued</b> keyword field, uninverted once from the Lucene
 * sidecar's postings and spilled to a memory-mapped node-local file — read-side only. The
 * single-valued counterpart is {@link UninvertedOrdinals}; this class shares term resolution with it
 * via {@link KeywordTermOrdinals} and adds the multi-value (SORTED_SET) doc→ords storage.
 *
 * <h2>On-disk format (v1)</h2>
 * <pre>[header][ords][offset-meta][offset-data][checkpoints][trailer]</pre>
 * <ul>
 *   <li><b>ords</b> — every document's ordinals, concatenated in doc order (each document's set is
 *       ascending and deduplicated), bit-packed with a single global width via {@link DirectWriter}.</li>
 *   <li><b>offsets</b> — a {@link DirectMonotonicWriter} over {@code maxDoc + 1} values where
 *       {@code offset[d]} is the start index of doc {@code d}'s ords in the stream. Then
 *       {@code offset[d+1] - offset[d]} is doc {@code d}'s value count, and {@code == 0} means the
 *       document has no value — so the offsets carry both cardinality and presence (no separate
 *       presence bitmap needed).</li>
 * </ul>
 * Ordinals are stored 0-based (presence comes from the offsets, so no {@code +1} sentinel is needed).
 */
public final class UninvertedSortedSetOrdinals implements Closeable {

    private static final String CODEC_PREFIX = "parquet-ords-set";
    private static final int SSORD_MAGIC = 0x53534F52; // "SSOR"
    private static final int SSORD_VERSION = 1;
    private static final int SSORD_FOOTER_MAGIC = 0x53534F46; // "SSOF"
    private static final int MONOTONIC_BLOCK_SHIFT = 16;
    // Trailer: ordsStart, ordsLen, offMetaStart, offMetaLen, offDataStart, offDataLen, checkpointStart (7 longs) + footerMagic (int).
    private static final int TRAILER_BYTES = 8 * 7 + 4;

    private record OrdFileMetadata(int maxDoc, long termCount, long totalOrds, int checkpointInterval, int ordBits) {}

    private final Directory directory;
    private final IndexInput input;
    private final IndexInput ordsInput;      // packed ords stream (slice)
    private final IndexInput offsetDataInput; // DirectMonotonic data (slice)
    private final LongValues ords;           // DirectReader over ordsInput (0-based ordinals)
    private final LongValues offsets;        // DirectMonotonicReader over maxDoc+1 offsets
    private final KeywordTermOrdinals termOrdinals;
    private final int maxDoc;
    private final long totalOrds;
    private final long sizeInBytes;
    private final String fileName;
    private final AtomicBoolean closed = new AtomicBoolean();

    private UninvertedSortedSetOrdinals(
        Directory directory,
        IndexInput input,
        IndexInput ordsInput,
        IndexInput offsetDataInput,
        LongValues ords,
        LongValues offsets,
        KeywordTermOrdinals termOrdinals,
        int maxDoc,
        long totalOrds,
        long sizeInBytes,
        String fileName
    ) {
        this.directory = directory;
        this.input = input;
        this.ordsInput = ordsInput;
        this.offsetDataInput = offsetDataInput;
        this.ords = ords;
        this.offsets = offsets;
        this.termOrdinals = termOrdinals;
        this.maxDoc = maxDoc;
        this.totalOrds = totalOrds;
        this.sizeInBytes = sizeInBytes;
        this.fileName = fileName;
    }

    private static int ordBitsFor(long termCount) {
        // ordinals are 0..termCount-1; DirectWriter needs a supported width covering the max value.
        return DirectWriter.bitsRequired(Math.max(1L, termCount));
    }

    static UninvertedSortedSetOrdinals build(Path ordsDir, String fileKey, Terms terms, int maxDoc, java.util.function.BooleanSupplier cancelled)
        throws IOException {
        long termCount = terms.size();
        if (termCount < 0) {
            throw new IllegalStateException("terms index reports unknown size; cannot uninvert multi-value ordinals");
        }
        Files.createDirectories(ordsDir);
        Directory directory = new MMapDirectory(ordsDir);
        String fileName = CODEC_PREFIX + "-" + fileKey + ".ord";
        try {
            LoadedFile loaded;
            boolean exists;
            try {
                directory.fileLength(fileName);
                exists = true;
            } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                exists = false;
            }
            if (exists == false) {
                writeFreshOrdFile(directory, fileName, terms, maxDoc, termCount, cancelled);
            }
            loaded = loadOrdFile(directory, fileName, terms, maxDoc, termCount);
            return new UninvertedSortedSetOrdinals(
                directory,
                loaded.input,
                loaded.ordsInput,
                loaded.offsetDataInput,
                loaded.ords,
                loaded.offsets,
                loaded.termOrdinals,
                maxDoc,
                loaded.totalOrds,
                loaded.sizeInBytes,
                fileName
            );
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
    }

    /** Two-pass uninvert (count → offsets → fill), then serialize ords + monotonic offsets + checkpoints. */
    private static void writeFreshOrdFile(
        Directory directory,
        String fileName,
        Terms terms,
        int maxDoc,
        long termCount,
        java.util.function.BooleanSupplier cancelled
    ) throws IOException {
        int interval = KeywordTermOrdinals.checkpointInterval();

        // ---- Pass 1: per-doc value counts + sample checkpoints ----
        int[] counts = new int[maxDoc];
        List<BytesRef> checkpoints = new ArrayList<>((int) (termCount / interval) + 1);
        TermsEnum te = terms.iterator();
        PostingsEnum postings = null;
        long ord = 0;
        for (BytesRef term = te.next(); term != null; term = te.next(), ord++) {
            if ((ord % interval) == 0) {
                if (cancelled.getAsBoolean()) {
                    throw new IOException("multi-value ordinal build cancelled for " + fileName);
                }
                checkpoints.add(BytesRef.deepCopyOf(term));
            }
            postings = te.postings(postings, PostingsEnum.NONE);
            for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                counts[doc]++;
            }
        }

        // ---- offsets prefix-sum (maxDoc + 1) ----
        long[] offset = new long[maxDoc + 1];
        for (int d = 0; d < maxDoc; d++) {
            offset[d + 1] = offset[d] + counts[d];
        }
        long totalOrds = offset[maxDoc];
        if (totalOrds > Integer.MAX_VALUE) {
            throw new IllegalStateException("multi-value ordinal count " + totalOrds + " exceeds the current single-file limit");
        }

        // ---- Pass 2: place each ord at its doc's next slot (ascending term order => sorted, deduped) ----
        int ordBits = ordBitsFor(termCount);
        PackedInts.Mutable ordsBuf = PackedInts.getMutable((int) totalOrds, ordBits, PackedInts.COMPACT);
        int[] writePos = new int[maxDoc];
        for (int d = 0; d < maxDoc; d++) {
            writePos[d] = (int) offset[d];
        }
        te = terms.iterator();
        ord = 0;
        for (BytesRef term = te.next(); term != null; term = te.next(), ord++) {
            postings = te.postings(postings, PostingsEnum.NONE);
            for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                ordsBuf.set(writePos[doc]++, ord);
            }
        }

        String tempName = fileName + ".tmp";
        deleteIfPresent(directory, tempName);
        try (IndexOutput out = directory.createOutput(tempName, IOContext.DEFAULT)) {
            // Header.
            out.writeInt(SSORD_MAGIC);
            out.writeInt(SSORD_VERSION);
            out.writeInt(maxDoc);
            out.writeLong(termCount);
            out.writeLong(totalOrds);
            out.writeInt(interval);
            out.writeInt(ordBits);

            // ords stream.
            long ordsStart = out.getFilePointer();
            if (totalOrds > 0) {
                DirectWriter w = DirectWriter.getInstance(out, totalOrds, ordBits);
                for (long i = 0; i < totalOrds; i++) {
                    w.add(ordsBuf.get((int) i));
                }
                w.finish();
            }
            long ordsLen = out.getFilePointer() - ordsStart;

            // offsets: DirectMonotonic needs a separate meta + data stream; buffer meta in memory, then append.
            ByteBuffersDataOutput metaBuffer = new ByteBuffersDataOutput();
            try (ByteBuffersIndexOutput metaOut = new ByteBuffersIndexOutput(metaBuffer, "ss-offset-meta", "ss-offset-meta")) {
                long offDataStart = out.getFilePointer();
                DirectMonotonicWriter mw = DirectMonotonicWriter.getInstance(metaOut, out, maxDoc + 1L, MONOTONIC_BLOCK_SHIFT);
                for (int d = 0; d <= maxDoc; d++) {
                    mw.add(offset[d]);
                }
                mw.finish();
                long offDataLen = out.getFilePointer() - offDataStart;
                byte[] metaBytes = metaBuffer.toArrayCopy();

                long offMetaStart = out.getFilePointer();
                out.writeBytes(metaBytes, 0, metaBytes.length);
                long offMetaLen = out.getFilePointer() - offMetaStart;

                long checkpointStart = out.getFilePointer();
                KeywordTermOrdinals.writeCheckpoints(out, checkpoints);

                // Trailer (fixed size).
                out.writeLong(ordsStart);
                out.writeLong(ordsLen);
                out.writeLong(offMetaStart);
                out.writeLong(offMetaLen);
                out.writeLong(offDataStart);
                out.writeLong(offDataLen);
                out.writeLong(checkpointStart);
                out.writeInt(SSORD_FOOTER_MAGIC);
            }
        }
        directory.rename(tempName, fileName);
    }

    private record LoadedFile(
        IndexInput input,
        IndexInput ordsInput,
        IndexInput offsetDataInput,
        LongValues ords,
        LongValues offsets,
        KeywordTermOrdinals termOrdinals,
        long totalOrds,
        long sizeInBytes
    ) {}

    private static LoadedFile loadOrdFile(Directory directory, String fileName, Terms terms, int expectedMaxDoc, long expectedTermCount)
        throws IOException {
        IndexInput input = directory.openInput(fileName, IOContext.DEFAULT);
        IndexInput ordsInput = null;
        IndexInput offsetDataInput = null;
        try {
            int magic = input.readInt();
            int version = input.readInt();
            int maxDoc = input.readInt();
            long termCount = input.readLong();
            long totalOrds = input.readLong();
            int interval = input.readInt();
            int ordBits = input.readInt();
            if (magic != SSORD_MAGIC || version != SSORD_VERSION) {
                throw new IOException("invalid multi-value ord file header");
            }
            if (maxDoc != expectedMaxDoc || termCount != expectedTermCount) {
                throw new IOException("multi-value ord file maxDoc/termCount mismatch");
            }
            if (interval != KeywordTermOrdinals.checkpointInterval()) {
                throw new IOException("multi-value ord file checkpoint interval mismatch");
            }

            long fileLen = input.length();
            input.seek(fileLen - TRAILER_BYTES);
            long ordsStart = input.readLong();
            long ordsLen = input.readLong();
            long offMetaStart = input.readLong();
            long offMetaLen = input.readLong();
            long offDataStart = input.readLong();
            long offDataLen = input.readLong();
            long checkpointStart = input.readLong();
            int footerMagic = input.readInt();
            if (footerMagic != SSORD_FOOTER_MAGIC) {
                throw new IOException("multi-value ord file footer mismatch");
            }

            int expectedCheckpointCount = expectedTermCount == 0 ? 0 : (int) ((expectedTermCount + interval - 1) / interval);
            BytesRef[] checkpoints = KeywordTermOrdinals.readCheckpoints(input, checkpointStart, expectedCheckpointCount);
            KeywordTermOrdinals termOrdinals = new KeywordTermOrdinals(terms, checkpoints, (int) termCount);

            LongValues ords = null; // only read when a doc has values, i.e. totalOrds > 0
            if (totalOrds > 0) {
                ordsInput = input.slice("ss-ords", ordsStart, ordsLen);
                ords = DirectReader.getInstance(ordsInput.randomAccessSlice(0, ordsLen), ordBits);
            }

            IndexInput offsetMeta = input.slice("ss-offset-meta", offMetaStart, offMetaLen);
            DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(offsetMeta, maxDoc + 1L, MONOTONIC_BLOCK_SHIFT);
            offsetMeta.close();
            offsetDataInput = input.slice("ss-offset-data", offDataStart, offDataLen);
            LongValues offsets = DirectMonotonicReader.getInstance(meta, offsetDataInput.randomAccessSlice(0, offDataLen));

            return new LoadedFile(input, ordsInput, offsetDataInput, ords, offsets, termOrdinals, totalOrds, directory.fileLength(fileName));
        } catch (IOException | RuntimeException e) {
            if (ordsInput != null) {
                ordsInput.close();
            }
            if (offsetDataInput != null) {
                offsetDataInput.close();
            }
            input.close();
            throw e;
        }
    }

    private static void deleteIfPresent(Directory directory, String name) {
        try {
            directory.deleteFile(name);
        } catch (IOException e) {
            // ignore
        }
    }

    // ---- read-side accessors used by ParquetUninvertedSortedSetDocValues ----

    /** Start index (inclusive) of {@code doc}'s ordinals in the flat stream. */
    public long startOffset(int doc) {
        return offsets.get(doc);
    }

    /** End index (exclusive) of {@code doc}'s ordinals: {@code startOffset(doc+1)}. */
    public long endOffset(int doc) {
        return offsets.get(doc + 1);
    }

    /** The ordinal stored at flat-stream index {@code i}. */
    public int ordAt(long i) {
        return (int) ords.get(i);
    }

    public long totalOrds() {
        return totalOrds;
    }

    public int getValueCount() {
        return termOrdinals.valueCount();
    }

    public BytesRef term(int ord) {
        return termOrdinals.term(ord);
    }

    public KeywordTermOrdinals.TermCursor newTermCursor() {
        return termOrdinals.newTermCursor();
    }

    public int rank(BytesRef key) {
        return termOrdinals.rank(key);
    }

    public TermsEnum termsEnum() throws IOException {
        return termOrdinals.termsEnum();
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public String fileName() {
        return fileName;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        IOException first = null;
        for (Closeable c : new Closeable[] { ordsInput, offsetDataInput, input, directory }) {
            if (c == null) {
                continue;
            }
            try {
                c.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
