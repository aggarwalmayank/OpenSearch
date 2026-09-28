/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues.bridge;

import java.io.IOException;

/** Minimal decoded-batch source used by the multi-valued keyword DocValues iterator. */
public interface ListBinaryValueReader {

    /** The currently decoded list batch, or {@code null} when none is loaded. */
    DecodedListBinaryBatch decodedListBinaryBatch();

    /** Loads a decoded list batch containing {@code row}. */
    void loadListBinaryBatchContaining(long row) throws IOException;
}
