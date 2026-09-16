/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.index;

import org.apache.lucene.index.LeafReader;

/**
 * A leaf reader that can serve per-document value reads (for example source derivation)
 * through a cheaper view of itself — one that skips acceleration structures built for
 * whole-segment access such as sorting and aggregations.
 *
 * @opensearch.internal
 */
public interface PerDocumentValuesProvider {
    /**
     * A view of this reader whose doc values serve per-document reads without building
     * whole-segment acceleration.
     */
    LeafReader perDocumentValuesReader();
}
