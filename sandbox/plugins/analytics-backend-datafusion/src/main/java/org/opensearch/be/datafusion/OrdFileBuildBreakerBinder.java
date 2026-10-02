/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.docvalues.UninvertedOrdinalsCache;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;

/** Hands the node fielddata breaker to the ord-file build path once the node's services are bound. */
public final class OrdFileBuildBreakerBinder extends AbstractLifecycleComponent {

    @Inject
    public OrdFileBuildBreakerBinder(CircuitBreakerService circuitBreakerService) {
        UninvertedOrdinalsCache.setBuildBreaker(circuitBreakerService.getBreaker(CircuitBreaker.FIELDDATA));
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}
}
