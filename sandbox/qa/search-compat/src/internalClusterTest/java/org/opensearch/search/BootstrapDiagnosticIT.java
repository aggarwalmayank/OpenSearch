package org.opensearch.search;


import org.opensearch.test.OpenSearchTestCase;

@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE)
public class BootstrapDiagnosticIT extends OpenSearchTestCase {
    public void testBootstrap() {
        // If we get here, BootstrapForTesting initialized successfully
        assertTrue(true);
    }
}
