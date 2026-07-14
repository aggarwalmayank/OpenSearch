package org.opensearch.sandbox.search;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.search.aggregations.metrics.AbstractNumericTestCase;
import java.util.ArrayList;
import java.util.List;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/** Overrides parent's setupSuiteScopeCluster to omit setId() (custom doc IDs not supported in Parquet append-only). */
public abstract class MustangAbstractNumericTestCase extends AbstractNumericTestCase {

    public MustangAbstractNumericTestCase(org.opensearch.common.settings.Settings staticSettings) {
        super(staticSettings);
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setSource(jsonBuilder().startObject()
                        .field("value", i + 1)
                        .startArray("values").value(i + 2).value(i + 3).endArray()
                        .endObject())
            );
        }
        minValue = 1;
        minValues = 2;
        maxValue = numDocs;
        maxValues = numDocs + 2;
        indexRandom(true, false, builders);

        prepareCreate("empty_bucket_idx").setMapping("value", "type=integer").execute().actionGet();
        builders = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx")
                .setSource(jsonBuilder().startObject().field("value", i * 2).endObject()));
        }
        indexRandom(true, false, builders);
        ensureSearchable();
    }
}
