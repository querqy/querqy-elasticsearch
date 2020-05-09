package querqy.elasticsearch.rewriter;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import querqy.elasticsearch.QuerqyPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;

public class AbstractRewriterIntegrationTest extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singleton(QuerqyPlugin.class);
    }

    private static final String INDEX_NAME = "test_index";

    protected static String getIndexName() {
        return INDEX_NAME;
    }

    public Map<String, String> doc(String... kv) {
        if (kv.length % 2 != 0) {
            throw new RuntimeException("Input size must be even");
        }

        Map<String, String> doc = new HashMap<>();
        for (int i = 0; i < kv.length; i = i + 2) {
            doc.put(kv[i], kv[i + 1]);
        }

        return doc;
    }

    @SafeVarargs
    public final void indexDocs(Map<String, String>... docs) {
        client().admin().indices().prepareCreate(getIndexName()).get();

        Arrays.stream(docs).forEach(doc ->
                client().prepareIndex(getIndexName(), null)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .setSource(doc)
                        .get());
    }
}
