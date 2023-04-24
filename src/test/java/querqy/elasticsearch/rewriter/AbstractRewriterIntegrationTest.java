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

public abstract class AbstractRewriterIntegrationTest extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singleton(QuerqyPlugin.class);
    }

    private static final String INDEX_NAME = "test_index";

    protected static String getIndexName() {
        return INDEX_NAME;
    }

    public Map<String, Object> doc(Object... kv) {
        if (kv.length % 2 != 0) {
            throw new RuntimeException("Input size must be even");
        }

        Map<String, Object> doc = new HashMap<>();
        for (int i = 0; i < kv.length; i = i + 2) {
            doc.put((String) kv[i], kv[i + 1]);
        }

        return doc;
    }

    public void createIndex() {
        client().admin().indices().prepareCreate(getIndexName()).get();
    }

    public void createIndexWithMapping(String mapping) {
        client().admin().indices().prepareCreate(getIndexName()).setMapping(mapping).get();
    }

    @SafeVarargs
    public final void createIndexWithDocs(Map<String, Object>... docs) {
        createIndex();
        indexDocs(docs);
    }

    @SafeVarargs
    public final void indexDocs(Map<String, Object>... docs) {
        Arrays.stream(docs).forEach(doc ->
                client().prepareIndex(getIndexName())
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .setSource(doc)
                        .get());
    }
}
