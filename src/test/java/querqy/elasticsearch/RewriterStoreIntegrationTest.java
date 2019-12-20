package querqy.elasticsearch;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheAction;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheRequest;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheResponse;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.greaterThan;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;

@ESIntegTestCase.ClusterScope(scope = SUITE, numClientNodes = 1, minNumDataNodes = 4, maxNumDataNodes = 6)
public class RewriterStoreIntegrationTest extends ESIntegTestCase {

    private static final int NUM_DOT_QUERY_REPLICAS = 2 + new Random().nextInt(4);


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {

        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, NUM_DOT_QUERY_REPLICAS)
                .build();
    }

    //@Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testPluginIsLoaded() {

        final NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugins(true).get();
        final List<NodeInfo> nodes = response.getNodes();

        assertThat(nodes.size(), greaterThan(0));

        for (final NodeInfo nodeInfo : nodes) {
            assertTrue(nodeInfo
                    .getPlugins()
                    .getPluginInfos()
                    .stream()
                    .anyMatch(info -> info.getName().equals(QuerqyPlugin.class.getName())));


        }
    }


    public void testThatRewriterConfigCanUseDifferentTypeForSamePropertyName() throws Exception {

        final Map<String, Object> payload1 = new HashMap<>();
        payload1.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config1 = new HashMap<>();
        config1.put("p1", 1L); // p1 as long
        payload1.put("config", config1);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r1", payload1)).get();


        final Map<String, Object> payload2 = new HashMap<>();
        payload2.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config2 = new HashMap<>();
        config2.put("p1", false); // p1 as boolean
        payload2.put("config", config2);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r2", payload2)).get();


        final Map<String, Object> payload3 = new HashMap<>();
        payload3.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config3 = new HashMap<>();
        final Map<String, Object> p1 = new HashMap<>();
        p1.put("p1", "c3p1");

        config3.put("p1", p1); // p1 as object
        payload3.put("config", config3);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r3", payload3)).get();

    }

    public void testThatReplicaSettingForDotQuerqyIndexIsApplied() throws Exception {
        final Map<String, Object> payload1 = new HashMap<>();
        payload1.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config1 = new HashMap<>();
        config1.put("p1", 1L);
        payload1.put("config", config1);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r1", payload1)).get();

        final GetSettingsResponse idxSettings = client().admin().indices().prepareGetSettings(".querqy").get();

        assertNotNull(idxSettings);
        assertEquals(NUM_DOT_QUERY_REPLICAS,
                Integer.parseInt(idxSettings.getIndexToSettings().get(".querqy").get("index.number_of_replicas")));
    }


    public void index() {
        final String indexName = "idx";
        client().admin().indices().prepareCreate(indexName).get();
        client().prepareIndex(indexName, null)
                .setSource("field1", "a b", "field2", "a c")
                .get();
        client().prepareIndex(indexName, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "b c")
                .get();
    }





}
