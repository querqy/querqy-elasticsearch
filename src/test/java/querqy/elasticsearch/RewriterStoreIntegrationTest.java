package querqy.elasticsearch;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.NodesReloadRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;
import querqy.elasticsearch.rewriterstore.PutRewriterResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.stringContainsInOrder;

@ESIntegTestCase.ClusterScope(scope = SUITE, numClientNodes = 1, maxNumDataNodes = 2)
public class RewriterStoreIntegrationTest extends ESIntegTestCase {

    private static String INDEX_NAME = "idx";


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
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

//        final NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().setPlugins(true).get();
//        final List<NodeInfo> nodes = response.getNodes();

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r1", payload1, null)).get();


        final Map<String, Object> payload2 = new HashMap<>();
        payload2.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config2 = new HashMap<>();
        config2.put("p1", false); // p1 as boolean
        payload2.put("config", config2);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r2", payload2, null)).get();


        final Map<String, Object> payload3 = new HashMap<>();
        payload3.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config3 = new HashMap<>();
        final Map<String, Object> p1 = new HashMap<>();
        p1.put("p1", "c3p1");

        config3.put("p1", p1); // p1 as object
        payload3.put("config", config3);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r3", payload3, null)).get();

    }

    public void index() {
        client().admin().indices().prepareCreate(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME, null)
                .setSource("field1", "a b", "field2", "a c")
                .get();
        client().prepareIndex(INDEX_NAME, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "b c")
                .get();
    }





}
