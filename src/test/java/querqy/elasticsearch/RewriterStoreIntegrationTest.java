package querqy.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

@ESIntegTestCase.ClusterScope(scope = SUITE, numClientNodes = 2, maxNumDataNodes = 2, supportsDedicatedMasters = false)
public class RewriterStoreIntegrationTest extends ESIntegTestCase {

    protected final static int HTTP_TEST_PORT = 9400;
//    protected static Client client;

//    @BeforeClass
//    public static void startRestClient() {
//
//        client = super.client();//RestClient.builder(new HttpHost("localhost", HTTP_TEST_PORT)).build();
//        try {
//            Response response = client.prepareGet()..performRequest(new Request("GET", "/"));
//            System.out.println("####" + response);
//            Map<String, Object> responseMap = entityAsMap(response);
//            assertThat(responseMap, hasEntry("tagline", "You Know, for Search"));
////            staticLogger.info("Integration tests ready to start... Cluster is running.");
//        } catch (IOException e) {
//            e.printStackTrace();
////            // If we have an exception here, let's ignore the test
////            assumeThat("Integration tests are skipped", e.getMessage(), not(containsString("Connection refused")));
////            staticLogger.error("Full error is", e);
////            fail("Something wrong is happening. REST Client seemed to raise an exception.");
//        }
//    }
//
//    @AfterClass
//    public static void stopRestClient() throws IOException {
//        if (client != null) {
//            clientclose();
//            client = null;
//        }
//    }



    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(QuerqyPlugin.class);
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

//    public void testList() throws IOException {
//
//
//        Response response = client()..performRequest(new Request("GET", "/"));///_querqy/rewriter/babe"));
//        System.out.println(response);
//
//        //assertThat(entityAsMap(response), hasEntry("message", "Hello World!"));
//
//    }



}
