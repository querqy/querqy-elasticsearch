package querqy.elasticsearch;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = SUITE, numClientNodes = 2, maxNumDataNodes = 2, supportsDedicatedMasters = false)
public class RewriterStoreIntegrationTest extends ESIntegTestCase {

    protected final static int HTTP_TEST_PORT = 9400;

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


}
