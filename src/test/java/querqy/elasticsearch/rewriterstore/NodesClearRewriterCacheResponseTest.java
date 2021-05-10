package querqy.elasticsearch.rewriterstore;

import static org.elasticsearch.common.transport.TransportAddress.META_ADDRESS;
import static org.junit.Assert.*;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class NodesClearRewriterCacheResponseTest {

    @SuppressWarnings("unchecked")
    @Test
    public void testJsonSerialization() throws Exception {

        final DiscoveryNode node1 = new DiscoveryNode("name1", "d1", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("name2", "d2", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);


        final NodesClearRewriterCacheResponse response = new NodesClearRewriterCacheResponse
                (new ClusterName("cluster27"),
                        Arrays.asList(new NodesClearRewriterCacheResponse.NodeResponse(node1),
                        new NodesClearRewriterCacheResponse.NodeResponse(node2)), Collections.emptyList());

        final Map<String, Object> parsed;
        try (InputStream stream = XContentHelper.toXContent(response, XContentType.JSON, true).streamInput()) {
            parsed = XContentHelper.convertToMap(XContentFactory.xContent(XContentType.JSON), stream, false);
        }

        final Map nodes = (Map) parsed.get("nodes");
        assertThat((Map<String, String>) nodes.get("d1"), Matchers.hasEntry("name", "name1"));
        assertThat((Map<String, String>) nodes.get("d2"), Matchers.hasEntry("name", "name2"));

    }

    @Test
    public void testStreamSerialization() throws IOException {
        final DiscoveryNode node1 = new DiscoveryNode("name1", "d1", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("name2", "d2", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);


        final NodesClearRewriterCacheResponse response1 = new NodesClearRewriterCacheResponse(
                new ClusterName("cluster27"),
                Arrays.asList(new NodesClearRewriterCacheResponse.NodeResponse(node1),
                        new NodesClearRewriterCacheResponse.NodeResponse(node2)), Collections.emptyList());

        final BytesStreamOutput output = new BytesStreamOutput();
        response1.writeTo(output);
        output.flush();


        final NodesClearRewriterCacheResponse response2 = new NodesClearRewriterCacheResponse(output.bytes()
                .streamInput());

        assertEquals(response1.getNodes(), response2.getNodes());


    }

}