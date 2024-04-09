package querqy.elasticsearch.rewriterstore;

import static org.elasticsearch.common.transport.TransportAddress.META_ADDRESS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class PutRewriterResponseTest {

    @Test
    public void testThatStatusIsTakenFromIndexResponse() {
        final RestStatus status = RestStatus.CREATED;
        final IndexResponse indexResponse = mock(IndexResponse.class);
        when(indexResponse.status()).thenReturn(status);
        final NodesReloadRewriterResponse reloadRewriterResponse = mock(NodesReloadRewriterResponse.class);

        final PutRewriterResponse response = new PutRewriterResponse(indexResponse, reloadRewriterResponse);
        assertSame(status, response.status());
        verify(indexResponse, times(1)).status();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToJson() throws IOException {

        final IndexResponse indexResponse = new IndexResponse(new ShardId("idx1", "shard1", 1), "id1", 11, 2L, 8L,
                true);

        final DiscoveryNode node1 = new DiscoveryNode("name1", "d1", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("name2", "d2", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);

        final NodesReloadRewriterResponse reloadRewriterResponse = new NodesReloadRewriterResponse(
                new ClusterName("cluster27"), Arrays.asList(new NodesReloadRewriterResponse.NodeResponse(node1, null),
                        new NodesReloadRewriterResponse.NodeResponse(node2, null)), Collections.emptyList());

        final PutRewriterResponse response = new PutRewriterResponse(indexResponse, reloadRewriterResponse);

        final Map<String, Object> parsed;
        try (InputStream stream = XContentHelper.toXContent(response, XContentType.JSON, true).streamInput()) {
            parsed = XContentHelper.convertToMap(XContentFactory.xContent(XContentType.JSON), stream, false);
        }

        assertEquals(2, parsed.size());

        final Map<String, Object> reloaded = (Map<String, Object>) parsed.get("reloaded");
        assertNotNull(reloaded);

        final Map nodes = (Map) reloaded.get("nodes");
        assertThat((Map<String, String>) nodes.get("d1"), Matchers.hasEntry("name", "name1"));
        assertThat((Map<String, String>) nodes.get("d2"), Matchers.hasEntry("name", "name2"));

        final Map<String, Object> put = (Map<String, Object>) parsed.get("put");
        assertNotNull(put);
        assertEquals("created", put.get("result"));

    }

    @Test
    public void testStreamSerialization() throws IOException {

        final IndexResponse indexResponse = new IndexResponse(new ShardId("idx1", "shard1", 1), "id1", 11, 2L, 8L,
                true);

        indexResponse.setShardInfo(new ReplicationResponse.ShardInfo(4, 4));

        final DiscoveryNode node1 = new DiscoveryNode("name1", "d1", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("name2", "d2", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);

        final NodesReloadRewriterResponse reloadRewriterResponse = new NodesReloadRewriterResponse(
                new ClusterName("cluster27"), Arrays.asList(new NodesReloadRewriterResponse.NodeResponse(node1, null),
                new NodesReloadRewriterResponse.NodeResponse(node2, null)), Collections.emptyList());

        final PutRewriterResponse response1 = new PutRewriterResponse(indexResponse, reloadRewriterResponse);


        final BytesStreamOutput output = new BytesStreamOutput();
        response1.writeTo(output);
        output.flush();

        final PutRewriterResponse response2 = new PutRewriterResponse(output.bytes().streamInput());

        assertEquals(response1.status(), response2.status());

        final DocWriteResponse indexResponse1 = response1.getIndexResponse();
        final DocWriteResponse indexResponse2 = response2.getIndexResponse();
        assertEquals(indexResponse1.getShardId(), indexResponse2.getShardId());
        assertEquals(indexResponse1.getSeqNo(), indexResponse2.getSeqNo());

        final NodesReloadRewriterResponse reloadResponse1 = response1.getReloadResponse();
        final NodesReloadRewriterResponse reloadResponse2 = response2.getReloadResponse();
        assertEquals(reloadResponse1.getNodes(), reloadResponse2.getNodes());
    }
}