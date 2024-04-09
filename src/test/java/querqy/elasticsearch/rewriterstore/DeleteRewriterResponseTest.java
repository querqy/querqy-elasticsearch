package querqy.elasticsearch.rewriterstore;

import static org.elasticsearch.common.transport.TransportAddress.META_ADDRESS;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.elasticsearch.Version;
import org.elasticsearch.action.delete.DeleteResponse;
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
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class DeleteRewriterResponseTest {

    @Test
    public void testThatStatusIsTakenFromDeleteResponse() {
        final RestStatus status = RestStatus.NOT_FOUND;
        final DeleteResponse deleteResponse = mock(DeleteResponse.class);
        Mockito.when(deleteResponse.status()).thenReturn(status);
        final NodesClearRewriterCacheResponse clearRewriterCacheResponse = mock(NodesClearRewriterCacheResponse.class);

        final DeleteRewriterResponse response = new DeleteRewriterResponse(deleteResponse, clearRewriterCacheResponse);
        assertSame(status, response.status());
        verify(deleteResponse, times(1)).status();
    }

    @Test
    public void testStreamSerialization() throws IOException {

        final DiscoveryNode node1 = new DiscoveryNode("name1", "d1", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("name2", "d2", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);

        final DeleteResponse deleteResponse1 = new DeleteResponse(new ShardId("idx1", "shard1", 1), "id1",
                11, 2L, 8L, true);
        deleteResponse1.setShardInfo(new ReplicationResponse.ShardInfo(2, 1));

        final NodesClearRewriterCacheResponse clearRewriterCacheResponse1 = new NodesClearRewriterCacheResponse
                (new ClusterName("cluster27"),
                        Arrays.asList(new NodesClearRewriterCacheResponse.NodeResponse(node1),
                                new NodesClearRewriterCacheResponse.NodeResponse(node2)), Collections.emptyList());

        final DeleteRewriterResponse response1 = new DeleteRewriterResponse(deleteResponse1,
                clearRewriterCacheResponse1);

        final BytesStreamOutput output = new BytesStreamOutput();
        response1.writeTo(output);
        output.flush();

        final DeleteRewriterResponse response2 = new DeleteRewriterResponse(output.bytes().streamInput());
        assertEquals(response1.status(), response2.status());

        final DeleteResponse deleteResponse2 = response2.getDeleteResponse();
        assertEquals(deleteResponse1.status(), deleteResponse2.status());
        assertEquals(deleteResponse1.getShardId(), deleteResponse2.getShardId());

        final NodesClearRewriterCacheResponse clearRewriterCacheResponse2 = response2.getClearRewriterCacheResponse();
        assertEquals(clearRewriterCacheResponse1.getNodes(), clearRewriterCacheResponse2.getNodes());

    }


    @SuppressWarnings("unchecked")
    @Test
    public void testToJson() throws IOException {

        final DiscoveryNode node1 = new DiscoveryNode("name1", "d1", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);
        final DiscoveryNode node2 = new DiscoveryNode("name2", "d2", new TransportAddress(META_ADDRESS, 0),
                Collections.emptyMap(), Collections.emptySet(), VersionInformation.CURRENT);

        final DeleteResponse deleteResponse = new DeleteResponse(new ShardId("idx1", "shard1", 1), "id1", 11,
                2L, 8L, true);
        final NodesClearRewriterCacheResponse clearRewriterCacheResponse = new NodesClearRewriterCacheResponse
                (new ClusterName("cluster27"),
                        Arrays.asList(new NodesClearRewriterCacheResponse.NodeResponse(node1),
                                new NodesClearRewriterCacheResponse.NodeResponse(node2)), Collections.emptyList());

        final DeleteRewriterResponse response = new DeleteRewriterResponse(deleteResponse, clearRewriterCacheResponse);

        final Map<String, Object> parsed;
        try (InputStream stream = XContentHelper.toXContent(response, XContentType.JSON, true).streamInput()) {
            parsed = XContentHelper.convertToMap(XContentFactory.xContent(XContentType.JSON), stream, false);
        }

        assertEquals(2, parsed.size());

        final Map<String, Object> clearcacheResult = (Map<String, Object>) parsed.get("clearcache");
        assertNotNull(clearcacheResult);

        final Map nodes = (Map) clearcacheResult.get("nodes");
        assertThat((Map<String, String>) nodes.get("d1"), Matchers.hasEntry("name", "name1"));
        assertThat((Map<String, String>) nodes.get("d2"), Matchers.hasEntry("name", "name2"));

        final Map<String, Object> delete =  (Map<String, Object>) parsed.get("delete");
        assertNotNull(delete);
        assertEquals("deleted", delete.get("result"));

    }

}