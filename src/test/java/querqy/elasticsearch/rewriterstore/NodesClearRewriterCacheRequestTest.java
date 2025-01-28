package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class NodesClearRewriterCacheRequestTest {
    @Test
    public void testNodeRequestCreationWithRewriterId() {
        final NodesClearRewriterCacheRequest request = new NodesClearRewriterCacheRequest("r1", "n1", "n2");
        final NodesClearRewriterCacheRequest.NodeRequest nodeRequest = request.newNodeRequest();
        assertEquals(Optional.of("r1"), nodeRequest.getRewriterId());
    }

    @Test
    public void testNodeRequestCreationWithoutRewriterId() {
        final NodesClearRewriterCacheRequest request = new NodesClearRewriterCacheRequest(null, "n1", "n2");
        final NodesClearRewriterCacheRequest.NodeRequest nodeRequest = request.newNodeRequest();
        assertFalse(nodeRequest.getRewriterId().isPresent());
    }

    @Test
    public void testNodeRequestSerializationWithRewriterId() throws IOException {

        final NodesClearRewriterCacheRequest.NodeRequest nodeRequest1 = new NodesClearRewriterCacheRequest
                .NodeRequest("r11");

        final BytesStreamOutput output = new BytesStreamOutput();
        nodeRequest1.writeTo(output);
        output.flush();

        final NodesClearRewriterCacheRequest.NodeRequest nodeRequest2 = new NodesClearRewriterCacheRequest
                .NodeRequest(output.bytes().streamInput());

        assertEquals(nodeRequest1.getRewriterId(), nodeRequest2.getRewriterId());

    }

    @Test
    public void testNodeRequestSerializationWithoutRewriterId() throws IOException {

        final NodesClearRewriterCacheRequest.NodeRequest nodeRequest1 = new NodesClearRewriterCacheRequest
                .NodeRequest();

        final BytesStreamOutput output = new BytesStreamOutput();
        nodeRequest1.writeTo(output);
        output.flush();

        final NodesClearRewriterCacheRequest.NodeRequest nodeRequest2 = new NodesClearRewriterCacheRequest
                .NodeRequest(output.bytes().streamInput());

        assertEquals(nodeRequest1.getRewriterId(), nodeRequest2.getRewriterId());
        assertFalse(nodeRequest2.getRewriterId().isPresent());

    }

}