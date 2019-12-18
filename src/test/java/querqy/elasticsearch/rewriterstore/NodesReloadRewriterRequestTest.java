package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;

public class NodesReloadRewriterRequestTest {

    @Test
    public void testStreamSerialization() throws IOException {

        final NodesReloadRewriterRequest request1 = new NodesReloadRewriterRequest("r1", "n1", "n2");
        final BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        output.flush();

        final NodesReloadRewriterRequest request2 = new NodesReloadRewriterRequest();
        request2.readFrom(output.bytes().streamInput());
        assertEquals("r1", request1.getRewriterId());
        assertEquals("r1", request2.getRewriterId());

    }
}