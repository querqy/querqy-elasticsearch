package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;

public class DeleteRewriterRequestTest {

    @Test(expected = ElasticsearchParseException.class)
    public void testNullRewriterIsNotAccepted() {
        new DeleteRewriterRequest((String) null);
    }

    @Test
    public void testValidate() {
        final DeleteRewriterRequest validRequest = new DeleteRewriterRequest("r27");
        assertNull(validRequest.validate());
    }

    @Test
    public void testStreamSerialization() throws IOException {
        final DeleteRewriterRequest request1 = new DeleteRewriterRequest("r31");
        final BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        output.flush();

        final DeleteRewriterRequest request2 = new DeleteRewriterRequest(output.bytes().streamInput());
        assertEquals(request1.getRewriterId(), request2.getRewriterId());
    }

}