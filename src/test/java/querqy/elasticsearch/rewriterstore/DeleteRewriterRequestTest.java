package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class DeleteRewriterRequestTest {

    @Test
    public void testValidate() {
        final DeleteRewriterRequest invalidRequest = new DeleteRewriterRequest();
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);

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