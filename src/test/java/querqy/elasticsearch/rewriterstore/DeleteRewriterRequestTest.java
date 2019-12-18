package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

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
}