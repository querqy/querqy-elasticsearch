package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.action.ActionRequestValidationException;
import org.junit.Test;

public class DeleteRewriterRequestTest {

    @Test
    public void testValidate() {

        final DeleteRewriterRequest invalidRequest = new DeleteRewriterRequest();
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);

        final DeleteRewriterRequest validRequest = new DeleteRewriterRequest("r27", null);
        assertNull(validRequest.validate());

    }
}