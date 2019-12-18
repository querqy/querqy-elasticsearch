package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.IndexShard;
import org.hamcrest.Matchers;
import org.junit.Test;
import querqy.elasticsearch.DummyESRewriterFactory;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PutRewriterRequestTest {

    @Test
    public void testValidateMissingRewriterId() {

        final PutRewriterRequest invalidRequest = new PutRewriterRequest();
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);
    }

    @Test
    public void testValidateMissingClassConfig() {

        final PutRewriterRequest invalidRequest = new PutRewriterRequest("r8", Collections.emptyMap());
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);

    }

    @Test
    public void testInvalidConfig() {

        final Map<String, Object> content = new HashMap<>();
        content.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config = new HashMap<>();
        config.put("error", "an error message");
        content.put("config", config);

        final PutRewriterRequest invalidRequest = new PutRewriterRequest("r8", content);
        final ActionRequestValidationException validationResult = invalidRequest.validate();
        assertNotNull(validationResult);
        assertThat(validationResult.validationErrors(), Matchers.contains("an error message"));

    }

    @Test
    public void testValidConfig() {

        final Map<String, Object> content = new HashMap<>();
        content.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config = new HashMap<>();
        content.put("config", config);

        final PutRewriterRequest validRequest = new PutRewriterRequest("r8", content);
        final ActionRequestValidationException validationResult = validRequest.validate();
        assertNull(validationResult);

    }

    @Test
    public void testStreamSerialization() throws IOException {

        final Map<String, Object> content = new HashMap<>();
        content.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config = new HashMap<>();
        config.put("prop1", "Some value");
        content.put("config", config);

        final PutRewriterRequest request1 = new PutRewriterRequest("r8", content);

        final BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        output.flush();

        final PutRewriterRequest request2 = new PutRewriterRequest();
        request2.readFrom(output.bytes().streamInput());

        assertEquals(request1.getRewriterId(), request2.getRewriterId());
        assertEquals(request1.getContent(), request2.getContent());

    }

}