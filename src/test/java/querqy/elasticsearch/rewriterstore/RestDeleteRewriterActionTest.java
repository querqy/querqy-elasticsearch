package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static querqy.elasticsearch.rewriterstore.RestDeleteRewriterAction.PARAM_REWRITER_ID;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RestDeleteRewriterActionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testThatNullRewriterIdIsRejected() {

        final NodeClient client = mock(NodeClient.class);
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(Collections.emptyMap()).build();

        new RestDeleteRewriterAction().prepareRequest(restRequest, client);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatEmptyRewriterIdIsRejected() {

        final NodeClient client = mock(NodeClient.class);
        final Map<String, String> params = new HashMap<>();
        params.put(PARAM_REWRITER_ID, " ");
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(params).build();

        new RestDeleteRewriterAction().prepareRequest(restRequest, client);

    }

    @Test
    public void testThatRequestIsParsed() {

        final NodeClient client = mock(NodeClient.class);

        final Map<String, String> params = new HashMap<>();
        params.put(RestPutRewriterAction.PARAM_REWRITER_ID, "rewriter11");

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(params)
                .build();
        final RestDeleteRewriterAction.DeleteRewriterRequestBuilder requestBuilder
                = new RestDeleteRewriterAction().createRequestBuilder(restRequest, client);

        final DeleteRewriterRequest deleteRewriterRequest = requestBuilder.request();
        assertNotNull(deleteRewriterRequest);

        assertEquals("rewriter11", deleteRewriterRequest.getRewriterId());

    }


}