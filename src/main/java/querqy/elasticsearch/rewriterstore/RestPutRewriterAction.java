package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestPutRewriterAction extends BaseRestHandler {

    public static final String PARAM_REWRITER_ID = "rewriterId";

    @Override
    public String getName() {
        return "Save a Querqy rewriter";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(RestRequest.Method.PUT, "/_querqy/rewriter/{rewriterId}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {

        final PutRewriterRequestBuilder requestBuilder = createRequestBuilder(request, client);

        return (channel) -> requestBuilder.execute(
                new RestToXContentListener<PutRewriterResponse>(channel));
    }

    PutRewriterRequestBuilder createRequestBuilder(final RestRequest request, final NodeClient client) {
        String rewriterId = request.param(PARAM_REWRITER_ID);
        if (rewriterId == null) {
            throw new IllegalArgumentException("RestPutRewriterAction requires rewriterId parameter");
        }

        rewriterId = rewriterId.trim();
        if (rewriterId.isEmpty()) {
            throw new IllegalArgumentException("RestPutRewriterAction: rewriterId parameter must not be empty");
        }


        final Map<String, Object> source = XContentHelper
                .convertToMap(request.content(), false, XContentType.JSON).v2();

        return new PutRewriterRequestBuilder(client, PutRewriterAction.INSTANCE,
                new PutRewriterRequest(rewriterId, source));
    }


    public static class PutRewriterRequestBuilder
            extends ActionRequestBuilder<PutRewriterRequest, PutRewriterResponse> {

        public PutRewriterRequestBuilder(final ElasticsearchClient client, final PutRewriterAction action,
                                         final PutRewriterRequest request) {
            super(client, action, request);
        }
    }
}
