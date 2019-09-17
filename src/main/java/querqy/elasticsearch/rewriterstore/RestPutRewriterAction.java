package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.util.Map;

public class RestPutRewriterAction extends BaseRestHandler {

    public RestPutRewriterAction(final Settings settings) {
        super(settings);
    }

    @Override
    public String getName() {
        return "Save a Querqy rewriter";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {

        String rewriterId = request.param("rewriterId");
        if (rewriterId == null) {
            throw new IllegalArgumentException("RestPutRewriterAction requires rewriterId parameter");
        }

        rewriterId = rewriterId.trim();
        if (rewriterId.isEmpty()) {
            throw new IllegalArgumentException("RestPutRewriterAction: rewriterId parameter must not be empty");
        }

        final String routing = request.param("routing");

        final Map<String, Object> source = XContentHelper
                .convertToMap(request.content(), false, XContentType.JSON).v2();

        final PutRewriterRequestBuilder builder = new PutRewriterRequestBuilder(client, PutRewriterAction.INSTANCE,
                new PutRewriterRequest(rewriterId, source, routing));


        return (channel) -> builder.execute(
                new RestStatusToXContentListener<>(channel, (r) -> r.getIndexResponse().getLocation(routing)));


    }

    public static class PutRewriterRequestBuilder
            extends ActionRequestBuilder<PutRewriterRequest, PutRewriterResponse> {

        public PutRewriterRequestBuilder(final ElasticsearchClient client, final PutRewriterAction action,
                                         final PutRewriterRequest request) {
            super(client, action, request);
        }
    }
}
