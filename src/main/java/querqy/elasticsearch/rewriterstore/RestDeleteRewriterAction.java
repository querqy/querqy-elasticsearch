package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;

public class RestDeleteRewriterAction extends BaseRestHandler {

    public RestDeleteRewriterAction(final Settings settings) {
        super(settings);
    }

    @Override
    public String getName() {
        return "Delete a Querqy rewriter";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client)
            throws IOException {

        String rewriterId = request.param("rewriterId");
        if (rewriterId == null) {
            throw new IllegalArgumentException("RestDeleteRewriterAction requires rewriterId parameter");
        }

        rewriterId = rewriterId.trim();
        if (rewriterId.isEmpty()) {
            throw new IllegalArgumentException("RestDeleteRewriterAction: rewriterId parameter must not be empty");
        }

        final String routing = request.param("routing");

        final RestDeleteRewriterAction.DeleteRewriterRequestBuilder builder = new RestDeleteRewriterAction
                .DeleteRewriterRequestBuilder(client, DeleteRewriterAction.INSTANCE,
                new DeleteRewriterRequest(rewriterId, routing));


        return (channel) -> builder.execute(
                new RestStatusToXContentListener<>(channel));
    }

    public static class DeleteRewriterRequestBuilder
            extends ActionRequestBuilder<DeleteRewriterRequest, DeleteRewriterResponse> {

        public DeleteRewriterRequestBuilder(final ElasticsearchClient client, final DeleteRewriterAction action,
                                         final DeleteRewriterRequest request) {
            super(client, action, request);
        }
    }
}
