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

public class RestPutRewriteChainAction extends BaseRestHandler {

    public RestPutRewriteChainAction(final Settings settings) {
        super(settings);
    }

    @Override
    public String getName() {
        return "Save a Querqy rewrite chain";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {

        String chainId = request.param("chainId");
        if (chainId == null) {
            throw new IllegalArgumentException("RestPutRewriteChainAction requires chainId parameter");
        }

        chainId = chainId.trim();
        if (chainId.isEmpty()) {
            throw new IllegalArgumentException("RestPutRewriteChainAction: chainId parameter must not be empty");
        }

        final String routing = request.param("routing");

        final Map<String, Object> source = XContentHelper
                .convertToMap(request.content(), false, XContentType.JSON).v2();

        final RestPutRewriteChainAction.PutRewriteChainRequestBuilder builder = new RestPutRewriteChainAction
                .PutRewriteChainRequestBuilder(client, PutRewriteChainAction.INSTANCE,
                new PutRewriteChainRequest(chainId, source, routing));


        return (channel) -> builder.execute(
                new RestStatusToXContentListener<>(channel, (r) -> r.getIndexResponse().getLocation(routing)));


    }

    public static class PutRewriteChainRequestBuilder
            extends ActionRequestBuilder<PutRewriteChainRequest, PutRewriteChainResponse> {

        public PutRewriteChainRequestBuilder(final ElasticsearchClient client, final PutRewriteChainAction action,
                                         final PutRewriteChainRequest request) {
            super(client, action, request);
        }
    }
}
