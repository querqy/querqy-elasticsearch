package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class DeleteRewriterResponse extends ActionResponse implements StatusToXContentObject {

    private DeleteResponse deleteResponse;
    private NodesClearRewriterCacheResponse clearRewriterCacheResponse;


    protected DeleteRewriterResponse() {}

    public DeleteRewriterResponse(final DeleteResponse deleteResponse,
                                  final NodesClearRewriterCacheResponse clearRewriterCacheResponse) {
        this.deleteResponse = deleteResponse;
        this.clearRewriterCacheResponse = clearRewriterCacheResponse;
    }

    @Override
    public RestStatus status() {
        return deleteResponse.status();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();
        builder.field("delete", deleteResponse);
        builder.field("clearcache", clearRewriterCacheResponse);
        builder.endObject();
        return builder;
    }
}
