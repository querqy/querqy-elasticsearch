package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class DeleteRewriterResponse extends ActionResponse implements ToXContentObject {

    private DeleteResponse deleteResponse;
    private NodesClearRewriterCacheResponse clearRewriterCacheResponse;


    public DeleteRewriterResponse(final StreamInput in) throws IOException {
        deleteResponse = new DeleteResponse(in);
        clearRewriterCacheResponse = new NodesClearRewriterCacheResponse(in);
    }

    public DeleteRewriterResponse(final DeleteResponse deleteResponse,
                                  final NodesClearRewriterCacheResponse clearRewriterCacheResponse) {
        this.deleteResponse = deleteResponse;
        this.clearRewriterCacheResponse = clearRewriterCacheResponse;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        deleteResponse.writeTo(out);
        clearRewriterCacheResponse.writeTo(out);
    }

    public RestStatus status() {
        return deleteResponse.status();
    }

    public DeleteResponse getDeleteResponse() {
        return deleteResponse;
    }

    public NodesClearRewriterCacheResponse getClearRewriterCacheResponse() {
        return clearRewriterCacheResponse;
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
