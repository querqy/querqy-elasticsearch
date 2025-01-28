package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class PutRewriterResponse extends ActionResponse implements ToXContentObject {

    private final DocWriteResponse indexResponse;
    private final NodesReloadRewriterResponse reloadResponse;

    public PutRewriterResponse(final DocWriteResponse indexResponse, final NodesReloadRewriterResponse reloadResponse) {
        this.indexResponse = indexResponse;
        this.reloadResponse = reloadResponse;
    }

    public PutRewriterResponse(final StreamInput in) throws IOException {
        super(in);
        indexResponse = new IndexResponse(in);
        reloadResponse = new NodesReloadRewriterResponse(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        indexResponse.writeTo(out);
        reloadResponse.writeTo(out);
    }

    public RestStatus status() {
        return indexResponse.status();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();
        builder.field("put", indexResponse);
        builder.field("reloaded", reloadResponse);
        builder.endObject();
        return builder;
    }

    public DocWriteResponse getIndexResponse() {
        return indexResponse;
    }

    public NodesReloadRewriterResponse getReloadResponse() {
        return reloadResponse;
    }
}
