package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Optional;

public class NodesClearRewriterCacheRequest extends BaseNodesRequest<NodesClearRewriterCacheRequest> {

    private String rewriterId;

    public NodesClearRewriterCacheRequest() {}

    public NodesClearRewriterCacheRequest(final String rewriterId, final String... nodesIds) {
        super(nodesIds);
        this.rewriterId = rewriterId;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(rewriterId);
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        rewriterId = in.readOptionalString();
    }

    public Optional<String> getRewriterId() {
        return Optional.ofNullable(rewriterId);
    }

    public NodeRequest newNodeRequest(final String nodeId) {
        return new NodeRequest(nodeId, rewriterId);
    }

    public static class NodeRequest extends BaseNodeRequest {

        String rewriterId;

        public NodeRequest() {
        }

        public NodeRequest(final String nodeId, final String rewriterId) {
            super(nodeId);
            this.rewriterId = rewriterId;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            rewriterId = in.readOptionalString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(rewriterId);
        }

        public Optional<String> getRewriterId() {
            return Optional.ofNullable(rewriterId);
        }

    }
}
