package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodesReloadRewriterRequest extends BaseNodesRequest<NodesReloadRewriterRequest> {

    private String rewriterId;

    public NodesReloadRewriterRequest() {}

    public NodesReloadRewriterRequest(final String rewriterId, final String... nodesIds) {
        super(nodesIds);
        this.rewriterId = rewriterId;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rewriterId);
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        rewriterId = in.readString();
    }

    public String getRewriterId() {
        return rewriterId;
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
            rewriterId = in.readString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rewriterId);
        }

        public String getRewriterId() {
            return rewriterId;
        }

    }
}
