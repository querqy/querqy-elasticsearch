package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class NodesReloadRewriterRequest extends BaseNodesRequest {

    private final String rewriterId;

    public NodesReloadRewriterRequest(final String rewriterId, final String... nodesIds) {
        super(nodesIds);
        this.rewriterId = rewriterId;
    }

    public NodeRequest newNodeRequest() {
        return new NodeRequest(rewriterId);
    }

    public String getRewriterId() {
        return rewriterId;
    }


    public static class NodeRequest extends TransportRequest {

        String rewriterId;

        public NodeRequest(final StreamInput in) throws IOException {
            super(in);
            rewriterId = in.readString();
        }

        public NodeRequest(final String rewriterId) {
            super();
            this.rewriterId = rewriterId;
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
