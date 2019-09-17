package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodesReloadRewriteChainRequest extends BaseNodesRequest<NodesReloadRewriteChainRequest> {

    private String chainId;

    public NodesReloadRewriteChainRequest() {}

    public NodesReloadRewriteChainRequest(final String chainId, final String... nodesIds) {
        super(nodesIds);
        this.chainId = chainId;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(chainId);
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        chainId = in.readString();
    }

    public String getChainId() {
        return chainId;
    }

    public static class NodeRequest extends BaseNodeRequest {

        String chainId;

        public NodeRequest() {
        }

        public NodeRequest(final String nodeId, final String chainId) {
            super(nodeId);
            this.chainId = chainId;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            chainId = in.readString();
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(chainId);
        }

        public String getChainId() {
            return chainId;
        }

    }

}
