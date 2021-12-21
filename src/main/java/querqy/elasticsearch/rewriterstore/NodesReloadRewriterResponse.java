package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NodesReloadRewriterResponse extends BaseNodesResponse<NodesReloadRewriterResponse.NodeResponse>
        implements ToXContentObject {


    public NodesReloadRewriterResponse(final ClusterName clusterName,
                                       final List<NodesReloadRewriterResponse.NodeResponse> responses,
                                       final List<FailedNodeException> failures) {
        super(clusterName, responses, failures);
    }

    public NodesReloadRewriterResponse(final StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(NodesReloadRewriterResponse.NodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<NodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startObject("nodes");
        for (final NodeResponse node : getNodes()) {
            builder.startObject(node.getNode().getId());
            builder.field("name", node.getNode().getName());
            final Exception e = node.reloadException();
            if (e != null) {
                builder.startObject("reload_exception");
                ElasticsearchException.generateThrowableXContent(builder, params, e);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof NodesReloadRewriterResponse)) {
            return false;
        }
        final NodesReloadRewriterResponse other = (NodesReloadRewriterResponse) obj;
        // We only count failures as they don't implement equals():
        final List<FailedNodeException> thisFailures = failures();
        final List<FailedNodeException> thatFailures = other.failures();
        if (thisFailures == null && thatFailures != null) {
            return false;
        }
        if (thisFailures != null) {
            if (thatFailures == null) {
                return false;
            }
            if (thisFailures.size() != thatFailures.size()) {
                return false;
            }
        }

        return Objects.equals(getClusterName(), other.getClusterName())
                && Objects.equals(getNodes(), other.getNodes());
    }

    @Override
    public String toString() {
        try {
            final XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (final IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final Exception reloadException;

        public NodeResponse(final StreamInput in) throws IOException {
            super(in);
            reloadException = in.readBoolean() ? in.readException() : null;
        }

        public NodeResponse(final DiscoveryNode node, final Exception reloadException) {
            super(node);
            this.reloadException = reloadException;
        }

        public Exception reloadException() {
            return this.reloadException;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            if (reloadException != null) {
                out.writeBoolean(true);
                out.writeException(reloadException);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final NodesReloadRewriterResponse.NodeResponse that = (NodesReloadRewriterResponse.NodeResponse) o;
            // We cannot rely on the Exception to implement equals(), users of NodesReloadRewriterResponse will
            // be interested just in the message anyway
            if (reloadException == null) {
                return that.reloadException == null;
            } else if (that.reloadException == null) {
                return false;
            }
            return Objects.equals(reloadException.getMessage(), that.reloadException.getMessage());
        }

        @Override
        public int hashCode() {
            return reloadException != null && reloadException.getMessage() != null
                    ? reloadException.getMessage().hashCode() : 0;
        }

        static NodesReloadRewriterResponse.NodeResponse readNodeResponse(final StreamInput in) throws IOException {
            return new NodesReloadRewriterResponse.NodeResponse(in);
        }
    }
}
