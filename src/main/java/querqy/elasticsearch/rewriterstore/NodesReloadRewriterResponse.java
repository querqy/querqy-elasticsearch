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
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

public class NodesReloadRewriterResponse extends BaseNodesResponse<NodesReloadRewriterResponse.NodeResponse>
        implements ToXContentObject {


    public NodesReloadRewriterResponse() { }

    public NodesReloadRewriterResponse(final ClusterName clusterName,
                                       final List<NodesReloadRewriterResponse.NodeResponse> responses,
                                       final List<FailedNodeException> failures) {
        super(clusterName, responses, failures);
    }

    public NodesReloadRewriterResponse(final StreamInput in) throws IOException {
        super();
        readFrom(in);
    }

    @Override
    protected List<NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(NodesReloadRewriterResponse.NodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<NodeResponse> nodes) throws IOException {
        out.writeStreamableList(nodes);
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

        private Exception reloadException = null;

        public NodeResponse() {
        }

        public NodeResponse(final StreamInput in) throws IOException {
            super();
            readFrom(in);
        }

        public NodeResponse(final DiscoveryNode node, Exception reloadException) {
            super(node);
            this.reloadException = reloadException;
        }

        public Exception reloadException() {
            return this.reloadException;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            if (in.readBoolean()) {
                reloadException = in.readException();
            }
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
            return reloadException != null ? reloadException.equals(that.reloadException) : that.reloadException == null;
        }

        @Override
        public int hashCode() {
            return reloadException != null ? reloadException.hashCode() : 0;
        }

        static NodesReloadRewriterResponse.NodeResponse readNodeResponse(final StreamInput in) throws IOException {
            return new NodesReloadRewriterResponse.NodeResponse(in);
        }
    }
}
