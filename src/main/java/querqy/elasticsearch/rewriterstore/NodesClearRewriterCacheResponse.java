package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class NodesClearRewriterCacheResponse extends BaseNodesResponse<NodesClearRewriterCacheResponse.NodeResponse>
        implements ToXContentObject {


    public NodesClearRewriterCacheResponse() { }

    public NodesClearRewriterCacheResponse(final ClusterName clusterName,
                                       final List<NodesClearRewriterCacheResponse.NodeResponse> responses,
                                       final List<FailedNodeException> failures) {
        super(clusterName, responses, failures);
    }

    public NodesClearRewriterCacheResponse(final StreamInput in) throws IOException {
        super();
        readFrom(in);
    }


    @Override
    protected List<NodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(NodesClearRewriterCacheResponse.NodeResponse::readNodeResponse);
    }

    @Override
    protected void writeNodesTo(final StreamOutput out, final List<NodeResponse> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startObject("nodes");
        for (final NodesClearRewriterCacheResponse.NodeResponse node : getNodes()) {
            builder.startObject(node.getNode().getId());
            node.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }



    public static class NodeResponse extends BaseNodeResponse
            implements ToXContentObject {

        public NodeResponse() {
        }

        public NodeResponse(final StreamInput in) throws IOException {
            super();
            readFrom(in);
        }

        public NodeResponse(final DiscoveryNode node) {
            super(node);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return Objects.equals(getNode(), ((NodesClearRewriterCacheResponse.NodeResponse) o).getNode());

        }


        static NodesClearRewriterCacheResponse.NodeResponse readNodeResponse(final StreamInput in) throws IOException {
            return new NodesClearRewriterCacheResponse.NodeResponse(in);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return builder.field("name", getNode().getName());
        }

        @Override
        public boolean isFragment() {
            return true;
        }
    }
}
