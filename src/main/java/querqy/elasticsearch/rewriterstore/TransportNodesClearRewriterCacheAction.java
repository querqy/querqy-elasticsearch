package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import querqy.elasticsearch.RewriterShardContexts;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class TransportNodesClearRewriterCacheAction extends TransportNodesAction<NodesClearRewriterCacheRequest,
        NodesClearRewriterCacheResponse, NodesClearRewriterCacheRequest.NodeRequest, NodesClearRewriterCacheResponse.NodeResponse> {

    protected RewriterShardContexts rewriterShardContexts;

    @Inject
    public TransportNodesClearRewriterCacheAction(final ThreadPool threadPool, final ClusterService clusterService,
                                              final TransportService transportService,
                                              final ActionFilters actionFilters,
                                              final IndicesService indexServices,
                                              final Client client,
                                              final RewriterShardContexts rewriterShardContexts) {

		super(
			NodesClearRewriterCacheAction.NAME,
			clusterService,
			transportService,
			actionFilters,
			NodesClearRewriterCacheRequest.NodeRequest::new,
			threadPool.executor(ThreadPool.Names.MANAGEMENT));
		this.rewriterShardContexts = rewriterShardContexts;
    }


    @Override
    protected NodesClearRewriterCacheResponse newResponse(final NodesClearRewriterCacheRequest request,
                                                          final List<NodesClearRewriterCacheResponse.NodeResponse>
                                                                  nodeResponses,
                                                          final List<FailedNodeException> failures) {
        return new NodesClearRewriterCacheResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodesClearRewriterCacheRequest.NodeRequest newNodeRequest(final NodesClearRewriterCacheRequest request) {
        return request.newNodeRequest();
    }

    @Override
    protected NodesClearRewriterCacheResponse.NodeResponse newNodeResponse(final StreamInput in,
                                                                           final DiscoveryNode discoveryNode)
            throws IOException {
        return new NodesClearRewriterCacheResponse.NodeResponse(in);
    }

    @Override
    protected NodesClearRewriterCacheResponse.NodeResponse nodeOperation(
            final NodesClearRewriterCacheRequest.NodeRequest request, final Task task) {

        final Optional<String> rewriterId = request.getRewriterId();
        if (rewriterId.isPresent()) {
            rewriterId.ifPresent(rewriterShardContexts::clearRewriter);
        } else {
            rewriterShardContexts.clearRewriters();
        }

        return new NodesClearRewriterCacheResponse.NodeResponse(clusterService.localNode());


    }
}
