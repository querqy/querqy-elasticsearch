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

public class TransportNodesReloadRewriterAction extends TransportNodesAction<NodesReloadRewriterRequest,
        NodesReloadRewriterResponse, NodesReloadRewriterRequest.NodeRequest, NodesReloadRewriterResponse.NodeResponse> {

    protected RewriterShardContexts rewriterShardContexts;
    protected Client client;
    protected IndicesService indexServices;

    @Inject
    public TransportNodesReloadRewriterAction(final ThreadPool threadPool, final ClusterService clusterService,
                                              final TransportService transportService,
                                              final ActionFilters actionFilters,
                                              final IndicesService indexServices,
                                              final Client client,
                                              final RewriterShardContexts rewriterShardContexts) {

        super(NodesReloadRewriterAction.NAME, threadPool, clusterService, transportService, actionFilters,
                NodesReloadRewriterRequest::new, NodesReloadRewriterRequest.NodeRequest::new,
                threadPool.executor(ThreadPool.Names.MANAGEMENT));
        this.rewriterShardContexts = rewriterShardContexts;
        this.client = client;
        this.indexServices = indexServices;
    }

    @Override
    protected NodesReloadRewriterResponse newResponse(final NodesReloadRewriterRequest request,
                                                      final List<NodesReloadRewriterResponse.NodeResponse> nodeResponses,
                                                      final List<FailedNodeException> failures) {
        return new NodesReloadRewriterResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodesReloadRewriterRequest.NodeRequest newNodeRequest(final NodesReloadRewriterRequest request) {
        return request.newNodeRequest();
    }

    @Override
    protected NodesReloadRewriterResponse.NodeResponse newNodeResponse(final StreamInput in, final DiscoveryNode node)
            throws IOException {
        return new NodesReloadRewriterResponse.NodeResponse(in);
    }

    @Override
    protected NodesReloadRewriterResponse.NodeResponse nodeOperation(
            final NodesReloadRewriterRequest.NodeRequest request, final Task task) {
        try {
            rewriterShardContexts.reloadRewriter(request.getRewriterId());
            return new NodesReloadRewriterResponse.NodeResponse(clusterService.localNode(), null);
        } catch (final Exception e) {
            return new NodesReloadRewriterResponse.NodeResponse(clusterService.localNode(), e);
        }
    }


}
