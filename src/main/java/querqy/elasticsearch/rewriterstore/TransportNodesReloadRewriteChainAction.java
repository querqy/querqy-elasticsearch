package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import querqy.elasticsearch.RewriterShardContexts;

import java.util.List;

public class TransportNodesReloadRewriteChainAction  extends TransportNodesAction<NodesReloadRewriteChainRequest,
        NodesReloadRewriteChainResponse, NodesReloadRewriteChainRequest.NodeRequest, NodesReloadRewriteChainResponse.NodeResponse> {

    protected RewriterShardContexts rewriterShardContexts;
    protected Client client;
    protected IndicesService indexServices;

    @Inject
    public TransportNodesReloadRewriteChainAction(final ThreadPool threadPool, final ClusterService clusterService,
                                              final TransportService transportService,
                                              final ActionFilters actionFilters,
                                              final IndicesService indexServices,
                                              final Client client,
                                              final RewriterShardContexts rewriterShardContexts) {

        super(NodesReloadRewriteChainAction.NAME, threadPool, clusterService, transportService, actionFilters,
                NodesReloadRewriteChainRequest::new, NodesReloadRewriteChainRequest.NodeRequest::new,
                ThreadPool.Names.MANAGEMENT, NodesReloadRewriteChainResponse.NodeResponse.class);
        this.rewriterShardContexts = rewriterShardContexts;
        this.client = client;
        this.indexServices = indexServices;
    }

    @Override
    protected NodesReloadRewriteChainResponse newResponse(final NodesReloadRewriteChainRequest request,
                                                      final List<NodesReloadRewriteChainResponse.NodeResponse> nodeResponses,
                                                      final List<FailedNodeException> failures) {
        return new NodesReloadRewriteChainResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected NodesReloadRewriteChainRequest.NodeRequest newNodeRequest(final String nodeId,
                                                                    final NodesReloadRewriteChainRequest request) {
        return new NodesReloadRewriteChainRequest.NodeRequest(nodeId, request.getChainId());
    }

    @Override
    protected NodesReloadRewriteChainResponse.NodeResponse newNodeResponse() {
        return new NodesReloadRewriteChainResponse.NodeResponse();
    }

    @Override
    protected NodesReloadRewriteChainResponse.NodeResponse nodeOperation(
            final NodesReloadRewriteChainRequest.NodeRequest request) {
        try {
            rewriterShardContexts.reloadRewriteChain(request.getChainId());
            //rewriters.reloadFactory(request.getRewriterId(), client, indexServices);
            return new NodesReloadRewriteChainResponse.NodeResponse(clusterService.localNode(), null);
        } catch (final Exception e) {
            return new NodesReloadRewriteChainResponse.NodeResponse(clusterService.localNode(), e);
        }
    }

}
