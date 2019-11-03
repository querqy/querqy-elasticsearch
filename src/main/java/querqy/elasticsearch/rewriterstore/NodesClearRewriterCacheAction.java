package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.Action;

/**
 * Remove a rewriter from cache
 */
public class NodesClearRewriterCacheAction extends Action<NodesClearRewriterCacheResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/_clearcache";
    public static final NodesClearRewriterCacheAction INSTANCE = new NodesClearRewriterCacheAction();


    public NodesClearRewriterCacheAction() {
        super(NAME);
    }


    @Override
    public NodesClearRewriterCacheResponse newResponse() {
        return new NodesClearRewriterCacheResponse();
    }
}
