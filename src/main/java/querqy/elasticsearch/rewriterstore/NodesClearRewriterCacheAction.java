package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionType;

/**
 * Remove a rewriter from cache
 */
public class NodesClearRewriterCacheAction extends ActionType<NodesClearRewriterCacheResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/_clearcache";
    public static final NodesClearRewriterCacheAction INSTANCE = new NodesClearRewriterCacheAction(NAME);


    protected NodesClearRewriterCacheAction(final String name) {
        super(name);
    }

}
