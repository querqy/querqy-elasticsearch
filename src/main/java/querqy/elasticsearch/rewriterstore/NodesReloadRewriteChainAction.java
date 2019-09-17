package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.Action;

public class NodesReloadRewriteChainAction  extends Action<NodesReloadRewriteChainResponse> {

    public static final String NAME = "cluster:admin/querqy/rewritechain/_reload";
    public static final NodesReloadRewriteChainAction INSTANCE = new NodesReloadRewriteChainAction();


    public NodesReloadRewriteChainAction() {
        super(NAME);
    }


    @Override
    public NodesReloadRewriteChainResponse newResponse() {
        return new NodesReloadRewriteChainResponse();
    }
}
