package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.Action;

public class NodesReloadRewriterAction extends Action<NodesReloadRewriterResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/_reload";
    public static final NodesReloadRewriterAction INSTANCE = new NodesReloadRewriterAction();


    public NodesReloadRewriterAction() {
        super(NAME);
    }


    @Override
    public NodesReloadRewriterResponse newResponse() {
        return new NodesReloadRewriterResponse();
    }
}
