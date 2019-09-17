package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.Action;

public class PutRewriteChainAction extends Action<PutRewriteChainResponse> {

    public static final String NAME = "cluster:admin/querqy/rewritechain/save";
    public static final PutRewriteChainAction INSTANCE = new PutRewriteChainAction(NAME);

    /**
     * @param name The name of the action, must be unique across actions.
     */
    protected PutRewriteChainAction(final String name) {
        super(name);
    }

    @Override
    public PutRewriteChainResponse newResponse() {
        return new PutRewriteChainResponse();
    }


}
