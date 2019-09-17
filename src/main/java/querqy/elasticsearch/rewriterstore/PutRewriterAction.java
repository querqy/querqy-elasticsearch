package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.Action;

public class PutRewriterAction extends Action<PutRewriterResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/save";
    public static final PutRewriterAction INSTANCE = new PutRewriterAction(NAME);

    /**
     * @param name The name of the action, must be unique across actions.
     */
    protected PutRewriterAction(final String name) {
        super(name);
    }

    @Override
    public PutRewriterResponse newResponse() {
        return new PutRewriterResponse();
    }


}
