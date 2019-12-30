package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionType;

public class PutRewriterAction extends ActionType<PutRewriterResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/save";
    public static final PutRewriterAction INSTANCE = new PutRewriterAction(NAME);

    /**
     * @param name The name of the action, must be unique across actions.
     */
    protected PutRewriterAction(final String name) {
        super(name, PutRewriterResponse::new);
    }


}
