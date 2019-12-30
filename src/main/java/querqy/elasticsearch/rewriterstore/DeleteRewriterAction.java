package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionType;

public class DeleteRewriterAction extends ActionType<DeleteRewriterResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/delete";
    public static final DeleteRewriterAction INSTANCE = new DeleteRewriterAction(NAME);

    /**
     * @param name The name of the action, must be unique across actions.
     */
    protected DeleteRewriterAction(final String name) {
        super(name, DeleteRewriterResponse::new);
    }

}
