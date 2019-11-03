package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.Action;

public class DeleteRewriterAction extends Action<DeleteRewriterResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/delete";
    public static final DeleteRewriterAction INSTANCE = new DeleteRewriterAction(NAME);

    /**
     * @param name The name of the action, must be unique across actions.
     */
    protected DeleteRewriterAction(final String name) {
        super(name);
    }

    @Override
    public DeleteRewriterResponse newResponse() {
        return new DeleteRewriterResponse();
    }
}
