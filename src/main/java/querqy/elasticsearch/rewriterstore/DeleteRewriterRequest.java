package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

import java.util.Collections;

public class DeleteRewriterRequest extends ActionRequest {

    private String rewriterId;

    public DeleteRewriterRequest() {
        super();
    }

    public DeleteRewriterRequest(final String rewriterId) {
        super();
        this.rewriterId = rewriterId;
    }

    @Override
    public ActionRequestValidationException validate() {

        if (rewriterId == null) {
            final ActionRequestValidationException arve = new ActionRequestValidationException();
            arve.addValidationErrors(Collections.singletonList("rewriterId must not be null"));
            return arve;
        }


        return null;
    }

    public String getRewriterId() {
        return rewriterId;
    }


}
