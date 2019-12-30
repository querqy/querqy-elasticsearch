package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;

public class DeleteRewriterRequest extends ActionRequest {

    private String rewriterId;

    // package private for testing
    DeleteRewriterRequest() {
        super();
    }

    public DeleteRewriterRequest(final StreamInput in) throws IOException {
        super(in);
        rewriterId = in.readString();
    }

    public DeleteRewriterRequest(final String rewriterId) {
        super();
        this.rewriterId = rewriterId;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rewriterId);
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
