package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import querqy.elasticsearch.ESRewriterFactory;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PutRewriterRequest extends ActionRequest {


    private Map<String, Object> content;
    private String rewriterId;

    public PutRewriterRequest() {
        super();
    }

    public PutRewriterRequest(final String rewriterId, final Map<String, Object> content) {
        super();
        this.rewriterId = rewriterId;
        this.content = content;
    }

    @Override
    public ActionRequestValidationException validate() {

        final ESRewriterFactory esRewriterFactory;
        try {
            esRewriterFactory = ESRewriterFactory.loadInstance(rewriterId, content, "class");
        } catch (final Exception e) {
            return ValidateActions.addValidationError("Invalid definition of rewriter 'class': " + e.getMessage(),
                    null);
        }


        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }


        final List<String> errors =  AccessController.doPrivileged(
                (PrivilegedAction<List<String> >) () -> {

                    try {
                        final Map<String, Object> config = (Map<String, Object>) content.getOrDefault("config",
                                Collections.emptyMap());
                        return esRewriterFactory.validateConfiguration(config);

                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                });





        if (errors != null && !errors.isEmpty()) {
            final ActionRequestValidationException arve = new ActionRequestValidationException();
            arve.addValidationErrors(errors);
            return arve;
        }

        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rewriterId);
        out.writeMap(content);
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        rewriterId = in.readString();
        content = in.readMap();
    }

    public String getRewriterId() {
        return rewriterId;
    }

    public void setRewriterId(String rewriterId) {
        this.rewriterId = rewriterId;
    }

    public Map<String, Object> getContent() {
        return content;
    }

    public void setContent(Map<String, Object> content) {
        this.content = content;
    }

}
