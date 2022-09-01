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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PutRewriterRequest extends ActionRequest {

    private final Map<String, Object> content;
    private final String rewriterId;

    public PutRewriterRequest(final StreamInput in) throws IOException {
        super(in);
        rewriterId = in.readString();
        content = in.readMap();
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

        final Map<String, Object> loggingConfig = (Map<String, Object>) content.get("info_logging");
        if (loggingConfig != null) {
            final Object sinksObj = loggingConfig.get("sinks");
            if (sinksObj != null) {
                if (sinksObj instanceof String) {
                    if (!sinksObj.equals("log4j")) {
                        final ActionRequestValidationException arve = new ActionRequestValidationException();
                        arve.addValidationError("Can only log to sink named 'log4j' but not to " + sinksObj);
                        return arve;
                    }
                } else if (sinksObj instanceof Collection) {
                    Collection<?> sinksCollection = (Collection<?>) sinksObj;
                    if (sinksCollection.size() > 0) {
                        if (sinksCollection.size() > 1 || !sinksCollection.iterator().next().equals("log4j")) {
                            final ActionRequestValidationException arve = new ActionRequestValidationException();
                            arve.addValidationError("Can only log to sink named 'log4j'");
                            return arve;
                        }
                    }
                }
            }
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
        out.writeGenericMap(content);
    }

    public String getRewriterId() {
        return rewriterId;
    }

    public Map<String, Object> getContent() {
        return content;
    }

}
