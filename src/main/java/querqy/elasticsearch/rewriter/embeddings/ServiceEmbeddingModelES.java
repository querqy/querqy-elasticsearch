package querqy.elasticsearch.rewriter.embeddings;

import org.elasticsearch.SpecialPermission;
import querqy.embeddings.ServiceEmbeddingModel;

import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Subclasses the {@link querqy.embeddings.ServiceEmbeddingModel} to wrap the outbound request code into
 * a privileged action for ES.
 */
public class ServiceEmbeddingModelES extends ServiceEmbeddingModel {

    @SuppressWarnings("removal")
    @Override
    protected InputStream doRequest(String requestBody)  {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<InputStream>) () -> super.doRequest(requestBody));
        } catch (PrivilegedActionException e) {
            throw new RuntimeException(e);
        }

    }
}
