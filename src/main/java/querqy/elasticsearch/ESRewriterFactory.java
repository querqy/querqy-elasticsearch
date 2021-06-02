package querqy.elasticsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.rewriterstore.LoadRewriterConfig;
import querqy.rewrite.RewriterFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;

public abstract class ESRewriterFactory {

    protected final String rewriterId;

    protected ESRewriterFactory(final String rewriterId) {
        this.rewriterId = rewriterId;
    }

    public abstract void configure(final Map<String, Object> config) throws ElasticsearchException;

    public abstract List<String> validateConfiguration(final Map<String, Object> config);

    public abstract RewriterFactory createRewriterFactory(final IndexShard indexShard) throws ElasticsearchException;

    public String getRewriterId() {
        return rewriterId;
    }

    public static ESRewriterFactory loadConfiguredInstance(final LoadRewriterConfig instanceDescription) {

        final String classField = instanceDescription.getRewriterClassName();
        if (classField == null) {
            throw new IllegalArgumentException("Property not found: " + instanceDescription
                    .getConfigMapping().getRewriterClassNameProperty());
        }

        final String className = classField.trim();
        if (className.isEmpty()) {
            throw new IllegalArgumentException("Class name expected in property: " + instanceDescription
                    .getConfigMapping().getRewriterClassNameProperty());
        }

        final Map<String, Object> config = instanceDescription.getConfig();

        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged(
                (PrivilegedAction<ESRewriterFactory>) () -> {

                    final ESRewriterFactory factory;

                    try {
                        factory = (ESRewriterFactory) Class.forName(className).getDeclaredConstructor(String.class)
                                .newInstance(instanceDescription.getRewriterId());

                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }

                    factory.configure(config);
                    return factory;

                });

    }


    public static ESRewriterFactory loadInstance(final String rewriterId, final Map<String, Object> instanceDesc,
                                                 final String argName) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }


        return AccessController.doPrivileged(
                (PrivilegedAction<ESRewriterFactory>) () -> {
                    final String classField = (String) instanceDesc.get(argName);
                    if (classField == null) {
                        throw new IllegalArgumentException("Property not found: " + argName);
                    }

                    final String className = classField.trim();
                    if (className.isEmpty()) {
                        throw new IllegalArgumentException("Class name expected in property: " + argName);
                    }


                    try {
                        return (ESRewriterFactory) Class.forName(className)
                                .getDeclaredConstructor(String.class).newInstance(rewriterId);
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                });





    }


}
