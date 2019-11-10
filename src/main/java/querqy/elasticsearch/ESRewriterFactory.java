package querqy.elasticsearch;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.IndexShard;
import querqy.rewrite.RewriterFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class ESRewriterFactory {

    protected final String rewriterId;

    protected ESRewriterFactory(final String rewriterId) {
        this.rewriterId = rewriterId;
    }

    public abstract void configure(final Map<String, Object> config) throws Exception;

    public abstract List<String> validateConfiguration(final Map<String, Object> config);

    public abstract RewriterFactory createRewriterFactory(final IndexShard indexShard) throws Exception;

    public String getRewriterId() {
        return rewriterId;
    }

    public static ESRewriterFactory loadConfiguredInstance(final String rewriterId, final Map<String, Object> instanceDesc,
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
                        final ESRewriterFactory factory = (ESRewriterFactory) Class.forName(className)
                                .getDeclaredConstructor(String.class).newInstance(rewriterId);

                        String configStr = (String) instanceDesc.get("config");
                        if (configStr != null) {
                            configStr = configStr.trim();
                        }
                        final Map<String, Object> config;
                        if (configStr != null && configStr.length() > 0) {
                            final XContentParser parser = XContentHelper.createParser(null, null,
                                    new BytesArray(configStr), XContentType.JSON);
                            config = parser.map();
                        } else {
                            config = Collections.emptyMap();
                        }

                        factory.configure(config);
                        return factory;
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
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
