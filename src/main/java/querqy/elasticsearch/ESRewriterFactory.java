package querqy.elasticsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.rewriterstore.LoadRewriterConfig;
import querqy.rewrite.RewriterFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

        final ESRewriterFactory factory = builder().rewriterId(instanceDescription.getRewriterId())
                .className(className).loadFactory();

        factory.configure(instanceDescription.getConfig());
        return factory;


    }

    public static ESRewriterFactory loadInstance(final String rewriterId, final Map<String, Object> instanceDesc,
                                                 final String argName) {

        final String classField = (String) instanceDesc.get(argName);
        if (classField == null) {
            throw new IllegalArgumentException("Property not found: " + argName);
        }

        final String className = classField.trim();
        if (className.isEmpty()) {
            throw new IllegalArgumentException("Class name expected in property: " + argName);
        }

        return builder()
                .rewriterId(rewriterId)
                .className(className)
                .loadFactory();

    }

    public static ESRewriterFactoryBuilder builder() {
        return new ESRewriterFactoryBuilder();
    }

    public static class ESRewriterFactoryBuilder {
        private String rewriterId;
        private String className;

        private ESRewriterFactoryBuilder() {}

        public ESRewriterFactoryBuilder rewriterId(final String rewriterId) {
            this.rewriterId = rewriterId;
            return this;
        }

        public ESRewriterFactoryBuilder className(final String className) {
            this.className = className;
            return this;
        }

        public ESRewriterFactory loadFactory() {
            Objects.requireNonNull(rewriterId);
            Objects.requireNonNull(className);
            try {
                Class<?> rewriterClass = Class.forName(className);
                if (!ESRewriterFactory.class.isAssignableFrom(rewriterClass)) {
                    throw new IllegalArgumentException("Class must implement " + ESRewriterFactory.class.getName());
                }
                return (ESRewriterFactory) rewriterClass.getDeclaredConstructor(String.class).newInstance(rewriterId);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
