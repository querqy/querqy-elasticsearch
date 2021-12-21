package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class RewriterConfigMapping {

    public static final int CURRENT_MAPPING_VERSION = 3;

    public static final String PROP_VERSION = "version";
    public static final String PROP_TYPE = "type";

    public final static RewriterConfigMapping CURRENT = new RewriterConfigMapping() {

        @Override
        public String getConfigStringProperty() {
            return "config_v_003";
        }

        @Override
        public final String getRewriterClassNameProperty() {
            return "class";
        }

        @Override
        public String getInfoLoggingProperty() {
            return "info_logging";
        }

        @Override
        public String getRewriterClassName(final String rewriterId, final Map<String, Object> source) {
            return (String) source.get(getRewriterClassNameProperty());
        }

        @Override
        public Map<String, Object> getInfoLoggingConfig(final String rewriterId, final Map<String, Object> source) {
            return (Map<String, Object>) source.get(getInfoLoggingProperty());
        }

    };

    public final static RewriterConfigMapping PRE3_MAPPING = new RewriterConfigMapping() {

        @Override
        public String getConfigStringProperty() {
            return "config";
        }

        @Override
        public final String getRewriterClassNameProperty() {
            return "class";
        }

        @Override
        public String getInfoLoggingProperty() {
            return "info_logging";
        }

        @Override
        public String getRewriterClassName(final String rewriterId, final Map<String, Object> source) {
            return (String) source.get(getRewriterClassNameProperty());
        }

        @Override
        public Map<String, Object> getInfoLoggingConfig(final String rewriterId, final Map<String, Object> source) {
            return (Map<String, Object>) source.get(getInfoLoggingProperty());
        }

    };





    public abstract String getRewriterClassNameProperty();
    public abstract String getConfigStringProperty();
    public abstract String getInfoLoggingProperty();
    public abstract String getRewriterClassName(final String rewriterId, final Map<String, Object> source);
    public abstract Map<String, Object> getInfoLoggingConfig(final String rewriterId, final Map<String, Object> source);



    public static RewriterConfigMapping getMapping(final Map<String, Object> source) {

        final Integer version = (Integer) source.get(PROP_VERSION);

        if (version == null) {
            return PRE3_MAPPING;
        }

        if (version == CURRENT_MAPPING_VERSION) {
            return CURRENT;
        }

        throw new IllegalArgumentException("Unknown rewriter config version: " + version);

    }

    public static Map<String, Object> toLuceneSource(final Map<String, Object> putRequestContent) throws IOException {
        final Map<String, Object> source = new HashMap<>(putRequestContent.size() + 3);
        source.put(PROP_TYPE, "rewriter");
        source.put(PROP_VERSION, CURRENT_MAPPING_VERSION);
        source.put(CURRENT.getRewriterClassNameProperty(), putRequestContent.get("class"));

        final Map<String, Object> infoLoggingConfig = (Map<String, Object>) putRequestContent.get("info_logging");
        if (infoLoggingConfig != null) {
            source.put(CURRENT.getInfoLoggingProperty(), infoLoggingConfig);
        }

        final Map<String, Object> config = (Map<String, Object>) putRequestContent.get("config");
        if (config != null) {
            source.put(CURRENT.getConfigStringProperty(), mapToJsonString(config));
        }

        return source;
    }

    public Map<String, Object> getConfig(final String rewriterId, final Map<String, Object> source) {

        String configStr = (String) source.get(getConfigStringProperty());
        if (configStr != null) {
            configStr = configStr.trim();
        }

        final Map<String, Object> config;

        if (configStr != null && configStr.length() > 0) {

            final XContentParser parser;
            try {
                parser = XContentHelper.createParser(null, null, new BytesArray(configStr),
                        XContentType.JSON);
            } catch (final IOException e) {
                throw new ElasticsearchException(e);
            }
            try {
                config = parser.map();
            } catch (final IOException e) {
                throw new ParsingException(parser.getTokenLocation(), "Could not load 'config' of rewriter "
                        + rewriterId);
            }

        } else {
            config = Collections.emptyMap();
        }

        return config;
    }

    private static String mapToJsonString(final Map<String, Object> map) throws IOException {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), bos);
            builder.value(map);
            builder.flush();
            builder.close();
            return new String(bos.toByteArray(), StandardCharsets.UTF_8);
        }
    }

}
