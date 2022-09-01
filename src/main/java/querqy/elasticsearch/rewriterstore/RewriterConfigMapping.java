package querqy.elasticsearch.rewriterstore;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
            final String jsonString = mapToJsonString(config);
            // See constraints in org.elasticsearch.index.mapper.KeywordFieldMapper.indexValue()
            source.put(CURRENT.getConfigStringProperty(), stringToSourceValue(jsonString, 32766));
        }

        return source;
    }

    /**
     * Lucene doesn't allow us to save Strings in keywords fields if their UTF-8-encoded version exceeds a certain byte
     * array length. This method splits Strings into an array of Strings whose elements are guaranteed not to exceed
     * that limit. If the input string does not exceed the limit, the method returns the input string.
     *
     * @param string The input string
     * @param maxUTFByteLength The max length
     * @return A String or an array of Strings
     */
    public static Object stringToSourceValue(final String string, final int maxUTFByteLength) {

        final BytesRef binaryValue = new BytesRef(string);

        if (binaryValue.length <= maxUTFByteLength) {
            return string;
        }

        if (maxUTFByteLength < 3) {
            // max UTF encoding length for a single char is 3 bytes
            throw new IllegalArgumentException("maxUTFByteLength >=3 expected");
        }

        final List<String> splits = new ArrayList<>();

        String s = string;
        while (new BytesRef(s).length > maxUTFByteLength) {

            String split = s;
            do {

                int length = Math.max(1, Math.min((int) Math.floor(split.length() * 0.95), maxUTFByteLength));

                split = split.substring(0, length);

            } while (new BytesRef(split).length > maxUTFByteLength);

            splits.add(split);
            s = s.substring(split.length());
        }
        if (s.length() > 0) {
            splits.add(s);
        }

        return splits.toArray(new String[0]);


    }

    public Map<String, Object> getConfig(final String rewriterId, final Map<String, Object> source) {

        final Object configStringValue = source.get(getConfigStringProperty());

        final String configStr;
        if (configStringValue == null) {
            configStr = null;
        } else if (configStringValue instanceof String) {
            configStr = ((String) configStringValue).trim();
        } else if (configStringValue instanceof List || configStringValue.getClass().isArray()) {
            configStr = String.join("", (Iterable<? extends CharSequence>) configStringValue).trim();
        } else {
            throw new IllegalArgumentException("Unexpected config value class: " + configStringValue);
        }

        final Map<String, Object> config;

        if (configStr != null && configStr.length() > 0) {

            final XContentParser parser;
            try {
                parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(configStr),
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
