package querqy.elasticsearch.rewriterstore;

import java.util.Map;

public class LoadRewriterConfig {

    private final RewriterConfigMapping configMapping;
    private final String rewriterId;
    private final Map<String, Object> luceneDoc;

    public LoadRewriterConfig(final String rewriterId, final Map<String, Object> luceneDoc) {
        this.configMapping = RewriterConfigMapping.getMapping(luceneDoc);
        this.rewriterId = rewriterId;
        this.luceneDoc = luceneDoc;
    }

    public RewriterConfigMapping getConfigMapping() {
        return configMapping;
    }

    public String getRewriterId() {
        return rewriterId;
    }

    public String getRewriterClassName() {
        return configMapping.getRewriterClassName(rewriterId, luceneDoc);
    }

    public Map<String, Object> getConfig() {
        return configMapping.getConfig(rewriterId, luceneDoc);
    }

    public Map<String, Object> getInfoLoggingConfig() {
        return configMapping.getInfoLoggingConfig(rewriterId, luceneDoc);
    }




}
