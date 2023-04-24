package querqy.elasticsearch.rewriter.embeddings;

import org.elasticsearch.ElasticsearchException;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.embeddings.Embedding;
import querqy.embeddings.EmbeddingCache;
import querqy.embeddings.EmbeddingModel;
import querqy.rewrite.RewriterFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmbeddingsRewriterFactory extends ESRewriterFactory {

    public static final String CONF_MODEL = "model";
    public static final String CONF_CLASS = "class";

    private EmbeddingModel model;

    private static final EmbeddingCache<String> NULL_CACHE = new EmbeddingCache<>() {
        @Override
        public Embedding getEmbedding(final String key) {
            return null;
        }

        @Override
        public void putEmbedding(final String key, final Embedding embedding) {
        }
    };

    public EmbeddingsRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) {
        final Map<String, Object> modelConfig = (Map<String, Object>) config.get(CONF_MODEL);
        if (modelConfig == null) {
            throw new IllegalArgumentException("Missing config property" + CONF_MODEL);
        }
        final EmbeddingModel embeddingModel = ConfigUtils.getInstanceFromArg(modelConfig, CONF_CLASS, null);
        if (embeddingModel == null) {
            throw new IllegalArgumentException("Missing " + CONF_MODEL + "/" + CONF_CLASS + "  property");
        }
        // todo: caching
        embeddingModel.configure(modelConfig, NULL_CACHE);
        this.model = embeddingModel;
    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {
        try {
            // TODO: provide some more meaningful error details (for now we just try to configure ourselves and
            //  see what happens)
            configure(config);
        } catch (final Exception e) {
            return Collections.singletonList("Cannot configure this EmbeddingsRewriterFactory because: " +
                    e.getMessage());

        }
        return Collections.emptyList(); // it worked, no error message
    }

    @Override
    public RewriterFactory createRewriterFactory(org.elasticsearch.index.shard.IndexShard indexShard)
        throws ElasticsearchException {
        return new querqy.lucene.embeddings.EmbeddingsRewriterFactory(getRewriterId(), model);
    }
}


