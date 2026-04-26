package querqy.elasticsearch.rewriter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RegexReplaceRewriterFactory extends ESRewriterFactory {

    private static final boolean DEFAULT_IGNORE_CASE = true;

    public static final String PROP_RULES = "rules";
    public static final String PROP_IGNORE_CASE = "ignoreCase";

    private querqy.rewrite.replace.RegexReplaceRewriterFactory delegate;

    public RegexReplaceRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) throws ElasticsearchException {

        final String rules = (String) config.get(PROP_RULES);
        final InputStreamReader rulesReader = new InputStreamReader(
                new ByteArrayInputStream(rules.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
        final boolean ignoreCase = ConfigUtils.getArg(config, PROP_IGNORE_CASE, DEFAULT_IGNORE_CASE);

        try {
            delegate = new querqy.rewrite.replace.RegexReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase);
        } catch (final IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {

        final String rules = (String) config.get(PROP_RULES);
        if (rules == null) {
            return Collections.singletonList("Property " + PROP_RULES + " not configured");
        }

        final InputStreamReader rulesReader = new InputStreamReader(
                new ByteArrayInputStream(rules.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);

        final boolean ignoreCase = ConfigUtils.getArg(config, PROP_IGNORE_CASE, DEFAULT_IGNORE_CASE);

        try {
            new querqy.rewrite.replace.RegexReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase);
        } catch (final IOException e) {
            return Collections.singletonList("Cannot create rewriter: " + e.getMessage());
        }

        return List.of();
    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard) {
        return delegate;
    }
}
