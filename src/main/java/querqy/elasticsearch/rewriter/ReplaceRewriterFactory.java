package querqy.elasticsearch.rewriter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.commonrules.QuerqyParserFactory;
import querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReplaceRewriterFactory extends ESRewriterFactory {

    private static final QuerqyParserFactory DEFAULT_RHS_QUERY_PARSER = new WhiteSpaceQuerqyParserFactory();

    private static final boolean DEFAULT_IGNORE_CASE = true;

    private static final String DEFAULT_INPUT_DELIMITER = "\t";

    private querqy.rewrite.contrib.ReplaceRewriterFactory delegate;

    public ReplaceRewriterFactory(String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(Map<String, Object> config) {
        final String rules = (String) config.get("rules");
        final InputStreamReader rulesReader = new InputStreamReader(
                new ByteArrayInputStream(rules.getBytes()));

        final boolean ignoreCase = ConfigUtils.getArg(config, "ignoreCase", DEFAULT_IGNORE_CASE);

        final String inputDelimiter = ConfigUtils.getArg(config, "inputDelimiter", DEFAULT_INPUT_DELIMITER);

        final QuerqyParserFactory querqyParser = ConfigUtils.getInstanceFromArg(
                config, "querqyParser", DEFAULT_RHS_QUERY_PARSER);

        try {
            delegate = new querqy.rewrite.contrib.ReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase,
                    inputDelimiter, querqyParser.createParser());
        } catch (final IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public List<String> validateConfiguration(Map<String, Object> config) {

        final String rules = (String) config.get("rules");
        if (rules == null) {
            return Collections.singletonList("Property 'rules' not configured");
        }

        final InputStreamReader rulesReader = new InputStreamReader(
                new ByteArrayInputStream(rules.getBytes()));

        final boolean ignoreCase = ConfigUtils.getArg(config, "ignoreCase", DEFAULT_IGNORE_CASE);

        final String inputDelimiter = ConfigUtils.getArg(config, "inputDelimiter", DEFAULT_INPUT_DELIMITER);


        final QuerqyParserFactory querqyParser;
        try {
            querqyParser = ConfigUtils.getInstanceFromArg(config, "querqyParser", DEFAULT_RHS_QUERY_PARSER);
        } catch (final Exception e) {
            return Collections.singletonList("Invalid attribute 'querqyParser': " + e.getMessage());
        }

        try {
            new querqy.rewrite.contrib.ReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase, inputDelimiter,
                    querqyParser.createParser());
        } catch (final IOException e) {
            return Collections.singletonList("Cannot create rewriter: " + e.getMessage());
        }

        return null;
    }

    @Override
    public RewriterFactory createRewriterFactory(IndexShard indexShard) {
        return delegate;
    }
}
