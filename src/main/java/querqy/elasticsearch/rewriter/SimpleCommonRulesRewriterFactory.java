package querqy.elasticsearch.rewriter;

import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.commonrules.QuerqyParserFactory;
import querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleCommonRulesRewriterFactory extends ESRewriterFactory {

    private final static QuerqyParserFactory DEFAULT_RHS_QUERY_PARSER = new WhiteSpaceQuerqyParserFactory();

    private querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory delegate;

    public SimpleCommonRulesRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    public void configure(final Map<String, Object> config) throws Exception {
        final boolean ignoreCase = ConfigUtils.getArg(config, "ignoreCase", true);

        final QuerqyParserFactory querqyParser = ConfigUtils
                .getInstanceFromArg(config, "querqyParser", DEFAULT_RHS_QUERY_PARSER);

        final String rules = ConfigUtils.getStringArg(config, "rules", "");

        delegate = new querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory(rewriterId,
                new StringReader(rules), querqyParser, ignoreCase, Collections.emptyMap());
    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {
        final String rules = ConfigUtils.getStringArg(config, "rules",  null);
        if (rules == null) {
            return Collections.singletonList("Missing attribute 'rules'");
        }
        final QuerqyParserFactory querqyParser;
        try {
            querqyParser = ConfigUtils
                    .getInstanceFromArg(config, "querqyParser", DEFAULT_RHS_QUERY_PARSER);
        } catch (final Exception e) {
            return Collections.singletonList("Invalid attribute 'querqyParser': " + e.getMessage());
        }


        final boolean ignoreCase = ConfigUtils.getArg(config, "ignoreCase", true);
        try {
            new querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory(rewriterId,
                    new StringReader(rules), querqyParser, ignoreCase, Collections.emptyMap());
        } catch (final IOException e) {
            return Collections.singletonList("Cannot create rewriter: " + e.getMessage());
        }

        return null;
    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard) {
        return delegate;
    }


}
