package querqy.elasticsearch.rewriter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.commonrules.QuerqyParserFactory;
import querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory;
import querqy.rewrite.commonrules.select.ExpressionCriteriaSelectionStrategyFactory;
import querqy.rewrite.commonrules.select.SelectionStrategyFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleCommonRulesRewriterFactory extends ESRewriterFactory {

    private static final SelectionStrategyFactory DEFAULT_SELECTION_STRATEGY_FACTORY =
            new ExpressionCriteriaSelectionStrategyFactory();


    private final static QuerqyParserFactory DEFAULT_RHS_QUERY_PARSER = new WhiteSpaceQuerqyParserFactory();

    private querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory delegate;

    public SimpleCommonRulesRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    public void configure(final Map<String, Object> config) {
        final boolean ignoreCase = ConfigUtils.getArg(config, "ignoreCase", true);

        final QuerqyParserFactory querqyParser = ConfigUtils
                .getInstanceFromArg(config, "querqyParser", DEFAULT_RHS_QUERY_PARSER);

        final String rules = ConfigUtils.getStringArg(config, "rules", "");

        // TODO: we might want to configure named selection strategies in the future
        final Map<String, SelectionStrategyFactory> selectionStrategyFactories = Collections.emptyMap();

        try {
            delegate = new querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory(rewriterId,
                    new StringReader(rules), querqyParser, ignoreCase, selectionStrategyFactories,
                    DEFAULT_SELECTION_STRATEGY_FACTORY);
        } catch (final IOException e) {
            throw new ElasticsearchException(e);
        }

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
                    new StringReader(rules), querqyParser, ignoreCase, Collections.emptyMap(),
                    DEFAULT_SELECTION_STRATEGY_FACTORY);
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
