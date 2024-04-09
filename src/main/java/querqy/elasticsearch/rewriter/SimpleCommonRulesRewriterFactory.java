package querqy.elasticsearch.rewriter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.commonrules.QuerqyParserFactory;
import querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory;
import querqy.rewrite.commonrules.model.BoostInstruction;
import querqy.rewrite.commonrules.select.ExpressionCriteriaSelectionStrategyFactory;
import querqy.rewrite.commonrules.select.SelectionStrategyFactory;
import querqy.rewrite.lookup.preprocessing.LookupPreprocessorType;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SimpleCommonRulesRewriterFactory extends ESRewriterFactory {

    public static final String CONF_IGNORE_CASE = "ignoreCase";
    public static final String CONF_ALLOW_BOOLEAN_INPUT = "allowBooleanInput";
//    public static final String CONF_BOOST_METHOD = "boostMethod";
    public static final String CONF_RHS_QUERY_PARSER = "querqyParser";
    public static final String CONF_RULES = "rules";
//    public static final String CONF_RULE_SELECTION_STRATEGIES = "ruleSelectionStrategies";
    public static final String CONF_LOOKUP_PREPROCESSOR = "lookupPreprocessor";

    static final QuerqyParserFactory DEFAULT_RHS_QUERY_PARSER = new WhiteSpaceQuerqyParserFactory();
    static final SelectionStrategyFactory DEFAULT_SELECTION_STRATEGY_FACTORY =
            new ExpressionCriteriaSelectionStrategyFactory();

    static final LookupPreprocessorType DEFAULT_LOOKUP_PREPROCESSOR_TYPE = LookupPreprocessorType.LOWERCASE;
//    public static final String CONF_BUILD_TERM_CACHE = "buildTermCache";

    private querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory delegate;

    public SimpleCommonRulesRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) {
        final boolean ignoreCase = ConfigUtils.getArg(config, CONF_IGNORE_CASE, true);
        final boolean allowBooleanInput = ConfigUtils.getArg(config, CONF_ALLOW_BOOLEAN_INPUT, false);

        final QuerqyParserFactory querqyParser = ConfigUtils
                .getInstanceFromArg(config, CONF_RHS_QUERY_PARSER, DEFAULT_RHS_QUERY_PARSER);

        final String rules = ConfigUtils.getStringArg(config, CONF_RULES, "");

        // TODO: we might want to configure named selection strategies in the future
        final Map<String, SelectionStrategyFactory> selectionStrategyFactories = Collections.emptyMap();

        final Optional<String> lookupPreprocessorTypeName = ConfigUtils.getStringArg(config, CONF_LOOKUP_PREPROCESSOR);
        final LookupPreprocessorType lookupPreprocessorType = lookupPreprocessorTypeName
                .map(LookupPreprocessorType::fromString)
                .orElse(DEFAULT_LOOKUP_PREPROCESSOR_TYPE);

        try {
            delegate = new querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory(rewriterId,
                    new StringReader(rules), allowBooleanInput, BoostInstruction.BoostMethod.ADDITIVE,
                    querqyParser, selectionStrategyFactories, DEFAULT_SELECTION_STRATEGY_FACTORY, false,
                    lookupPreprocessorType);
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
        final boolean allowBooleanInput = ConfigUtils.getArg(config, "allowBooleanInput", false);

        final Optional<String> lookupPreprocessorTypeName = ConfigUtils.getStringArg(config, CONF_LOOKUP_PREPROCESSOR);
        final LookupPreprocessorType lookupPreprocessorType = lookupPreprocessorTypeName
                .map(LookupPreprocessorType::fromString)
                .orElse(DEFAULT_LOOKUP_PREPROCESSOR_TYPE);

        try {
            new querqy.rewrite.commonrules.SimpleCommonRulesRewriterFactory(rewriterId,
                    new StringReader(rules), allowBooleanInput, BoostInstruction.BoostMethod.ADDITIVE,
                    querqyParser, Collections.emptyMap(), DEFAULT_SELECTION_STRATEGY_FACTORY, false,
                    lookupPreprocessorType);
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
