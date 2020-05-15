package querqy.elasticsearch;

import static querqy.lucene.PhraseBoosting.makePhraseFieldsBoostQuery;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryShardContext;
import querqy.elasticsearch.query.BoostingQueries;
import querqy.elasticsearch.query.Generated;
import querqy.elasticsearch.query.PhraseBoosts;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.QueryBuilderRawQuery;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.query.RewrittenQueries;
import querqy.infologging.InfoLoggingContext;
import querqy.lucene.LuceneSearchEngineRequestAdapter;
import querqy.lucene.PhraseBoosting.PhraseBoostFieldParams;
import querqy.lucene.QuerySimilarityScoring;
import querqy.lucene.rewrite.SearchFieldsAndBoosting;
import querqy.lucene.rewrite.cache.TermQueryCache;
import querqy.model.QuerqyQuery;
import querqy.model.RawQuery;
import querqy.parser.QuerqyParser;
import querqy.rewrite.ContextAwareQueryRewriter;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriteChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 *  Rewriters will access params using prefix 'querqy.&lt;rewriter id&gt;....
 */
public class DismaxSearchEngineRequestAdapter implements LuceneSearchEngineRequestAdapter {

    private final RewriteChain rewriteChain;
    private final QueryShardContext shardContext;
    private final QuerqyQueryBuilder queryBuilder;
    private final Map<String, Object> context = new HashMap<>();

    public DismaxSearchEngineRequestAdapter(final QuerqyQueryBuilder queryBuilder,
                                            final RewriteChain rewriteChain,
                                            final QueryShardContext shardContext) {
        this.shardContext = shardContext;
        this.rewriteChain = rewriteChain;
        this.queryBuilder = queryBuilder;
    }

    /**
     * <p>Get the query string that should be parsed into the main query.</p>
     *
     * <p>Must be neither null or nor empty.</p>
     *
     * @return The query string.
     */
    @Override
    public String getQueryString() {
        return queryBuilder.getMatchingQuery().getQueryString();
    }

    /**
     * <p>Does this query string mean 'match all documents'?</p>
     *
     * @param queryString The query string.
     * @return true if the  query string means 'match all documents' and false otherwise
     */
    @Override
    public boolean isMatchAllQuery(final String queryString) {
        // TODO: do we want this?
        return false;
    }

    /**
     * <p>Should the query results be scored?</p>
     *
     * <p>This should return false for filter queries. If this method returns false, no boost queries will be used
     * (neither those from Querqy query rewriting nor those that were passed in request parameters).</p>
     *
     * @return true if the query results should be scored and false otherwise
     */
    @Override
    public boolean needsScores() {
        // TODO: should we allow to switch this off?
        return true;
    }

    /**
     * <p>Get the analyzer for applying text analysis to query terms.</p>
     *
     * <p>This will normally be an {@link Analyzer} that delegates to other Analyzers based on the given query fields.</p>
     *
     * @return The query analyzer.
     */
    @Override
    public Analyzer getQueryAnalyzer() {
        return shardContext.getMapperService().searchAnalyzer();
    }

    /**
     * Get an optional {@link TermQueryCache}
     *
     * @return The optional TermQueryCache
     */
    @Override
    public Optional<TermQueryCache> getTermQueryCache() {
        // TODO
        return Optional.empty();
    }

    /**
     * <p>Should Querqy boost queries be added to the main query? - yes, as we will not have control over
     * re-rank queries from within the query builder.</p>
     *
     * @return Always true
     */
    @Override
    public boolean addQuerqyBoostQueriesToMainQuery() {
        return true;
    }


    @Override
    public Optional<QuerySimilarityScoring> getUserQuerySimilarityScoring() {
        return queryBuilder.getMatchingQuery().getSimilarityScoring();
    }

    @Override
    public Optional<QuerySimilarityScoring> getBoostQuerySimilarityScoring() {
        final BoostingQueries boostingQueries = queryBuilder.getBoostingQueries();
        if (boostingQueries == null) {
            return Optional.empty();
        }
        return boostingQueries.getRewrittenQueries().map(RewrittenQueries::getSimilarityScoring);
    }

    /**
     * <p>Get the query fields and their weights for the query entered by the user.</p>
     *
     * @return A map of field names and field boost factors.
     * @see #getGeneratedQueryFieldsAndBoostings()
     */
    @Override
    public Map<String, Float> getQueryFieldsAndBoostings() {
        return queryBuilder.getQueryFieldsAndBoostings();
    }

    /**
     * <p>Get the query fields and their weights for queries that were generated during query rewriting.</p>
     *
     * <p>If this method returns an empty map, the map returned by {@link #getQueryFieldsAndBoostings()} will also be
     * used for generated queries.</p>
     *
     * @return A map of field names and field boost factors, or an empty map.
     * @see #useFieldBoostingInQuerqyBoostQueries()
     */
    @Override
    public Map<String, Float> getGeneratedQueryFieldsAndBoostings() {
        return queryBuilder.getGenerated().map(Generated::getQueryFieldsAndBoostings).orElse(Collections.emptyMap());
    }

    @Override
    public Optional<QuerqyParser> createQuerqyParser() {
        // TODO
        return Optional.empty();
    }

    @Override
    public boolean useFieldBoostingInQuerqyBoostQueries() {
        final BoostingQueries boostingQueries = queryBuilder.getBoostingQueries();
        if (boostingQueries == null) {
            return true;
        }
        return boostingQueries.getRewrittenQueries().map(RewrittenQueries::isUseFieldBoosts).orElse(true);
    }

    @Override
    public Optional<Float> getTiebreaker() {
        return queryBuilder.getTieBreaker();
    }

    /**
     * <p>Apply the 'minimum should match' setting of the request.</p>
     * <p>It will be the responsibility of the LuceneSearchEngineRequestAdapter implementation to derive the
     * 'minimum should match' setting from request parameters or other configuration.</p>
     * <p>The query parameter is the rewritten user query. {@link QueryRewriter}s shall guarantee to
     * preserve the number of top-level query clauses at query rewriting.</p>
     *
     * @param query The parsed and rewritten user query.
     * @return The query after application of 'minimum should match'
     * @see BooleanQuery#getMinimumNumberShouldMatch()
     */
    @Override
    public Query applyMinimumShouldMatch(final BooleanQuery query) {
        return Queries.applyMinimumShouldMatch(query, queryBuilder.getMinimumShouldMatch());
    }

    /**
     * Get the weight to be multiplied with the main Querqy query (the query entered by the user).
     *
     * @return An optional weight for the main query
     */
    @Override
    public Optional<Float> getUserQueryWeight() {
        return queryBuilder.getMatchingQuery().getWeight();
    }


    @Override
    public Optional<Float> getGeneratedFieldBoost() {
        final Optional<Generated> generated = queryBuilder.getGenerated();
        if (generated.isPresent()) {
            return generated.get().getFieldBoostFactor();
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Float> getPositiveQuerqyBoostWeight() {

        final BoostingQueries boostingQueries = queryBuilder.getBoostingQueries();
        if (boostingQueries == null) {
            return Optional.empty();
        }
        return boostingQueries.getRewrittenQueries().map(RewrittenQueries::getPositiveWeight);

    }

    @Override
    public Optional<Float> getNegativeQuerqyBoostWeight() {

        final BoostingQueries boostingQueries = queryBuilder.getBoostingQueries();
        if (boostingQueries == null) {
            return Optional.empty();
        }
        return boostingQueries.getRewrittenQueries().map(RewrittenQueries::getNegativeWeight);

    }

    /**
     * <p>Get the list of boost queries whose scores should be added to the score of the main query.</p>
     * <p>The queries are not a result of query rewriting but queries that may have been added as request parameters
     * (like 'bq' in Solr's Dismax query parser).</p>
     *
     * @param userQuery The user query parsed into a {@link QuerqyQuery}
     * @return The list of additive boost queries or an empty list if no such query exists.
     * @throws SyntaxException if a multiplicative boost query could not be parsed
     */
    @Override
    public List<Query> getAdditiveBoosts(final QuerqyQuery<?> userQuery) throws SyntaxException {

        //final PhraseBoosts phraseBoosts = queryBuilder.getPhraseBoosts();
        final BoostingQueries boostingQueries = queryBuilder.getBoostingQueries();
        if (boostingQueries != null) {
            final PhraseBoosts phraseBoosts = boostingQueries.getPhraseBoosts();
            if (phraseBoosts != null) {

                final List<Query> boosts = new ArrayList<>(1);
                final List<PhraseBoostFieldParams> phraseBoostFieldParams = phraseBoosts.toPhraseBoostFieldParams();
                if (phraseBoostFieldParams == null || phraseBoostFieldParams.isEmpty()) {
                    return boosts;
                }

                makePhraseFieldsBoostQuery(userQuery, phraseBoostFieldParams, phraseBoosts.getTieBreaker(),
                        getQueryAnalyzer()).ifPresent(boosts::add);
                return boosts;

            }
        }
        return null;
    }

    /**
     * <p>Get the list of boost queries whose scores should be multiplied to the score of the main query.</p>
     * <p>The queries are
     * not a result of query rewriting but queries that may have been added as request parameters (like 'boost'
     * in Solr's Extended Dismax query parser).</p>
     *
     * @param userQuery The user query parsed into a {@link QuerqyQuery}
     * @return The list of multiplicative boost queries or an empty list if no such query exists.
     * @throws SyntaxException if a multiplicative boost query could not be parsed
     */
    @Override
    public List<Query> getMultiplicativeBoosts(QuerqyQuery<?> userQuery) throws SyntaxException {
        return null;
    }

    /**
     * <p>Parse a {@link RawQuery}.</p>
     *
     * @param rawQuery The raw query.
     * @return The Query parsed from {@link RawQuery#queryString}
     * @throws SyntaxException @throws SyntaxException if the raw query query could not be parsed
     */
    @Override
    public Query parseRawQuery(final RawQuery rawQuery) throws SyntaxException {

        try {
            if (rawQuery instanceof QueryBuilderRawQuery) {
                return ((QueryBuilderRawQuery) rawQuery).getQueryBuilder().toQuery(shardContext);
            }

            final XContentParser parser = XContentHelper.createParser(shardContext.getXContentRegistry(), null,
                    new BytesArray(rawQuery.getQueryString()), XContentType.JSON);

            return shardContext.parseInnerQueryBuilder(parser).toQuery(shardContext);

        } catch (final IOException e) {
            throw new SyntaxException("Error parsing raw query", e);
        }

    }

    @Override
    public Optional<SearchFieldsAndBoosting.FieldBoostModel> getFieldBoostModel() {
        return queryBuilder.getFieldBoostModel();
    }

    /**
     * <p>Get the rewrite chain to be applied to the user query.</p>
     *
     * @return The rewrite chain.
     */
    @Override
    public RewriteChain getRewriteChain() {
        return rewriteChain;
    }

    /**
     * <p>Get a map to hold context information while rewriting the query.</p>
     *
     * @return A non-null context map.
     * @see ContextAwareQueryRewriter
     */
    @Override
    public Map<String, Object> getContext() {
        return context;
    }

    /**
     * Get request parameter as String
     *
     * @param name the parameter name
     * @return the optional parameter value
     */
    @Override
    public Optional<String> getRequestParam(final String name) {
        return getParam(name);
    }

    <T> Optional<T> getParam(final String name) {

        final String[] parts = name.split("\\.");
        if (parts.length < 3 || !"querqy".equals(parts[0])) {
            return Optional.empty();
        }

        final String rewriterId = parts[1];
        for (final Rewriter rewriter : queryBuilder.getRewriters()) {
            if (rewriterId.equals(rewriter.getName())) {
                return getRewriterParam(rewriter, Arrays.copyOfRange(parts, 2, parts.length));
            }
        }

        return Optional.empty();

    }


    <T> Optional<T> getRewriterParam(final Rewriter rewriter, final String[] path) {

        final Map<String, Object> params = rewriter.getParams();
        if (params == null) {
            return Optional.empty();
        }

        Map<String, Object> current = params;
        final int len = path.length - 1;
        for (int i = 0; i < len; i++) {
            current = (Map<String, Object>) current.get(path[i]);
            if (current == null) {
                return Optional.empty();
            }
        }

        return Optional.ofNullable((T) current.get(path[len]));


    }

    /**
     * Get request parameter as an array of Strings
     *
     * @param name the parameter name
     * @return the parameter value String array (String[0] if not set)
     */
    @Override
    public String[] getRequestParams(final String name) {
        final String[] parts = name.split("\\.");
        if (parts.length < 3 || !"querqy".equals(parts[0])) {
            return new String[0];
        }

        final String rewriterId = parts[1];
        for (final Rewriter rewriter : queryBuilder.getRewriters()) {
            if (rewriterId.equals(rewriter.getName())) {
                final Map<String, Object> params = rewriter.getParams();
                if (params == null) {
                    return new String[0];
                }

                Map<String, Object> current = params;
                final int len = parts.length - 1;
                for (int i = 2; i < len; i++) {
                    current = ( Map<String, Object>) current.get(parts[i]);
                    if (current == null) {
                        return new String[0];
                    }
                }
                final Object obj = current.get(parts[len]);
                if (obj == null) {
                    return new String[0];
                }
                if (obj instanceof String) {
                    return new String[] {obj.toString()};
                } else {
                    return (String[]) obj;
                }
            }
        }

        return new String[0];

    }

    /**
     * Get request parameter as Boolean
     *
     * @param name the parameter name
     * @return the optional parameter value
     */
    @Override
    public Optional<Boolean> getBooleanRequestParam(final String name) {
        return getParam(name);
    }

    /**
     * Get request parameter as Integer
     *
     * @param name the parameter name
     * @return the optional parameter value
     */
    @Override
    public Optional<Integer> getIntegerRequestParam(final String name) {
        return getParam(name);
    }

    /**
     * Get request parameter as Float
     *
     * @param name the parameter name
     * @return the optional parameter value
     */
    @Override
    public Optional<Float> getFloatRequestParam(final String name) {
        return getParam(name);
    }

    /**
     * Get request parameter as Double
     *
     * @param name the parameter name
     * @return the optional parameter value
     */
    @Override
    public Optional<Double> getDoubleRequestParam(final String name) {
        return getParam(name);
    }

    /**
     * <p>Get the per-request info logging. Return an empty option if logging hasn't been configured or was disabled
     * for this request.</p>
     *
     * @return the InfoLoggingContext object
     */
    @Override
    public Optional<InfoLoggingContext> getInfoLoggingContext() {
        return Optional.empty();
    }

    /**
     * <p>Should debug information be collected while rewriting the query?</p>
     * <p>Debug information will be kept in the context map under the
     * {@link ContextAwareQueryRewriter#CONTEXT_KEY_DEBUG_DATA} key.</p>
     *
     * @return true if debug information shall be collected, false otherwise
     * @see #getContext()
     */
    @Override
    public boolean isDebugQuery() {
        return false;
    }

    public QueryShardContext getShardContext() {
        return shardContext;
    }
}
