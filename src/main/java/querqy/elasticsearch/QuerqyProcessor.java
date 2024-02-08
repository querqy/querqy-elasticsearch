package querqy.elasticsearch;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.SearchExecutionContext;
import querqy.elasticsearch.infologging.LogPayloadType;
import querqy.elasticsearch.infologging.SingleSinkInfoLogging;
import querqy.elasticsearch.query.InfoLoggingSpec;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.infologging.InfoLogging;
import querqy.infologging.Sink;
import querqy.lucene.LuceneQueries;
import querqy.lucene.LuceneSearchEngineRequestAdapter;
import querqy.lucene.QueryParsingController;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.commonrules.model.DecorateInstruction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class QuerqyProcessor {

    public static final Setting<TimeValue> CACHE_DECORATIONS_EXPIRE_AFTER_WRITE = Setting.timeSetting(
            "querqy.caches.decorations.expire_after_write",
            TimeValue.timeValueSeconds(10L), // 10s
            TimeValue.timeValueSeconds(10L),
            Setting.Property.NodeScope);

    public static final Setting<TimeValue> CACHE_DECORATIONS_EXPIRE_AFTER_READ = Setting.timeSetting(
            "querqy.caches.decorations.expire_after_read",
            TimeValue.timeValueSeconds(10L), // 10ns
            TimeValue.timeValueSeconds(10L),
            Setting.Property.NodeScope);

    private static final RewriteChain EMPTY_REWRITE_CHAIN = new RewriteChain(Collections.emptyList());

    //private final Map<Query, Map<String, Object>> querqyQueryCache;
    private final Cache<Query, Map<String, Object>> querqyQueryCache;

    private RewriterShardContexts rewriterShardContexts;
    private Sink infoLoggingSink;

    public QuerqyProcessor(final RewriterShardContexts rewriterShardContexts, final Sink infoLoggingSink) {
        this(rewriterShardContexts, infoLoggingSink, Settings.EMPTY);
    }

    public QuerqyProcessor(final RewriterShardContexts rewriterShardContexts, final Sink infoLoggingSink, final Settings settings) {
        this.rewriterShardContexts = rewriterShardContexts;
        this.infoLoggingSink = infoLoggingSink;
        //querqyInfoCache = new HashMap<>();
        querqyQueryCache = Caches.buildCache(CACHE_DECORATIONS_EXPIRE_AFTER_READ.get(settings), CACHE_DECORATIONS_EXPIRE_AFTER_WRITE.get(settings));
    }

    public Query parseQuery(final QuerqyQueryBuilder queryBuilder, final SearchExecutionContext context)
            throws LuceneSearchEngineRequestAdapter.SyntaxException {

        final List<Rewriter> rewriters = queryBuilder.getRewriters();

        final RewriteChain rewriteChain;
        final Set<String> rewritersEnabledForLogging;
        if (rewriters == null || rewriters.isEmpty()) {

            rewriteChain = EMPTY_REWRITE_CHAIN;
            rewritersEnabledForLogging = Collections.emptySet();

        } else {

            final RewriteChainAndLogging rewriteChainAndLogging = rewriterShardContexts.getRewriteChain(
                    rewriters.stream().map(Rewriter::getName).collect(Collectors.toList()), context);

            rewriteChain = rewriteChainAndLogging.rewriteChain;
            final InfoLoggingSpec infoLoggingSpec = queryBuilder.getInfoLoggingSpec();

            if ((infoLoggingSpec != null) && (infoLoggingSpec.getPayloadType() != LogPayloadType.NONE)
                    && !infoLoggingSpec.isLogged()) {

                infoLoggingSpec.setLogged(true);
                rewritersEnabledForLogging = rewriteChainAndLogging.rewritersEnabledForLogging;

            } else {
                rewritersEnabledForLogging = Collections.emptySet();
            }

        }

        final InfoLogging infoLogging = rewritersEnabledForLogging.isEmpty()
                ? null : new SingleSinkInfoLogging(infoLoggingSink, rewritersEnabledForLogging);

        final DismaxSearchEngineRequestAdapter requestAdapter =
                new DismaxSearchEngineRequestAdapter(queryBuilder, rewriteChain, context, infoLogging);

        final QueryParsingController controller = new QueryParsingController(requestAdapter);
        final LuceneQueries queries = controller.process();

        if ((queries.querqyBoostQueries == null || queries.querqyBoostQueries.isEmpty())
                && (queries.filterQueries == null || queries.filterQueries.isEmpty())
                && queries.mainQuery instanceof BooleanQuery) {

            // TODO: we should do this when building the query
            final List<BooleanClause> clauses = ((BooleanQuery) queries.mainQuery).clauses();
            if (clauses.size() == 1) {
                final BooleanClause onlyClause = clauses.get(0);
                if (onlyClause.isScoring()) {
                    return onlyClause.getQuery();
                }
            }
        }

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        builder.add(queries.mainQuery, BooleanClause.Occur.MUST);
        if (queries.querqyBoostQueries != null) {
            for (final Query query : queries.querqyBoostQueries) {
                builder.add(query, BooleanClause.Occur.SHOULD);
            }
        }

        appendFilterQueries(queries, builder);

        final BooleanQuery query = builder.build();
        if (infoLogging != null) {
            infoLogging.endOfRequest(requestAdapter);
        }

        final Set<Object> decorations = (Set<Object>) requestAdapter.getContext().get(DecorateInstruction.DECORATION_CONTEXT_KEY);
        if (decorations != null) {
            querqyQueryCache.put(query, Collections.singletonMap("decorations", decorations));
        }

        return query;

    }

    public Map<String, Object> getQuerqyInfoForQuery(Query query) {
        final Map<String, Object> querqyInfo = querqyQueryCache.get(query);
        querqyQueryCache.invalidate(query);
        return querqyInfo;
    }

    void appendFilterQueries(final LuceneQueries queries, final BooleanQuery.Builder builder) {

        if (queries.filterQueries != null) {

            for (final Query query : queries.filterQueries) {

                if (query instanceof BooleanQuery) {

                    final BooleanQuery bq = (BooleanQuery) query;

                    final List<BooleanClause> clauses = bq.clauses();
                    final int minimumNumberShouldMatch = bq.getMinimumNumberShouldMatch();

                    if (clauses.size() < 2 || clauses.size() <= minimumNumberShouldMatch ||
                            clauses.stream().allMatch(booleanClause ->
                                    BooleanClause.Occur.MUST_NOT.equals(booleanClause.getOccur()))) {

                        for (final BooleanClause clause : clauses) {
                            if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
                                builder.add(clause.getQuery(), BooleanClause.Occur.MUST_NOT);
                            } else {
                                builder.add(clause.getQuery(), BooleanClause.Occur.FILTER);
                            }
                        }
                    } else {
                        builder.add(query, BooleanClause.Occur.FILTER);
                    }
                } else {
                    builder.add(query, BooleanClause.Occur.FILTER);
                }

            }
        }
    }

}
