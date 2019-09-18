package querqy.elasticsearch;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryShardContext;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.lucene.LuceneQueries;
import querqy.lucene.LuceneSearchEngineRequestAdapter;
import querqy.lucene.QueryParsingController;
import querqy.rewrite.RewriteChain;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class QuerqyProcessor {

    private static final RewriteChain EMPTY_REWRITE_CHAIN = new RewriteChain(Collections.emptyList());

    //private final Rewriters rewriters;

    private RewriterShardContexts rewriterShardContexts;

    public QuerqyProcessor(final RewriterShardContexts rewriterShardContexts) {
        this.rewriterShardContexts = rewriterShardContexts;
    }

    public Query parseQuery(final QuerqyQueryBuilder queryBuilder, final QueryShardContext shardContext)
            throws LuceneSearchEngineRequestAdapter.SyntaxException {

        final List<Rewriter> rewriters = queryBuilder.getRewriters();

        final RewriteChain rewriteChain = rewriters == null || rewriters.isEmpty()
                ? EMPTY_REWRITE_CHAIN : rewriterShardContexts.getRewriteChain(rewriters.stream().map(Rewriter::getName)
                .collect(Collectors.toList()), shardContext);

        final DismaxSearchEngineRequestAdapter requestAdapter =
                new DismaxSearchEngineRequestAdapter(queryBuilder, rewriteChain, shardContext);

        final QueryParsingController controller = new QueryParsingController(requestAdapter);
        final LuceneQueries queries = controller.process();


//        // TODO: make decos part of the general Querqy object model
//        final Set<Object> decorations = (Set<Object>) requestAdapter.getContext().get(DecorateInstruction.CONTEXT_KEY);

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

        if (queries.filterQueries != null) {
            for (final Query query : queries.filterQueries) {
                builder.add(query, BooleanClause.Occur.FILTER);
            }
        }


        return builder.build();

    }

}
