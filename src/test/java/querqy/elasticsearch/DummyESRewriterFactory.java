package querqy.elasticsearch;

import org.elasticsearch.index.shard.IndexShard;
import querqy.model.ExpandedQuery;
import querqy.model.Term;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.SearchEngineRequestAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DummyESRewriterFactory extends ESRewriterFactory {

    protected DummyESRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) {

    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {
        final Object error = config.get("error");
        return error == null ? null : Collections.singletonList(error.toString());
    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard)  {
        return new RewriterFactory(rewriterId) {
            @Override
            public QueryRewriter createRewriter(final ExpandedQuery input,
                                                final SearchEngineRequestAdapter searchEngineRequestAdapter) {
                return query -> query;
            }

            @Override
            public Set<Term> getGenerableTerms() {
                return Collections.emptySet();
            }
        };
    }
}
