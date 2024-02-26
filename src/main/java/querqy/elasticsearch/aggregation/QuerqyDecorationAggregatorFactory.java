package querqy.elasticsearch.aggregation;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class QuerqyDecorationAggregatorFactory extends AggregatorFactory {

    public QuerqyDecorationAggregatorFactory(
        String name,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
    }

    @Override
    public Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        if (parent != null) {
            throw new IllegalArgumentException(
                "Aggregation ["
                    + parent.name()
                    + "] cannot have a querqy_decoration "
                    + "sub-aggregation ["
                    + name
                    + "]. querqy_decoration aggregations can only be defined as top level aggregations"
            );
        }
        if (cardinality != CardinalityUpperBound.ONE) {
            throw new AggregationExecutionException("Aggregation [" + name() + "] must have cardinality 1 but was [" + cardinality + "]");
        }
        Query query = context.subSearchContext() == null ? null : context.subSearchContext().query();
        Set<DecoratedQuery<?>> decoratedQueries = getDecoratedQueries(query);
        Set<Object> decorations;
        if (decoratedQueries.isEmpty()) {
            decorations = Collections.emptySet();
        } else {
            decorations = new HashSet<>();
            decoratedQueries.forEach(decoratedQuery -> decorations.addAll(decoratedQuery.getDecorations()));
        }
        return new QuerqyDecorationAggregator(name, context, metadata, decorations);
    }

    private Set<DecoratedQuery<?>> getDecoratedQueries(Query query) {
        if (query instanceof DecoratedQuery) {
            return Collections.singleton((DecoratedQuery<?>) query);
        }
        if (query instanceof ConstantScoreQuery) {
            return getDecoratedQueries(((ConstantScoreQuery) query).getQuery());
        }
        if (query instanceof BooleanQuery) {
            Set<DecoratedQuery<?>> decoratedQueries = new HashSet<>();
            ((BooleanQuery) query).clauses().forEach(
                    booleanClause -> decoratedQueries.addAll(getDecoratedQueries(booleanClause.getQuery()))
            );
            return decoratedQueries;
        }
        return Collections.emptySet();
    }
}
