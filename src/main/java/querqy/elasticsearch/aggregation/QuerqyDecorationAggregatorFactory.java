package querqy.elasticsearch.aggregation;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
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
import java.util.stream.Collectors;

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
        return new QuerqyDecorationAggregator(name, context, metadata, collectAllDecorations(decoratedQueries));
    }


    private Set<Object> collectAllDecorations(Set<DecoratedQuery<?>> decoratedQueries) {
        return decoratedQueries.stream().flatMap(decoratedQuery -> decoratedQuery.getDecorations().stream()).collect(Collectors.toSet());
    }

    private Set<DecoratedQuery<?>> getDecoratedQueries(Query query) {
        if (query == null) {
            return Collections.emptySet();
        }
        Set<DecoratedQuery<?>> decoratedQueries = new HashSet<>();
        query.visit(new QueryVisitor() {
            @Override
            public void visitLeaf(Query query) {
                if (query instanceof DecoratedQuery<?>) {
                    decoratedQueries.add((DecoratedQuery<?>) query);
                }
            }
        });
        return decoratedQueries;
    }

}
