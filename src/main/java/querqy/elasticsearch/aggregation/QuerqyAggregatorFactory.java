package querqy.elasticsearch.aggregation;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QuerqyAggregatorFactory extends AggregatorFactory {

    public QuerqyAggregatorFactory(
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
        List<Object> decorations =
                query instanceof DecoratedQuery ?
                        ((DecoratedQuery) query).getDecorations() :
                        Collections.emptyList();
        return new QuerqyAggregator(name, context, metadata, decorations);
    }
}
