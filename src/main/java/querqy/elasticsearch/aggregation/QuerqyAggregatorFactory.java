package querqy.elasticsearch.aggregation;

import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import querqy.elasticsearch.QuerqyProcessor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class QuerqyAggregatorFactory extends AggregatorFactory {

    final private QuerqyProcessor querqyProcessor;

    public QuerqyAggregatorFactory(
        String name,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactories,
        Map<String, Object> metadata,
        QuerqyProcessor querqyProcessor
    ) throws IOException {
        super(name, context, parent, subFactories, metadata);
        this.querqyProcessor = Objects.requireNonNull(querqyProcessor);
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
        Map<String, Object> info = querqyProcessor.getQuerqyInfoForQuery(context.subSearchContext().query());
        if (info != null && !info.isEmpty()) {
            if (metadata == null) {
                metadata = new HashMap<>();
            }
            metadata.putAll(info);
        }
        return new QuerqyAggregator(name, context, metadata);
    }
}
