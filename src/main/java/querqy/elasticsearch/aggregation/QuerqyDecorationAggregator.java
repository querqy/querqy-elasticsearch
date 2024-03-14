package querqy.elasticsearch.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

public final class QuerqyDecorationAggregator extends MetricsAggregator {

    final Set<Object> decorations;

    public QuerqyDecorationAggregator(final String name, final AggregationContext context, final Map<String, Object> metadata, final Set<Object> decorations)
        throws IOException {
        super(name, context, null, metadata);
        this.decorations = decorations;
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(final LeafReaderContext ctx, final LeafBucketCollector sub) {
        // No sub-aggregations
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation buildAggregation(final long l) {
        StreamOutput.checkWriteable(decorations);
        return new InternalDecorationAggregation(
                name,
                new ArrayList<>(decorations),
                metadata()
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException(
            "querqy_decoration aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations"
        );
    }

}
