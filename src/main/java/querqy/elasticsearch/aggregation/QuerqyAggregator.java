package querqy.elasticsearch.aggregation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public final class QuerqyAggregator extends MetricsAggregator {

    public QuerqyAggregator(String name, AggregationContext context, Map<String, Object> metadata)
        throws IOException {
        super(name, context, null, metadata);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // No sub-aggregations
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation buildAggregation(long l) {
        Map<String, Object> metadata = metadata();
        Map<String, Object> result = metadata != null && !metadata.isEmpty() ? metadata : Collections.emptyMap();
        StreamOutput.checkWriteable(result);
        return new InternalQuerqy(
                name,
                Collections.singletonList(result),
                null
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException(
            "querqy_decoration aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations"
        );
    }

}
