package querqy.elasticsearch.aggregation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class QuerqyDecorationAggregationBuilder extends AbstractAggregationBuilder<QuerqyDecorationAggregationBuilder> {

    public static final String NAME = "decorations";

    public static final ObjectParser<QuerqyDecorationAggregationBuilder, String> PARSER =
            new ObjectParser<>(NAME, QuerqyDecorationAggregationBuilder::new);

    public QuerqyDecorationAggregationBuilder() {
        super(NAME);
    }

    public QuerqyDecorationAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    protected QuerqyDecorationAggregationBuilder(QuerqyDecorationAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new QuerqyDecorationAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        return new QuerqyDecorationAggregatorFactory(name, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // no state to write out
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        return super.equals(obj);
    }

}
