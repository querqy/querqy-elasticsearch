package querqy.elasticsearch.aggregation;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class QuerqyAggregationBuilder extends AbstractAggregationBuilder<QuerqyAggregationBuilder> {

    public static final String NAME = "decorations";

    private static final ParseField PARAMS_FIELD = new ParseField("params");

    public static final ObjectParser<QuerqyAggregationBuilder, String> PARSER =
            new ObjectParser<>(NAME, QuerqyAggregationBuilder::new);

    static {
        PARSER.declareObject(QuerqyAggregationBuilder::params, (p, name) -> p.map(), PARAMS_FIELD);
    }

    private Map<String, Object> params;

    public QuerqyAggregationBuilder() {
        super(NAME);
    }

    public QuerqyAggregationBuilder(String name) {
        super(name);
    }

    protected QuerqyAggregationBuilder(QuerqyAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.params = clone.params;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new QuerqyAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public QuerqyAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            params = in.readMap();
        }
    }

    /**
     * Set parameters that will be available in the {@code init},
     * {@code map} and {@code combine} phases.
     */
    public QuerqyAggregationBuilder params(Map<String, Object> params) {
        if (params == null) {
            throw new IllegalArgumentException("[params] must not be null: [" + name + "]");
        }
        this.params = params;
        return this;
    }

    /**
     * Get parameters that will be available in the {@code init},
     * {@code map} and {@code combine} phases.
     */
    public Map<String, Object> params() {
        return params;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        return new QuerqyAggregatorFactory(name, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject();
        if (params != null) {
            builder.field(PARAMS_FIELD.getPreferredName());
            builder.map(params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        boolean hasParams = params != null;
        out.writeBoolean(hasParams);
        if (hasParams) {
            out.writeMap(params);
        }
    }

    public static QuerqyAggregationBuilder fromXContent(final XContentParser parser) {
        final QuerqyAggregationBuilder builder;
        try {
            builder = PARSER.apply(parser, null);
        } catch (final IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), params);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        QuerqyAggregationBuilder other = (QuerqyAggregationBuilder) obj;
        return Objects.equals(params, other.params);
    }

}
