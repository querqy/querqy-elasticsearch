package querqy.elasticsearch.aggregation;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.ScriptedMetric;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class InternalDecorationAggregation extends InternalAggregation implements ScriptedMetric {

    private final List<Object> aggregations;

    InternalDecorationAggregation(String name, List<Object> aggregations, Map<String, Object> metadata) {
        super(name, metadata);
        this.aggregations = aggregations;
    }

    public InternalDecorationAggregation(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_7_8_0)) {
            aggregations = singletonList(in.readGenericValue());
        } else {
            aggregations = in.readList(StreamInput::readGenericValue);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_7_8_0)) {
            if (aggregations.size() > 1) {
                /*
                 * If aggregations has more than one entry we're trying to
                 * serialize an unreduced aggregation. This *should* only
                 * happen when we're returning a scripted_metric over cross
                 * cluster search.
                 */
                throw new IllegalArgumentException("querqy doesn't support cross cluster search until 7.8.0");
            }
            out.writeGenericValue(aggregations.get(0));
        } else {
            out.writeCollection(aggregations, StreamOutput::writeGenericValue);
        }
    }

    @Override
    public String getWriteableName() {
        return QuerqyDecorationAggregationBuilder.NAME;
    }

    @Override
    public Object aggregation() {
        if (aggregations.size() != 1) {
            throw new IllegalStateException("aggregation was not reduced");
        }
        return aggregations.get(0);
    }

    List<Object> aggregationsList() {
        return aggregations;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Object> aggregationObjects = new ArrayList<>();
        for (InternalAggregation aggregation : aggregations) {
            InternalDecorationAggregation mapReduceAggregation = (InternalDecorationAggregation) aggregation;
            aggregationObjects.addAll(mapReduceAggregation.aggregations);
        }
        InternalDecorationAggregation firstAggregation = ((InternalDecorationAggregation) aggregations.get(0));
        List<Object> aggregation;
        if (reduceContext.isFinalReduce()) {
            aggregation = Collections.singletonList(aggregationObjects);
        } else {
            // if we are not an final reduce we have to maintain all the aggs from all the incoming one
            // until we hit the final reduce phase.
            aggregation = aggregationObjects;
        }
        return new InternalDecorationAggregation(firstAggregation.getName(), aggregation, getMetadata());
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return aggregation();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), aggregation());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalDecorationAggregation other = (InternalDecorationAggregation) obj;
        return Objects.equals(aggregations, other.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggregations);
    }

}
