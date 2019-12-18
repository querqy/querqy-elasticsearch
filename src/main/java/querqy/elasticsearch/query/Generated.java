package querqy.elasticsearch.query;

import static querqy.elasticsearch.query.RequestUtils.paramToQueryFieldsAndBoosting;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class Generated implements NamedWriteable, ToXContent {

    public static final String NAME = "generated";


    public static final ObjectParser<Generated, Void> PARSER = new ObjectParser<>(NAME, Generated::new);
    private static final ParseField FIELD_QUERY_FIELDS = new ParseField("query_fields");
    private static final ParseField FIELD_FIELD_BOOST_FACTOR = new ParseField("field_boost_factor");

    static {
        PARSER.declareStringArray(Generated::setQueryFieldsAndBoostings, FIELD_QUERY_FIELDS);
        PARSER.declareFloat(Generated::setFieldBoostFactor, FIELD_FIELD_BOOST_FACTOR);
    }

    private Map<String, Float> queryFieldsAndBoostings = null;
    private Float fieldBoostFactor = null;

    public Generated() {}

    public Generated(final List<String> queryFieldsAndBoostings) {
        setQueryFieldsAndBoostings(queryFieldsAndBoostings);
    }

    public Generated(final StreamInput in) throws IOException {

        final int numGeneratedFields = in.readInt();
        if (numGeneratedFields > 0) {
            queryFieldsAndBoostings = new HashMap<>(numGeneratedFields);
            for (int i = 0; i < numGeneratedFields; i++) {
                queryFieldsAndBoostings.put(in.readString(), in.readFloat());
            }
        }
        fieldBoostFactor = in.readOptionalFloat();

    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        final int numFields = queryFieldsAndBoostings == null ? 0 : queryFieldsAndBoostings.size();
        out.writeInt(numFields);
        if (numFields > 0) {
            for (final Map.Entry<String, Float> entry : queryFieldsAndBoostings.entrySet()) {
                out.writeString(entry.getKey());
                out.writeFloat(entry.getValue());
            }
        }
        out.writeOptionalFloat(fieldBoostFactor);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();

        if (queryFieldsAndBoostings != null && !queryFieldsAndBoostings.isEmpty()) {
            builder.startArray(FIELD_QUERY_FIELDS.getPreferredName());
            for (final Map.Entry<String, Float> fieldEntry : queryFieldsAndBoostings.entrySet()) {
                final float boost = fieldEntry.getValue();
                if (boost == 1f) {
                    builder.value(fieldEntry.getKey());
                } else {
                    builder.value(fieldEntry.getKey() + "^" + boost);
                }
            }
            builder.endArray();
        }

        if (fieldBoostFactor != null) {
            builder.field(FIELD_FIELD_BOOST_FACTOR.getPreferredName(), fieldBoostFactor);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Generated)) return false;
        final Generated generated = (Generated) o;
        return Objects.equals(queryFieldsAndBoostings, generated.queryFieldsAndBoostings) &&
                Objects.equals(fieldBoostFactor, generated.fieldBoostFactor);
    }

    @Override
    public int hashCode() {

        return Objects.hash(queryFieldsAndBoostings, fieldBoostFactor);
    }

    public void setQueryFieldsAndBoostings(final List<String> queryFieldsAndBoostings) {
        this.queryFieldsAndBoostings = paramToQueryFieldsAndBoosting(queryFieldsAndBoostings);
    }

    public Map<String, Float> getQueryFieldsAndBoostings() {
        return queryFieldsAndBoostings == null ? Collections.emptyMap() : queryFieldsAndBoostings;
    }

    public Optional<Float> getFieldBoostFactor() {
        return Optional.ofNullable(fieldBoostFactor);
    }

    public void setFieldBoostFactor(final Float fieldBoostFactor) {
        this.fieldBoostFactor = fieldBoostFactor;
    }
}
