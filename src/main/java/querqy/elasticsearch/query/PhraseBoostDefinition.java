package querqy.elasticsearch.query;

import static querqy.elasticsearch.query.RequestUtils.paramToQueryFieldsAndBoosting;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import querqy.lucene.PhraseBoosting;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PhraseBoostDefinition implements NamedWriteable, ToXContent {

    static final ObjectParser<PhraseBoostDefinition, Void> PARSER = new ObjectParser<>(
            "phrase_boost_definition", PhraseBoostDefinition::new);

    private static final ParseField FIELD_SLOP = new ParseField("slop");
    private static final ParseField FIELD_FIELDS = new ParseField("fields");

    static {
        PARSER.declareInt(PhraseBoostDefinition::setSlop, FIELD_SLOP);
        PARSER.declareStringArray(PhraseBoostDefinition::setFields, FIELD_FIELDS);
    }


    private int slop = 0;
    private Map<String, Float> queryFieldsAndBoostings;

    public PhraseBoostDefinition() {}

    public PhraseBoostDefinition(final int slop, final List<String> fields) {
        setSlop(slop);
        setFields(fields);
    }

    public PhraseBoostDefinition(final StreamInput in) throws IOException {
        slop = in.readInt();
        final int numFields = in.readInt();
        queryFieldsAndBoostings = new HashMap<>(numFields);
        for (int i = 0; i < numFields; i++) {
            queryFieldsAndBoostings.put(in.readString(), in.readFloat());
        }
    }


    public void setSlop(final int slop) {
        this.slop = slop;
    }

    public void setFields(final List<String> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("Query fields must not be null");
        }
        this.queryFieldsAndBoostings = paramToQueryFieldsAndBoosting(fields);
    }

    public List<PhraseBoosting.PhraseBoostFieldParams> toPhraseBoostFieldParams(final PhraseBoosting.NGramType nGramType) {
        return queryFieldsAndBoostings.entrySet().stream()
                .map(entry -> new PhraseBoosting.PhraseBoostFieldParams(entry.getKey(), nGramType, slop, entry.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public String getWriteableName() {
        return "phraseBoostDefinition";
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeInt(slop);
        out.writeInt(queryFieldsAndBoostings.size());
        for (Map.Entry<String, Float> entry : queryFieldsAndBoostings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeFloat(entry.getValue());
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();
        builder.field(FIELD_SLOP.getPreferredName(), slop);

        if (queryFieldsAndBoostings != null && !queryFieldsAndBoostings.isEmpty()) {
            builder.startArray(FIELD_FIELDS.getPreferredName());
            for (final Map.Entry<String, Float> fieldEntry : queryFieldsAndBoostings.entrySet()) {
                builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
            }
            builder.endArray();
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
        if (!(o instanceof PhraseBoostDefinition)) return false;
        final PhraseBoostDefinition that = (PhraseBoostDefinition) o;
        return slop == that.slop &&
                Objects.equals(queryFieldsAndBoostings, that.queryFieldsAndBoostings);
    }

    @Override
    public int hashCode() {

        return Objects.hash(slop, queryFieldsAndBoostings);
    }
}
