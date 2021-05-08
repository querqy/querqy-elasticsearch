package querqy.elasticsearch.query;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import querqy.elasticsearch.infologging.LogPayloadType;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class InfoLoggingSpec implements NamedWriteable, ToXContent {

    public static final String NAME = "info_logging";

    public static final ObjectParser<InfoLoggingSpec, Void> PARSER = new ObjectParser<>(NAME, InfoLoggingSpec::new);
    private static final ParseField FIELD_ID = new ParseField("id");
    private static final ParseField FIELD_TYPE = new ParseField("type");

    static {
        PARSER.declareString(InfoLoggingSpec::setId, FIELD_ID);
        PARSER.declareString(InfoLoggingSpec::setPayloadType, FIELD_TYPE);
    }

    private String id = null;
    private LogPayloadType payloadType = LogPayloadType.NONE;

    public InfoLoggingSpec() {}

    public InfoLoggingSpec(final LogPayloadType payloadType) {
        this(payloadType, null);
    }

    public InfoLoggingSpec(final LogPayloadType payloadType, final String id) {
        this.payloadType = payloadType;
        this.id = id;
    }

    public InfoLoggingSpec(final StreamInput in) throws IOException {
        id = in.readOptionalString();
        setPayloadType(in.readString());
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field(FIELD_ID.getPreferredName(), id);
        }
        builder.field(FIELD_TYPE.getPreferredName(), payloadType.name());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalString(id);
        out.writeString(payloadType.name());
    }

    public Optional<String> getId() {
        return Optional.ofNullable(id);
    }

    public void setId(final String id) {
        this.id = id;
    }

    public void setPayloadType(final String type) {
        if (type == null){
            this.payloadType = LogPayloadType.NONE;
        } else {
            switch (type.toUpperCase(Locale.ROOT)) {
                case "NONE":
                    this.payloadType = LogPayloadType.NONE;
                    break;
                case "REWRITER_ID":
                    this.payloadType = LogPayloadType.REWRITER_ID;
                    break;
                case "DETAIL":
                    this.payloadType = LogPayloadType.DETAIL;
                    break;
                default:
                    throw new IllegalArgumentException("Invalid payload type " + type);
            }
        }
    }

    public LogPayloadType getPayloadType() {
        return payloadType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof InfoLoggingSpec)) return false;
        final InfoLoggingSpec that = (InfoLoggingSpec) o;
        return Objects.equals(id, that.id) && payloadType == that.payloadType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payloadType);
    }
}
