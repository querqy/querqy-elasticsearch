package querqy.elasticsearch.query;

import org.elasticsearch.common.xcontent.ParseField;
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
    private static final ParseField FIELD_LOGGED = new ParseField("_logged");

    static {
        PARSER.declareString(InfoLoggingSpec::setId, FIELD_ID);
        PARSER.declareString(InfoLoggingSpec::setPayloadType, FIELD_TYPE);
        PARSER.declareBoolean(InfoLoggingSpec::setLogged, FIELD_LOGGED);
    }

    private String id = null;
    private LogPayloadType payloadType = LogPayloadType.NONE;
    /**
     * Iff true, the info logging message was already logged in this request. This is not a property to be set by the
     * user. It helps us avoid logging twice if the query is parsed twice per node, which can happen in a multi-shard
     * request (in the query and fetch phases).
     */
    private Boolean logged;

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
        logged = in.readOptionalBoolean();
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();

        if (id != null) {
            builder.field(FIELD_ID.getPreferredName(), id);
        }

        builder.field(FIELD_TYPE.getPreferredName(), payloadType.name());

        if (logged != null) {
            builder.field(FIELD_LOGGED.getPreferredName(), logged);
        }

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
        out.writeOptionalBoolean(logged);
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

    public boolean isLogged() {
        return logged != null && logged;
    }

    public void setLogged(final boolean logged) {
        this.logged = logged;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof InfoLoggingSpec)) return false;
        // do not include logged into comparison in case this is used as a cache key
        final InfoLoggingSpec that = (InfoLoggingSpec) o;
        return Objects.equals(id, that.id) && payloadType == that.payloadType;
    }

    @Override
    public int hashCode() {
        // do not include logged into comparison in case this is used as a cache key
        return Objects.hash(id, payloadType);
    }
}
