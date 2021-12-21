package querqy.elasticsearch.query;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class BoostingQueries implements NamedWriteable, ToXContent {

    private static final ParseField FIELD_REWRITTEN_QUERIES = new ParseField("rewritten_queries");
    private static final ParseField FIELD_PHRASE_BOOSTS = new ParseField("phrase_boosts");

    public static final ObjectParser<BoostingQueries, Void> PARSER = new ObjectParser<>("boosting_queries",
            BoostingQueries::new);


    static {

        PARSER.declareObject(BoostingQueries::setRewrittenQueries, RewrittenQueries.PARSER, FIELD_REWRITTEN_QUERIES);
        PARSER.declareObject(BoostingQueries::setPhraseBoosts, PhraseBoosts.PARSER, FIELD_PHRASE_BOOSTS);
    }

    private RewrittenQueries rewrittenQueries = null;
    private PhraseBoosts phraseBoosts = null;

    public BoostingQueries() {}

    public BoostingQueries(final StreamInput in) throws IOException {
        this.rewrittenQueries = in.readOptionalWriteable(RewrittenQueries::new);
        this.phraseBoosts = in.readOptionalWriteable(PhraseBoosts::new);
    }


    @Override
    public String getWriteableName() {
        return "boosting_queries";
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(rewrittenQueries);
        out.writeOptionalWriteable(phraseBoosts);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();
        if (rewrittenQueries != null) {
            builder.field(FIELD_REWRITTEN_QUERIES.getPreferredName(), rewrittenQueries);
        }
        if (phraseBoosts != null) {
            builder.field(FIELD_PHRASE_BOOSTS.getPreferredName(), phraseBoosts);
        }

        builder.endObject();

        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public Optional<RewrittenQueries> getRewrittenQueries() {

        return Optional.ofNullable(rewrittenQueries);
    }

    public void setRewrittenQueries(final RewrittenQueries rewrittenQueries) {
        this.rewrittenQueries = rewrittenQueries;
    }

    public BoostingQueries rewrittenQueries(final RewrittenQueries rewrittenQueries) {
        setRewrittenQueries(rewrittenQueries);
        return this;
    }

    public PhraseBoosts getPhraseBoosts() {
        return phraseBoosts;
    }

    public void setPhraseBoosts(final PhraseBoosts phraseBoosts) {
        this.phraseBoosts = phraseBoosts;
    }

    public BoostingQueries phraseBoosts(final PhraseBoosts phraseBoosts) {
        setPhraseBoosts(phraseBoosts);
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof BoostingQueries)) return false;
        final BoostingQueries that = (BoostingQueries) o;
        return Objects.equals(rewrittenQueries, that.rewrittenQueries) &&
                Objects.equals(phraseBoosts, that.phraseBoosts);
    }

    @Override
    public int hashCode() {

        return Objects.hash(rewrittenQueries, phraseBoosts);
    }
}
