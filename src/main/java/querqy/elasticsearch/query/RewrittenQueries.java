package querqy.elasticsearch.query;

import static querqy.elasticsearch.query.RequestUtils.paramToQuerySimilarityScoring;
import static querqy.elasticsearch.query.RequestUtils.querySimilarityScoringToString;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import querqy.lucene.QuerySimilarityScoring;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class RewrittenQueries implements NamedWriteable, ToXContent {

    static final ObjectParser<RewrittenQueries, Void> PARSER = new ObjectParser<>(
            "rewritten_queries_boosts", RewrittenQueries::new);

    private static final ParseField FIELD_USE_FIELD_BOOST = new ParseField("use_field_boost");
    private static final ParseField FIELD_NEGATIVE_WEIGHT = new ParseField("negative_query_weight");
    private static final ParseField FIELD_POSITIVE_WEIGHT = new ParseField("positive_query_weight");
    private static final ParseField FIELD_SIMILARITY_SCORING = new ParseField("similarity_scoring");

    static {
        PARSER.declareBoolean(RewrittenQueries::setUseFieldBoosts, FIELD_USE_FIELD_BOOST);
        PARSER.declareFloat(RewrittenQueries::setNegativeWeight, FIELD_NEGATIVE_WEIGHT);
        PARSER.declareFloat(RewrittenQueries::setPositiveWeight, FIELD_POSITIVE_WEIGHT);
        PARSER.declareString(RewrittenQueries::setSimilarityScoring, FIELD_SIMILARITY_SCORING);
    }


    private boolean useFieldBoosts = true;
    private float positiveWeight = 1f;
    private float negativeWeight = 1f;
    private QuerySimilarityScoring similarityScoring = null;


    public RewrittenQueries() {}

    public RewrittenQueries(final StreamInput in) throws IOException {
        useFieldBoosts = in.readBoolean();
        positiveWeight = in.readFloat();
        negativeWeight = in.readFloat();
        final String strSimilarityScoring = in.readOptionalString();
        similarityScoring = strSimilarityScoring == null
                ? null : QuerySimilarityScoring.valueOf(strSimilarityScoring);
    }

    @Override
    public String getWriteableName() {
        return "rewritten_queries_boosts";
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeBoolean(useFieldBoosts);
        out.writeFloat(positiveWeight);
        out.writeFloat(negativeWeight);
        out.writeOptionalString(querySimilarityScoringToString(similarityScoring).orElse(null));
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {

        builder.startObject();

        builder.field(FIELD_USE_FIELD_BOOST.getPreferredName(), useFieldBoosts);
        builder.field(FIELD_NEGATIVE_WEIGHT.getPreferredName(), negativeWeight);
        builder.field(FIELD_POSITIVE_WEIGHT.getPreferredName(), positiveWeight);
        final Optional<String> scoringOpt = querySimilarityScoringToString(similarityScoring);
        if (scoringOpt.isPresent()) {
            builder.field(FIELD_SIMILARITY_SCORING.getPreferredName(), scoringOpt.get());
        }

        builder.endObject();

        return builder;

    }

    @Override
    public boolean isFragment() {
        return false;
    }

    public boolean isUseFieldBoosts() {
        return useFieldBoosts;
    }

    public void setUseFieldBoosts(boolean useFieldBoosts) {
        this.useFieldBoosts = useFieldBoosts;
    }

    public float getPositiveWeight() {
        return positiveWeight;
    }

    public void setPositiveWeight(float positiveWeight) {
        this.positiveWeight = positiveWeight;
    }

    public float getNegativeWeight() {
        return negativeWeight;
    }

    public void setNegativeWeight(float negativeWeight) {
        this.negativeWeight = negativeWeight;
    }

    public QuerySimilarityScoring getSimilarityScoring() {
        return similarityScoring;
    }

    public void setSimilarityScoring(final String boostQuerySimilarityScoring) {

        this.similarityScoring = paramToQuerySimilarityScoring(boostQuerySimilarityScoring,
                FIELD_SIMILARITY_SCORING);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof RewrittenQueries)) return false;
        final RewrittenQueries that = (RewrittenQueries) o;
        return useFieldBoosts == that.useFieldBoosts &&
                Float.compare(that.positiveWeight, positiveWeight) == 0 &&
                Float.compare(that.negativeWeight, negativeWeight) == 0 &&
                similarityScoring == that.similarityScoring;
    }

    @Override
    public int hashCode() {

        return Objects.hash(useFieldBoosts, positiveWeight, negativeWeight, similarityScoring);
    }
}
