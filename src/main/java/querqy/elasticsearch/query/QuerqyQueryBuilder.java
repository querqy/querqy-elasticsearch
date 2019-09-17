package querqy.elasticsearch.query;

import static querqy.elasticsearch.query.RequestUtils.fieldBoostModelToString;
import static querqy.elasticsearch.query.RequestUtils.paramToFieldBoostModel;
import static querqy.elasticsearch.query.RequestUtils.paramToQueryFieldsAndBoosting;
import static querqy.elasticsearch.query.RequestUtils.paramToQuerySimilarityScoring;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.lucene.LuceneSearchEngineRequestAdapter;
import querqy.lucene.QuerySimilarityScoring;
import querqy.lucene.rewrite.SearchFieldsAndBoosting;
import querqy.lucene.rewrite.SearchFieldsAndBoosting.FieldBoostModel;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QuerqyQueryBuilder extends AbstractQueryBuilder<QuerqyQueryBuilder> {

    public static final String NAME = "querqy";

    private static final ParseField FIELD_MATCHING_QUERY = new ParseField("matching_query");
    private static final ParseField FIELD_BOOSTING_QUERIES = new ParseField("boosting_queries");

    private static final ParseField FIELD_TIE_BREAKER = new ParseField("tie_breaker");
    private static final ParseField FIELD_MINIMUM_SHOULD_MATCH = new ParseField("minimum_should_match");
    private static final ParseField FIELD_QUERY_FIELDS = new ParseField("query_fields");
    private static final ParseField FIELD_FIELD_BOOST_MODEL = new ParseField("field_boost_model");
    private static final ParseField FIELD_GENERATED_QUERY_FIELDS = new ParseField("generated_query_fields");

    private static final ParseField FIELD_REWRITERS = new ParseField("rewriters");



    private static final ObjectParser<QuerqyQueryBuilder, Void> PARSER = new ObjectParser<>(NAME,
            QuerqyQueryBuilder::new);


    static {
        declareStandardFields(PARSER);
        PARSER.declareFloat(QuerqyQueryBuilder::setTieBreaker, FIELD_TIE_BREAKER);
        PARSER.declareString(QuerqyQueryBuilder::setMinimumShouldMatch, FIELD_MINIMUM_SHOULD_MATCH);
        PARSER.declareString(QuerqyQueryBuilder::setFieldBoostModel, FIELD_FIELD_BOOST_MODEL);
        PARSER.declareStringArray(QuerqyQueryBuilder::setQueryFieldsAndBoostings, FIELD_QUERY_FIELDS);
        PARSER.declareStringArray(QuerqyQueryBuilder::setRewriters, FIELD_REWRITERS);
        PARSER.declareStringArray(QuerqyQueryBuilder::setGeneratedQueryFieldsAndBoostings,
                FIELD_GENERATED_QUERY_FIELDS);
        PARSER.declareObject(QuerqyQueryBuilder::setMatchingQuery, MatchingQuery.PARSER, FIELD_MATCHING_QUERY);
        PARSER.declareObject(QuerqyQueryBuilder::setBoostingQueries, BoostingQueries.PARSER, FIELD_BOOSTING_QUERIES);
    }


    private Float tieBreaker;
    private String minimumShouldMatch;
    private Map<String, Float> queryFieldsAndBoostings;
    private List<String> queryFields;
    private List<String> rewriters = Collections.emptyList();
    private Map<String, Float> generatedQueryFieldsAndBoostings;
    private BoostingQueries boostingQueries = null;
    private MatchingQuery matchingQuery = null;
    private FieldBoostModel fieldBoostModel = null;


    private QuerqyProcessor querqyProcessor;

    public QuerqyQueryBuilder() {
        super();
    }


    public QuerqyQueryBuilder(final QuerqyProcessor querqyProcessor) {
        super();
        this.querqyProcessor = querqyProcessor;
    }

    public QuerqyQueryBuilder(final StreamInput in, final QuerqyProcessor querqyProcessor) throws IOException {
        super(in);
        this.querqyProcessor = querqyProcessor;

        matchingQuery = new MatchingQuery(in);

        queryFields = in.readStringList();
        setQueryFieldsAndBoostings(queryFields);

        final int numGeneratedFields = in.readInt();
        if (numGeneratedFields > 0) {
            generatedQueryFieldsAndBoostings = new HashMap<>(numGeneratedFields);
            for (int i = 0; i < numGeneratedFields; i++) {
                generatedQueryFieldsAndBoostings.put(in.readString(), in.readFloat());
            }
        }

        minimumShouldMatch = in.readOptionalString();

        tieBreaker = in.readOptionalFloat();

        final String strFieldBoostModel = in.readOptionalString();
        fieldBoostModel = strFieldBoostModel == null
                ? null : FieldBoostModel.valueOf(strFieldBoostModel);

        boostingQueries = in.readOptionalWriteable(BoostingQueries::new);

        rewriters = in.readStringList();
    }

    @Override
    protected void doWriteTo(final StreamOutput out) throws IOException {
        matchingQuery.writeTo(out);
        out.writeStringCollection(queryFields);

        final int numFields = generatedQueryFieldsAndBoostings == null ? 0 : generatedQueryFieldsAndBoostings.size();
        out.writeInt(numFields);
        if (numFields > 0) {
            for (Map.Entry<String, Float> entry : generatedQueryFieldsAndBoostings.entrySet()) {
                out.writeString(entry.getKey());
                out.writeFloat(entry.getValue());
            }
        }

        out.writeOptionalString(minimumShouldMatch);
        out.writeOptionalFloat(tieBreaker);
        out.writeOptionalString(fieldBoostModel == null ? null : fieldBoostModel.name());
        out.writeOptionalWriteable(boostingQueries);

        out.writeStringCollection(rewriters);
    }

    @Override
    protected void doXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);

        builder.field(FIELD_MATCHING_QUERY.getPreferredName(), matchingQuery);

        builder.field(FIELD_QUERY_FIELDS.getPreferredName(), queryFields);

        if (generatedQueryFieldsAndBoostings != null && !generatedQueryFieldsAndBoostings.isEmpty()) {
            builder.startArray(FIELD_GENERATED_QUERY_FIELDS.getPreferredName());
            for (final Map.Entry<String, Float> fieldEntry : generatedQueryFieldsAndBoostings.entrySet()) {
                builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
            }
            builder.endArray();
        }

        if (minimumShouldMatch != null) {
            builder.field(FIELD_MINIMUM_SHOULD_MATCH.getPreferredName(), minimumShouldMatch);
        }

        if (tieBreaker != null) {
            builder.field(FIELD_TIE_BREAKER.getPreferredName(), tieBreaker);
        }

        if (fieldBoostModel != null) {
            final Optional<String> strFieldBoostModel = fieldBoostModelToString(fieldBoostModel);
            if (strFieldBoostModel.isPresent()) {
                builder.field(FIELD_FIELD_BOOST_MODEL.getPreferredName(), strFieldBoostModel);
            }
        }

        if (boostingQueries != null) {
            builder.field(FIELD_BOOSTING_QUERIES.getPreferredName(), boostingQueries);
        }

        if (rewriters != null) {

            builder.startArray(FIELD_REWRITERS.getPreferredName());

            for (final String rewriter : rewriters) {
                builder.value(rewriter);
            }

            builder.endArray();
        }

        builder.endObject();
    }

    public static QuerqyQueryBuilder fromXContent(final XContentParser parser, final QuerqyProcessor querqyProcessor) {
        final QuerqyQueryBuilder builder;
        try {
            builder = PARSER.apply(parser, null);
        } catch (final IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }

        if (builder.matchingQuery == null) {
            throw new ParsingException(parser.getTokenLocation(), "[querqy] requires a matching_query, none specified");
        }
        if (builder.matchingQuery.getQueryString() == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[querqy] requires a query, none specified");
        }
        if (builder.queryFields == null) {
            throw new ParsingException(parser.getTokenLocation(),
                    "[querqy] requires query_fields, none specified");
        }
        builder.setQuerqyProcessor(querqyProcessor);
        return builder;
    }

    @Override
    protected Query doToQuery(final QueryShardContext context) throws IOException {
        try {
            final Query query = querqyProcessor.parseQuery(this, context);
            System.out.println(query);
            return query;
        } catch (final LuceneSearchEngineRequestAdapter.SyntaxException e) {
            throw new IOException(e);
        }
    }

    /**
     * Indicates whether some other {@link org.elasticsearch.index.query.QueryBuilder} object of the same type is
     * "equal to" this one.
     *
     * @param other
     */
    @Override
    protected boolean doEquals(final QuerqyQueryBuilder other) {
        return (this.matchingQuery == other.matchingQuery
                || (this.matchingQuery != null && this.matchingQuery.equals(other.matchingQuery)))
                && Objects.equals(this.queryFields, other.queryFields)
                && Objects.equals(this.generatedQueryFieldsAndBoostings, other.generatedQueryFieldsAndBoostings)
                && Objects.equals(this.minimumShouldMatch, other.minimumShouldMatch)
                && Objects.equals(this.rewriters, other.rewriters)
                && Objects.equals(this.tieBreaker, other.tieBreaker)
                && Objects.equals(this.fieldBoostModel, other.fieldBoostModel)
                && Objects.equals(this.boostingQueries, other.boostingQueries)
                ;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(matchingQuery, queryFields, generatedQueryFieldsAndBoostings, minimumShouldMatch,
                rewriters, tieBreaker, fieldBoostModel, boostingQueries);
    }

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    public QuerqyProcessor getQuerqyProcessor() {
        return querqyProcessor;
    }

    public void setQuerqyProcessor(final QuerqyProcessor querqyProcessor) {
        this.querqyProcessor = querqyProcessor;
    }

    public MatchingQuery getMatchingQuery() {
        return matchingQuery;
    }

    public void setMatchingQuery(final MatchingQuery matchingQuery) {
        this.matchingQuery = matchingQuery;
    }

    public BoostingQueries getBoostingQueries() {
        return boostingQueries;
    }

    public void setBoostingQueries(BoostingQueries boostingQueries) {
        this.boostingQueries = boostingQueries;
    }

    public void setQueryFieldsAndBoostings(final List<String> queryFieldsAndBoostings) {
        if (queryFieldsAndBoostings == null) {
            throw new IllegalArgumentException("Query fields must not be null");
        }
        this.queryFieldsAndBoostings = paramToQueryFieldsAndBoosting(queryFieldsAndBoostings);
        this.queryFields = queryFieldsAndBoostings;
    }

    public void setGeneratedQueryFieldsAndBoostings(final List<String> generatedQueryFieldsAndBoostings) {
        this.generatedQueryFieldsAndBoostings = paramToQueryFieldsAndBoosting(generatedQueryFieldsAndBoostings);
    }

    public void setRewriters(final List<String> rewriters) {
        this.rewriters = rewriters == null ? Collections.emptyList() : rewriters;
    }

    public List<String> getRewriters() {
        return rewriters;
    }

    public Map<String, Float> getQueryFieldsAndBoostings() {
        return queryFieldsAndBoostings;
    }

    public Map<String, Float> getGeneratedQueryFieldsAndBoostings() {
        return generatedQueryFieldsAndBoostings;
    }

    public void setTieBreaker(final float tie) {
        this.tieBreaker = tie;
    }

    public Float getTieBreaker() {
        return tieBreaker;
    }

    public String getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(final String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    public Optional<FieldBoostModel> getFieldBoostModel() {
        return Optional.ofNullable(fieldBoostModel);
    }

    public void setFieldBoostModel(final String fieldBoostModel) {
        this.fieldBoostModel = paramToFieldBoostModel(fieldBoostModel, FIELD_FIELD_BOOST_MODEL);
    }
}
