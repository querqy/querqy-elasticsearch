package querqy.elasticsearch.query;

import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.Before;
import org.junit.Test;
import querqy.elasticsearch.QuerqyPlugin;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.lucene.rewrite.DependentTermQueryBuilder;
import querqy.lucene.rewrite.DocumentFrequencyCorrection;
import querqy.lucene.rewrite.IndependentFieldBoost;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class QuerqyQueryBuilderTest extends AbstractQueryTestCase<QuerqyQueryBuilder> {

    QuerqyProcessor querqyProcessor;

    Query query;

    QueryShardContext queryShardContext;

    @Before
    public void setUpMocks() {
        querqyProcessor = mock(QuerqyProcessor.class);
        query = mock(Query.class);
        queryShardContext = mock(QueryShardContext.class);
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(QuerqyPlugin.class);
    }

    @Override
    protected QuerqyQueryBuilder doCreateTestQueryBuilder() {

        QuerqyProcessor querqyProcessor = new QuerqyProcessor(null) {
            @Override
            public Query parseQuery(QuerqyQueryBuilder queryBuilder, QueryShardContext shardContext) {

                final Map<String, Float> boosts = new HashMap<>();
                boosts.put("f1", 1f);
                final BooleanQuery.Builder builder = new BooleanQuery.Builder();
                final DocumentFrequencyCorrection dfc = new DocumentFrequencyCorrection();
                dfc.newClause();
                final Term term = new Term("f1", "test1");
                dfc.prepareTerm(term);

                final DependentTermQueryBuilder.DependentTermQuery termQuery = new DependentTermQueryBuilder
                        .DependentTermQuery(term, dfc, new IndependentFieldBoost(boosts, 1f));
                dfc.finishedUserQuery();

                builder.add(termQuery, BooleanClause.Occur.MUST);
                return builder.build();
            }
        };
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder(querqyProcessor);
        builder.setMatchingQuery(new MatchingQuery("test1"));
        builder.setQueryFieldsAndBoostings(Collections.singletonList("f1"));
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(final QuerqyQueryBuilder querqyQueryBuilder, final Query query,
                                       final SearchContext searchContext) throws IOException {

        assertThat(query, AbstractLuceneQueryTest.bq(AbstractLuceneQueryTest.dtq(BooleanClause.Occur.MUST, "f1", "test1")));

    }

    @Override
    protected boolean isCacheable(final QuerqyQueryBuilder queryBuilder) {
        return true;
    }

    @Test
    public void testWriteReadStreamForMinimalProperties() throws IOException {

        final QuerqyQueryBuilder writeQuerqyQueryBuilder = new QuerqyQueryBuilder(querqyProcessor);
        writeQuerqyQueryBuilder.setMatchingQuery(new MatchingQuery("minimum query string"));
        writeQuerqyQueryBuilder.setQueryFieldsAndBoostings(Collections.singletonList("f0^6.4"));

        final BytesStreamOutput out = new BytesStreamOutput();
        writeQuerqyQueryBuilder.writeTo(out);
        out.flush();
        out.close();

        final QuerqyQueryBuilder readQuerqyQueryBuilder = new QuerqyQueryBuilder(out.bytes().streamInput(),
                querqyProcessor);

        assertEqualBuilders(writeQuerqyQueryBuilder, readQuerqyQueryBuilder);

    }

    @Test
    public void testQueryBuildingIsDelegatedToQuerqyProcessor() throws Exception {

        final QuerqyQueryBuilder writeQuerqyQueryBuilder = new QuerqyQueryBuilder(querqyProcessor);

        writeQuerqyQueryBuilder.setMatchingQuery(new MatchingQuery("minimum query string"));
        writeQuerqyQueryBuilder.setQueryFieldsAndBoostings(Collections.singletonList("f1"));

        when(querqyProcessor.parseQuery(eq(writeQuerqyQueryBuilder), eq(queryShardContext))).thenReturn(query);

        writeQuerqyQueryBuilder.doToQuery(queryShardContext);

        verify(querqyProcessor, times(1)).parseQuery(eq(writeQuerqyQueryBuilder), eq(queryShardContext));

    }

    @Test
    public void testWriteReadJsonForMinimalProperties() throws IOException {

        final QuerqyQueryBuilder writeQuerqyQueryBuilder = new QuerqyQueryBuilder(querqyProcessor);
        writeQuerqyQueryBuilder.setMatchingQuery(new MatchingQuery("minimum query string"));
        writeQuerqyQueryBuilder.setQueryFieldsAndBoostings(Collections.singletonList("f0^6.4"));

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        final XContentBuilder xContentBuilder = new XContentBuilder(JsonXContent.jsonXContent, os);
        xContentBuilder.startObject();
        writeQuerqyQueryBuilder.doXContent(xContentBuilder, null);
        xContentBuilder.endObject();
        xContentBuilder.flush();
        xContentBuilder.close();

        assertEqualBuilders(writeQuerqyQueryBuilder, fromJsonInnerObject(os.toByteArray()));

    }

    @Test
    public void testWriteReadStreamForAllProperties() throws IOException {

        final QuerqyQueryBuilder writeQuerqyQueryBuilder = new QuerqyQueryBuilder(querqyProcessor);
        writeQuerqyQueryBuilder.setMatchingQuery(new MatchingQuery("query string", "on"));
        writeQuerqyQueryBuilder.setQueryFieldsAndBoostings(Arrays.asList("f1^0.4", "f2^3.0"));
        writeQuerqyQueryBuilder.setGenerated(new Generated(Arrays.asList("f3^0.2", "f4^3.2")));
        writeQuerqyQueryBuilder.setMinimumShouldMatch("2<-25% 9<-3");
        writeQuerqyQueryBuilder.setRewriters(Arrays.asList(new Rewriter("common1"), new Rewriter("wordbreak")));
        writeQuerqyQueryBuilder.setTieBreaker(0.7f);
        writeQuerqyQueryBuilder.setFieldBoostModel("prms");

        final PhraseBoosts phraseBoosts = new PhraseBoosts();
        phraseBoosts.setTieBreaker(0.15f);
        phraseBoosts.setFull(new PhraseBoostDefinition(4, Collections.singletonList("ffull1^0.9")));
        phraseBoosts.setBigram(new PhraseBoostDefinition(1, Collections.singletonList("fb1^0.25")));
        phraseBoosts.setTrigram(new PhraseBoostDefinition(3, Arrays.asList("ft1", "ft2^7")));

        final BoostingQueries boostingQueries = new BoostingQueries();
        boostingQueries.setPhraseBoosts(phraseBoosts);

        final RewrittenQueries rewrittenQueries = new RewrittenQueries();
        rewrittenQueries.setNegativeWeight(0.2f);
        rewrittenQueries.setPositiveWeight(0.7f);
        rewrittenQueries.setSimilarityScoring("off");
        rewrittenQueries.setUseFieldBoosts(false);
        boostingQueries.setRewrittenQueries(rewrittenQueries);

        writeQuerqyQueryBuilder.setBoostingQueries(boostingQueries);

        final BytesStreamOutput out = new BytesStreamOutput();
        writeQuerqyQueryBuilder.writeTo(out);
        out.flush();
        out.close();

        final QuerqyQueryBuilder readQuerqyQueryBuilder = new QuerqyQueryBuilder(out.bytes().streamInput(),
                querqyProcessor);

        assertEqualBuilders(writeQuerqyQueryBuilder, readQuerqyQueryBuilder);


    }

    @Test
    public void testWriteReadStreamForAllPropertiesAndRewriterParams() throws IOException {

        final QuerqyQueryBuilder writeQuerqyQueryBuilder = new QuerqyQueryBuilder(querqyProcessor);
        writeQuerqyQueryBuilder.setMatchingQuery(new MatchingQuery("query string", "on"));
        writeQuerqyQueryBuilder.setQueryFieldsAndBoostings(Arrays.asList("f1^0.4", "f2^3.0"));
        writeQuerqyQueryBuilder.setGenerated(new Generated(Arrays.asList("f3^0.2", "f4^3.2")));
        writeQuerqyQueryBuilder.setMinimumShouldMatch("2<-25% 9<-3");

        final Rewriter rewriter1 = new Rewriter("common1");

        final Map<String, Object> params = new HashMap<>();

        final Map<String, Object> criteria = new HashMap<>();
        criteria.put("sort", "prio desc");
        criteria.put("limit", 1);
        params.put("criteria", criteria);
        rewriter1.setParams(params);

        writeQuerqyQueryBuilder.setRewriters(Arrays.asList(rewriter1, new Rewriter("wordbreak")));
        writeQuerqyQueryBuilder.setTieBreaker(0.7f);
        writeQuerqyQueryBuilder.setFieldBoostModel("prms");

        final PhraseBoosts phraseBoosts = new PhraseBoosts();
        phraseBoosts.setTieBreaker(0.15f);
        phraseBoosts.setFull(new PhraseBoostDefinition(4, Collections.singletonList("ffull1^0.9")));
        phraseBoosts.setBigram(new PhraseBoostDefinition(1, Collections.singletonList("fb1^0.25")));
        phraseBoosts.setTrigram(new PhraseBoostDefinition(3, Arrays.asList("ft1", "ft2^7")));

        final BoostingQueries boostingQueries = new BoostingQueries();
        boostingQueries.setPhraseBoosts(phraseBoosts);

        final RewrittenQueries rewrittenQueries = new RewrittenQueries();
        rewrittenQueries.setNegativeWeight(0.2f);
        rewrittenQueries.setPositiveWeight(0.7f);
        rewrittenQueries.setSimilarityScoring("off");
        rewrittenQueries.setUseFieldBoosts(false);
        boostingQueries.setRewrittenQueries(rewrittenQueries);

        writeQuerqyQueryBuilder.setBoostingQueries(boostingQueries);

        final BytesStreamOutput out = new BytesStreamOutput();
        writeQuerqyQueryBuilder.writeTo(out);
        out.flush();
        out.close();

        final QuerqyQueryBuilder readQuerqyQueryBuilder = new QuerqyQueryBuilder(out.bytes().streamInput(),
                querqyProcessor);

        assertEqualBuilders(writeQuerqyQueryBuilder, readQuerqyQueryBuilder);


    }


    @Test
    public void testWriteReadJsonForAllProperties() throws IOException {

        final QuerqyQueryBuilder writeQuerqyQueryBuilder = new QuerqyQueryBuilder(querqyProcessor);
        writeQuerqyQueryBuilder.setMatchingQuery(new MatchingQuery("query string", "on"));
        writeQuerqyQueryBuilder.setQueryFieldsAndBoostings(Arrays.asList("f1^0.4", "f2^3.0"));
        writeQuerqyQueryBuilder.setGenerated(new Generated(Arrays.asList("f3^0.2", "f4^3.2")));
        writeQuerqyQueryBuilder.setMinimumShouldMatch("2<-25% 9<-3");

        final Rewriter rewriter1 = new Rewriter("common1");

        final Map<String, Object> params = new HashMap<>();

        final Map<String, Object> criteria = new HashMap<>();
        criteria.put("sort", "prio desc");
        criteria.put("limit", 1);
        params.put("criteria", criteria);
        rewriter1.setParams(params);

        writeQuerqyQueryBuilder.setRewriters(Arrays.asList(rewriter1, new Rewriter("wordbreak")));
        writeQuerqyQueryBuilder.setTieBreaker(0.7f);
        writeQuerqyQueryBuilder.setFieldBoostModel("prms");

        final PhraseBoosts phraseBoosts = new PhraseBoosts();
        phraseBoosts.setTieBreaker(0.5f);
        phraseBoosts.setFull(new PhraseBoostDefinition(2, Arrays.asList("ffull1^9", "ffull2")));
        phraseBoosts.setBigram(new PhraseBoostDefinition(0, Collections.singletonList("fb1^0.5")));
        phraseBoosts.setTrigram(new PhraseBoostDefinition(1, Arrays.asList("ft1", "ft2")));

        final BoostingQueries boostingQueries = new BoostingQueries();
        boostingQueries.setPhraseBoosts(phraseBoosts);

        final RewrittenQueries rewrittenQueries = new RewrittenQueries();
        rewrittenQueries.setNegativeWeight(0.2f);
        rewrittenQueries.setPositiveWeight(0.7f);
        rewrittenQueries.setSimilarityScoring("off");
        rewrittenQueries.setUseFieldBoosts(false);
        boostingQueries.setRewrittenQueries(rewrittenQueries);

        writeQuerqyQueryBuilder.setBoostingQueries(boostingQueries);

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        final XContentBuilder xContentBuilder = new XContentBuilder(JsonXContent.jsonXContent, os);
        xContentBuilder.startObject();
        writeQuerqyQueryBuilder.doXContent(xContentBuilder, null);
        xContentBuilder.endObject();
        xContentBuilder.flush();
        xContentBuilder.close();

        assertEqualBuilders(writeQuerqyQueryBuilder, fromJsonInnerObject(os.toByteArray()));

    }

    @Test
    public void testThatMissingMatchingQueryCausesParsingException() throws IOException {

        try {
            QuerqyQueryBuilder.fromXContent(XContentHelper.createParser(null, null, new BytesArray("{}"),
                    XContentType.JSON), null);
            fail("missing matching_query must cause Exception");
        } catch (final ParsingException e) {
            assertTrue(e.getMessage().contains("matching_query"));
        }

    }

    @Test
    public void testThatMissingQueryStringCausesParsingException() throws IOException {

        try {
            QuerqyQueryBuilder.fromXContent(XContentHelper.createParser(null, null, new BytesArray("{" +
                            "\"matching_query\": {}}"),
                    XContentType.JSON), null);
            fail("missing query string must cause Exception");
        } catch (final ParsingException e) {
            assertTrue(e.getMessage().contains("[querqy] requires a query"));
        }

    }

    @Test
    public void testThatMissingQueryFieldsCausesParsingException() throws IOException {

        try {
            QuerqyQueryBuilder.fromXContent(XContentHelper.createParser(null, null, new BytesArray("{" +
                            "\"matching_query\": {" +
                            "\"query\": \"hello\"" +
                            "}}"),
                    XContentType.JSON), null);
            fail("missing query fields must cause Exception");
        } catch (final ParsingException e) {
            assertTrue(e.getMessage().contains("query_fields"));
        }

    }


    private void assertEqualBuilders(final QuerqyQueryBuilder builder1, final QuerqyQueryBuilder builder2) {

        assertTrue(Objects.equals(builder1, builder2));
        assertEquals(builder1.hashCode(), builder2.hashCode());

        // do not trust QuerqyQueryBuilder.hashCode/equals only:

        assertEquals(builder1.getMatchingQuery(), builder2.getMatchingQuery());
        assertEquals(builder1.getQueryFieldsAndBoostings(), builder2.getQueryFieldsAndBoostings());
        assertEquals(builder1.getGenerated(), builder2.getGenerated());
        assertEquals(builder1.getMinimumShouldMatch(), builder2.getMinimumShouldMatch());
        assertEquals(builder1.getRewriters(), builder2.getRewriters());
        assertEquals(builder1.getTieBreaker(), builder2.getTieBreaker());
        assertEquals(builder1.getBoostingQueries(), builder2.getBoostingQueries());

    }

    private QuerqyQueryBuilder fromJsonInnerObject(final byte[] bytes) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes);



        ObjectParser<Wrapper, Void> objectParser = new ObjectParser<>("wrapper", Wrapper::new);
        objectParser.declareObject(Wrapper::setQuerqyQueryBuilder,
                (p, c) -> QuerqyQueryBuilder.fromXContent(p, querqyProcessor), new ParseField("querqy"));

        return objectParser.parse(parser, null).querqyQueryBuilder;
    }

    static class Wrapper {
        QuerqyQueryBuilder querqyQueryBuilder;
        public void setQuerqyQueryBuilder(QuerqyQueryBuilder querqyQueryBuilder) {
            this.querqyQueryBuilder = querqyQueryBuilder;
        }
    }

}