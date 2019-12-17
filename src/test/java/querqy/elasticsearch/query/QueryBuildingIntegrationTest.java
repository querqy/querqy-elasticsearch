package querqy.elasticsearch.query;

import static java.util.Collections.*;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESSingleNodeTestCase;
import querqy.elasticsearch.QuerqyPlugin;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class QueryBuildingIntegrationTest extends ESSingleNodeTestCase {

    private final String INDEX_NAME = "test_index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singleton(QuerqyPlugin.class);
    }

    public void testThatUpBoostWithPurelyNegativeSingleTokenQueryIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): -aa");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatUpBoostWithPurelyNegativeMultiTokenQueryIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): -aa -bb");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatBoostWithPurelyPositiveSingleTokenQueryIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): kk");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatBoostWithPurelyPositiveMultiTokenQueryIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): kk jj");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatDownWithPurelyPositiveMultiTokenQueryIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nDOWN(1000): aa bb");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc ff"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }


    public void testThatBoostWithPosNegQueryIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): kk -aa");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc dd"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatBoostWithNegMultiRuleIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): -aa\nUP(1000): -bb");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc dd"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatPurelyNegativeSingleTokenFilterIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nFILTER: -aa");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc dd"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(1L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatPurelyNegativeMultiTokenFilterIsApplied() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nFILTER: -ii -jj");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc kk"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(1L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("1", first.getSourceAsMap().get("id"));

    }

    public void testThatBoostUpIsAppliedForRawQuery() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nUP(1000): *{\"term\": {\"field1\":\"ii\"}}");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatBoostDownIsAppliedForRawQuery() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nDOWN(1000): *{\"term\": {\"field1\":\"bb\"}}");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }

    public void testThatFilterIsAppliedForRawQuery() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory");

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "cc =>\nFILTER: *{\"term\": {\"field1\":\"ii\"}}");
        config.put("ignoreCase", true);
        config.put("querqyParser", "querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content, null);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("cc aa"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(1L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("2", first.getSourceAsMap().get("id"));

    }


    public void testPhraseBoostNotMatching() throws Exception {

        index();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));

        querqyQuery.setMatchingQuery(new MatchingQuery("aa bb cc dd 11"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field2"));
        querqyQuery.setBoostingQueries(new BoostingQueries().phraseBoosts(new PhraseBoosts()
                .full(new PhraseBoostDefinition(0, "field2"))));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("3", first.getSourceAsMap().get("id"));

    }

    public void testFullPhraseBoostMatching() throws Exception {

        index();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));

        querqyQuery.setMatchingQuery(new MatchingQuery("aa bb cc dd"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field2"));
        querqyQuery.setBoostingQueries(new BoostingQueries().phraseBoosts(new PhraseBoosts()
                .full(new PhraseBoostDefinition(0, "field2"))));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("4", first.getSourceAsMap().get("id"));

    }


    public void testTrigramMatching() throws Exception {

        index();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));

        querqyQuery.setMatchingQuery(new MatchingQuery("aa bb cc 11 dd"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field2"));
        querqyQuery.setBoostingQueries(new BoostingQueries().phraseBoosts(new PhraseBoosts()
                .trigram(new PhraseBoostDefinition(0, "field2^100"))));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("4", first.getSourceAsMap().get("id"));

    }

    public void testBigramMatching() throws Exception {

        index();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));

        querqyQuery.setMatchingQuery(new MatchingQuery("aa bb nono cc 11 dd"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field2"));
        querqyQuery.setBoostingQueries(new BoostingQueries().phraseBoosts(new PhraseBoosts()
                .bigram(new PhraseBoostDefinition(0, "field2^100"))));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("4", first.getSourceAsMap().get("id"));

    }

    public void testBigramMatchingWithSlop() throws Exception {

        index();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));

        querqyQuery.setMatchingQuery(new MatchingQuery("aa cc nono 11"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field2"));
        querqyQuery.setBoostingQueries(
                new BoostingQueries().phraseBoosts(
                        new PhraseBoosts().bigram(new PhraseBoostDefinition().slop(1).fields("field2^100"))));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery).setExplain(true);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        final SearchHits hits = response.getHits();
        assertEquals(2L, hits.getTotalHits().value);

        final SearchHit first = hits.getAt(0);
        assertEquals("4", first.getSourceAsMap().get("id"));

    }


    public void index() {
        client().admin().indices().prepareCreate(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME, null)
                .setSource("id", "1", "field1", "aa bb cc dd ee ff gg hh")
                .get();
        client().prepareIndex(INDEX_NAME, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("id", "2", "field1", "ii jj cc kk ee ll gg hh")
                .get();

        client().prepareIndex(INDEX_NAME, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("id", "3", "field2", "aa xx bb yy cc zz dd ee ff gg hh 11")
                .get();


        client().prepareIndex(INDEX_NAME, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("id", "4", "field2", "aa bb cc dd ee ff gg hh xx yy zz 22")
                .get();
    }
}
