package querqy.elasticsearch.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import querqy.elasticsearch.QuerqyPlugin;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QuerqyDecorationAggregationIntegrationTest extends ESSingleNodeTestCase {

    private final static Logger LOGGER = LogManager.getLogger(QuerqyDecorationAggregationIntegrationTest.class);

    private final String INDEX_NAME = "test_index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    public void testSearchWithConfig() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory.class.getName());

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "k =>\nSYNONYM: c\na =>\nDECORATE: REDIRECT /faq/a\ny =>\nDECORATE: REDIRECT /faq/y");
        config.put("ignoreCase", true);
        config.put("querqyParser", querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory.class.getName());
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyProcessor querqyProcessor = getInstanceFromNode(QuerqyProcessor.class);
        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(querqyProcessor);
        querqyQuery.setRewriters(Collections.singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("a k"));
        querqyQuery.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        querqyQuery.setMinimumShouldMatch("1");

        QuerqyDecorationAggregationBuilder aggregationBuilder = new QuerqyDecorationAggregationBuilder();
        // with decorations
        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(querqyQuery);
        searchRequestBuilder.addAggregation(aggregationBuilder);
        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        assertEquals(2L, response.getHits().getTotalHits().value);
        assertEquals("{\"decorations\":{\"value\":[\"REDIRECT /faq/a\"]}}", response.getAggregations().getAsMap().get(QuerqyDecorationAggregationBuilder.NAME).toString());
        LOGGER.info("Response:\n{}", response);

        // without hits, without decorations
        querqyQuery.setMatchingQuery(new MatchingQuery("x z"));
        searchRequestBuilder.setQuery(querqyQuery);
        response = client().search(searchRequestBuilder.request()).get();
        assertEquals(0L, response.getHits().getTotalHits().value);
        assertEquals("{\"decorations\":{\"value\":[]}}", response.getAggregations().getAsMap().get(QuerqyDecorationAggregationBuilder.NAME).toString());

        // without hits, with decorations
        querqyQuery.setMatchingQuery(new MatchingQuery("x y"));
        searchRequestBuilder.setQuery(querqyQuery);
        response = client().search(searchRequestBuilder.request()).get();
        assertEquals(0L, response.getHits().getTotalHits().value);
        assertEquals("{\"decorations\":{\"value\":[\"REDIRECT /faq/y\"]}}", response.getAggregations().getAsMap().get(QuerqyDecorationAggregationBuilder.NAME).toString());

    }

    @After
    public void deleteRewriterIndex() {
        client().admin().indices().prepareDelete(".querqy").get();
    }

    public void index() {
        client().admin().indices().prepareCreate(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME, null)
                .setSource("field1", "a b", "field2", "a c")
                .get();
        client().prepareIndex(INDEX_NAME, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "b c")
                .get();
    }
}
