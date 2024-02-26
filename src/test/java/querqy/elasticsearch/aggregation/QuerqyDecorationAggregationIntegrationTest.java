package querqy.elasticsearch.aggregation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class QuerqyDecorationAggregationIntegrationTest extends ESSingleNodeTestCase {

    public static String DECORATIONS_TEMPLATE = "{\"decorations\":{\"value\":[%s]}}";
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

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(querqyProcessor);
        querqyQuery.setRewriters(Collections.singletonList(new Rewriter("common_rules")));
        querqyQuery.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        querqyQuery.setMinimumShouldMatch("1");

        QuerqyDecorationAggregationBuilder aggregationBuilder = new QuerqyDecorationAggregationBuilder();
        String[] expectedDecoration = new String[] {"REDIRECT /faq/a","REDIRECT /faq/y"};

        // with decorations
        querqyQuery.setMatchingQuery(new MatchingQuery("a k"));
        searchRequestBuilder.setQuery(querqyQuery);
        searchRequestBuilder.addAggregation(aggregationBuilder);
        testSearchRequest(searchRequestBuilder, 2L, Collections.singleton(expectedDecoration[0]));

        // without hits, without decorations
        querqyQuery.setMatchingQuery(new MatchingQuery("x z"));
        searchRequestBuilder.setQuery(querqyQuery);
        testSearchRequest(searchRequestBuilder, 0L, Collections.emptySet());

        // without hits, with decorations
        querqyQuery.setMatchingQuery(new MatchingQuery("x y"));
        searchRequestBuilder.setQuery(querqyQuery);
        testSearchRequest(searchRequestBuilder, 0L, Collections.singleton(expectedDecoration[1]));

        // with hits, without decorations
        querqyQuery.setMatchingQuery(new MatchingQuery("k x"));
        searchRequestBuilder.setQuery(querqyQuery);
        testSearchRequest(searchRequestBuilder, 2L, Collections.emptySet());

        // inner boolean query
        querqyQuery.setMatchingQuery(new MatchingQuery("a k"));
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(querqyQuery);
        searchRequestBuilder.setQuery(boolQueryBuilder);
        testSearchRequest(searchRequestBuilder, 2L, Collections.singleton(expectedDecoration[0]));

        // inner constant score && inner bool query
        querqyQuery.setMatchingQuery(new MatchingQuery("a k"));

        QuerqyQueryBuilder querqyQuery2 = new QuerqyQueryBuilder(querqyProcessor);
        querqyQuery2.setRewriters(Collections.singletonList(new Rewriter("common_rules")));
        querqyQuery2.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        querqyQuery2.setMinimumShouldMatch("1");
        querqyQuery2.setMatchingQuery(new MatchingQuery("x y"));

        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(querqyQuery2);
        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(querqyQuery);
        boolQueryBuilder.should(constantScoreQueryBuilder);
        searchRequestBuilder.setQuery(boolQueryBuilder);
        testSearchRequest(searchRequestBuilder, 2L, new HashSet<>(Arrays.asList(expectedDecoration)));

    }

    private void testSearchRequest(SearchRequestBuilder searchRequestBuilder, long expectedHits, Set<Object> expectedDecorations) throws ExecutionException, InterruptedException {
        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        LOGGER.info("Response:\n{}", response);
        assertEquals(expectedHits, response.getHits().getTotalHits().value);
        InternalDecorationAggregation aggregation = response.getAggregations().get(QuerqyDecorationAggregationBuilder.NAME);
        assertEquals(
                expectedDecorations,
                new HashSet<>((List) aggregation.aggregation())
        );
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
