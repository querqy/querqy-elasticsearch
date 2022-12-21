package querqy.elasticsearch.rewriter;

import static java.util.Collections.singletonList;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SimpleCommonRulesRewriterFactoryTest extends AbstractRewriterIntegrationTest {


    public void testBooleanInput() throws ExecutionException, InterruptedException {
        indexDocs(
                doc("id", "1", "field1", "a"),
                doc("id", "2", "field1", "a test1 some other tokens that bring down normalised tf")
        );

        final Map<String, Object> content = new HashMap<>();
        content.put("class", SimpleCommonRulesRewriterFactory.class.getName());

        final Map<String, Object> config = new HashMap<>();
        config.put("allowBooleanInput", true);
        config.put("rules", "a AND NOT b => \nUP(1000): test1");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("a"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        SearchHits hits = response.getHits();

        assertEquals(2L, hits.getTotalHits().value);
        assertEquals("2", hits.getHits()[0].getSourceAsMap().get("id"));

        querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("a b"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        response = client().search(searchRequestBuilder.request()).get();
        hits = response.getHits();

        assertEquals(2L, hits.getTotalHits().value);
        assertEquals("1", hits.getHits()[0].getSourceAsMap().get("id"));

    }

    public void testRuleSelectionCriteria() throws ExecutionException, InterruptedException {
        indexDocs(
                doc("id", "1", "field1", "a"),
                doc("id", "2", "field1", "c")
        );

        final Map<String, Object> content = new HashMap<>();
        content.put("class", SimpleCommonRulesRewriterFactory.class.getName());

        final Map<String, Object> config = new HashMap<>();
        config.put("allowBooleanInput", true);
        config.put("rules", "a=>\nSYNONYM: c\n@lang:\"l1\"");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));

        final Map<String, Object> criteria = new HashMap<>();
        criteria.put("filter", "$[?(@.lang == 'l1')]");
        final Map<String, Object> params = new HashMap<>();
        params.put("criteria", criteria);

        final Rewriter rewriter = new Rewriter("common_rules");
        rewriter.setParams(params);

        querqyQuery.setRewriters(singletonList(rewriter));
        querqyQuery.setMatchingQuery(new MatchingQuery("a"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        SearchHits hits = response.getHits();

        assertEquals(2L, hits.getTotalHits().value);


        querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        criteria.put("filter", "$[?(@.lang == 'l2')]");

        querqyQuery.setRewriters(singletonList(rewriter));
        querqyQuery.setMatchingQuery(new MatchingQuery("a"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        response = client().search(searchRequestBuilder.request()).get();
        hits = response.getHits();

        assertEquals(1L, hits.getTotalHits().value);



    }

}