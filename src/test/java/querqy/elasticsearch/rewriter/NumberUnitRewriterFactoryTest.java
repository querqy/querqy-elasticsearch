package querqy.elasticsearch.rewriter;

import org.assertj.core.api.Assertions;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.Test;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;
import querqy.lucene.QuerySimilarityScoring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class NumberUnitRewriterFactoryTest extends AbstractRewriterIntegrationTest {

    private static String basePath = "/numberunit/";

    @Before
    public void setup() {
        indexDocs();
    }

    @Test
    public void testBoostingForExactMatchRange() {
        String q = "smartphone 9 zoll";

        SearchResponse response = req(q, "number-unit-exact-range-config.json");
        assertContainsAllDocsInAnyOrder(response, "13", "12");

        SearchHit[] hits = response.getHits().getHits();
        Assertions.assertThat(hits[0].getScore()).isEqualTo(hits[1].getScore());
    }

    @Test
    public void testBoostingForExactMatchRangeAcrossUnits() {
        String q = "smartphone 9 zoll 1000gb";

        SearchResponse response = req(q, "number-unit-config.json");
        assertOrderForDocs(response, "12", "13");
    }

    @Test
    public void testUnlimitedRange() {
        String q = "55unitUnlimited";

        SearchResponse response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "5", "6", "20");
    }

    @Test
    public void testNumberUnitOnlyQuery() {
        String q = "55 zoll";

        SearchResponse response = req(q, "number-unit-config.json");
        assertSize(response, 4);
    }

    @Test
    public void testBoostingForMultipleNumberUnitInputs() {
        String q = "tv 200 cm 2 cm";

        SearchResponse response = req(q, "number-unit-config.json");
        assertOrderForDocs(response, "1", "2");
    }

    @Test
    public void testBoostingForMultipleNumberUnitInputsAcrossUnits() {
        String q = "notebook 14 zoll 1tb";

        SearchResponse response = req(q, "number-unit-config.json");
        assertOrderForDocs(response, "7", "8", "6", "11", "10");
    }

    @Test
    public void testBoostingForSingleNumberUnitInputAndSingleUnitConfig() {
        String q = "notebook 15 zoll";

        SearchResponse response = req(q, "number-unit-config.json");
        assertOrderForDocs(response, "7", "6", "8", "9");
    }

    @Test
    public void testFilteringForSingleNumberUnitInputAndSingleUnitConfig() {
        String q = "tv 55 zoll";

        SearchResponse response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "1", "2", "3");
    }

    @Test
    public void testFilteringForMultipleNumberUnitInputs() {
        String q = "tv 200 cm 2 cm";

        SearchResponse response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "1", "2");
    }

    @Test
    public void testFilteringForMultipleNumberUnitInputsAcrossUnits() {
        String q;
        SearchResponse response;

        q = "tv 55 zoll 20 mm";
        response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "1", "2");

        q = "tv 35 zoll 20 mm";
        response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "4");
    }

    @Test
    public void testFilteringForSingleNumberUnitInputAndMultipleUnitConfig() {
        String q = "tv 210 cm";

        SearchResponse response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "1", "2");
    }

    @Test
    public void testFilteringForSingleNumberUnitInputAndMultipleUnitConfig2() {
        String q = "tv 120 cm";

        SearchResponse response = req(q, "number-unit-config.json");
        assertContainsAllDocsInAnyOrder(response, "1", "2", "3", "4");
    }

    private SearchResponse req(String q, String configName) {
        try {
            final String numberUnitConfig = getConfigFromFileName(configName);

            final Map<String, Object> content = new HashMap<>();
            content.put("class", "querqy.elasticsearch.rewriter.NumberUnitRewriterFactory");

            final Map<String, Object> config = new HashMap<>();
            config.put("config", numberUnitConfig);

            content.put("config", config);

            final PutRewriterRequest request = new PutRewriterRequest("numberunit_rules", content);

            client().execute(PutRewriterAction.INSTANCE, request).get();

            QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
            querqyQuery.setRewriters(singletonList(new Rewriter("numberunit_rules")));

            MatchingQuery matchingQuery = new MatchingQuery(q);
            matchingQuery.setSimilarityScoring(QuerySimilarityScoring.SIMILARITY_SCORE_OFF);

            querqyQuery.setMatchingQuery(matchingQuery);
            querqyQuery.setTieBreaker(0.0f);
            querqyQuery.setMinimumShouldMatch("100%");
            querqyQuery.setQueryFieldsAndBoostings(singletonList("f1"));

            SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
            searchRequestBuilder.setQuery(querqyQuery);

            return client().search(searchRequestBuilder.request()).get();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void indexDocs() {

        indexDocs(
                doc("identifier", "1", "f1", "tv", "f2", "tele", "depth", 2, "width", 200, "screen_size", 55.0),
                doc("identifier", "2", "f1", "tv", "height", 130, "depth", 2, "width", 190, "screen_size", 54.6),
                doc("identifier", "3", "f1", "tv", "height", 110, "depth", 10, "width", 160, "screen_size", 50.0),
                doc("identifier", "4", "f1", "tv", "height", 80, "depth", 2, "width", 120, "screen_size", 35.7),
                doc("identifier", "5", "f1", "tv", "fieldUnlimited", 100000),

                doc("identifier", "6", "f1", "notebook", "disk", 1150, "screen_size", 14.8, "fieldUnlimited", 0),
                doc("identifier", "7", "f1", "notebook", "disk", 1000, "screen_size", 15.0),
                doc("identifier", "8", "f1", "notebook", "disk", 1199, "screen_size", 14.3),
                doc("identifier", "9", "f1", "notebook", "disk", 1201, "screen_size", 17.0),
                doc("identifier", "10", "f1", "notebook", "disk", 800, "screen_size", 11.7),
                doc("identifier", "11", "f1", "notebook", "disk", 1000, "screen_size", 11.7),

                doc("identifier", "12", "f1", "smartphone", "disk", 1000, "screen_size", 9.0),
                doc("identifier", "13", "f1", "smartphone", "disk", 1001, "screen_size", 9.1),
                doc("identifier", "14", "f1", "smartphone", "disk", 1500, "screen_size", 11.7),

                doc("identifier", "20", "f1", "10 zoll", "screen_size", 48.7, "fieldUnlimited", -100000)
        );
    }

    private static void assertOrderForDocs(SearchResponse actual, String... expected) {
        List<String> ids = Arrays.stream(actual.getHits().getHits())
                .map(hit -> hit.getSourceAsMap().get("identifier"))
                .map(Object::toString)
                .collect(Collectors.toList());

        Assertions.assertThat(ids).hasSize(expected.length);
        Assertions.assertThat(ids).containsExactly(expected);

    }

    private static void assertContainsAllDocsInAnyOrder(SearchResponse actual, String... expected) {
        List<String> ids = Arrays.stream(actual.getHits().getHits())
                .map(hit -> hit.getSourceAsMap().get("identifier"))
                .map(Object::toString)
                .collect(Collectors.toList());

        Assertions.assertThat(ids).hasSize(expected.length);
        Assertions.assertThat(ids).containsExactlyInAnyOrder(expected);
    }

    private static void assertSize(SearchResponse actual, int expected) {
        Assertions.assertThat(actual.getHits().getTotalHits().value).isEqualTo(expected);
    }

    private String getConfigFromFileName(String fileName) throws IOException {
        InputStream inputStream = this.getClass().getResourceAsStream(basePath + fileName);

        BufferedReader buf = new BufferedReader(new InputStreamReader(inputStream));

        String line = buf.readLine();
        StringBuilder sb = new StringBuilder();

        while(line != null){
            sb.append(line).append("\n");
            line = buf.readLine();
        }

        return sb.toString();
    }

}
