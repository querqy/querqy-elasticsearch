package querqy.elasticsearch.rewriter;

import com.github.tomakehurst.wiremock.WireMockServer;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static java.util.Collections.singletonList;

public class EmbeddingsRewriterFactoryTest extends AbstractRewriterIntegrationTest {

    public void testEmbeddingsRewriterMainQuery() throws ExecutionException, InterruptedException {
        WireMockServer embeddingServer = mockEmbeddingServer("[0.5, 9, 6]");
        createIndexWithDenseVectorField();
        indexDocs(
                doc("id", "1", "field1", "test1", "vector", new float[] { 2f, 3.2f, 2.4f }),
                doc("id", "2", "field1", "test2", "vector", new float[] { -0.5f, 10f, 10f }),
                doc("id", "3", "field1", "test1", "vector", new float[] { 3f, 2f, 1f }),
                doc("id", "4", "field1", "test1", "vector", new float[] { 0.5f, 8.8f, 6f })
        );

        createRewriter("http://localhost:" + embeddingServer.port() + "/minilm/text/",
                "embtxt");

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        final Map<String, Object> rewriterParams = new HashMap<>();
        rewriterParams.put("topK", 10);
        rewriterParams.put("boost", 100f);
        rewriterParams.put("mode", "MAIN_QUERY");
        rewriterParams.put("f", "vector");
        Rewriter rewriter = new Rewriter("embtxt", rewriterParams);
        querqyQuery.setRewriters(List.of(rewriter));
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));
        querqyQuery.setMatchingQuery(new MatchingQuery("test1"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        SearchHits hits = response.getHits();

        assertEquals(4L, hits.getTotalHits().value);
        assertEquals("4", hits.getHits()[0].getSourceAsMap().get("id"));

        embeddingServer.stop();
    }

    public void testEmbeddingsRewriterBoost() throws ExecutionException, InterruptedException {
        WireMockServer embeddingServer = mockEmbeddingServer("[0.5, 9, 6]");
        createIndexWithDenseVectorField();
        indexDocs(
                doc("id", "1", "field1", "test1", "vector", new float[] { 2f, 3.2f, 2.4f }),
                doc("id", "2", "field1", "test2", "vector", new float[] { -0.5f, 10f, 10f }),
                doc("id", "3", "field1", "test1", "vector", new float[] { 3f, 2f, 1f }),
                doc("id", "4", "field1", "test1", "vector", new float[] { 0.5f, 8.8f, 6f })
        );

        createRewriter("http://localhost:" + embeddingServer.port() + "/minilm/text/",
                "embtxt");

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        final Map<String, Object> rewriterParams = new HashMap<>();
        rewriterParams.put("topK", 10);
        rewriterParams.put("boost", 100f);
        rewriterParams.put("mode", "BOOST");
        rewriterParams.put("f", "vector");
        Rewriter rewriter = new Rewriter("embtxt", rewriterParams);
        querqyQuery.setRewriters(List.of(rewriter));
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));
        querqyQuery.setMatchingQuery(new MatchingQuery("test1"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        SearchHits hits = response.getHits();

        assertEquals(3L, hits.getTotalHits().value);
        assertEquals("4", hits.getHits()[0].getSourceAsMap().get("id"));

        embeddingServer.stop();
    }

    private static WireMockServer mockEmbeddingServer(String embeddingAsFloatArray) {
        WireMockServer wireMockServer = new WireMockServer(options().dynamicPort());
        wireMockServer.start();
        int wireMockPort = wireMockServer.port();
        configureFor("localhost", wireMockPort);
        stubFor(post("/minilm/text/")
                .withHeader("Content-Type", containing("json"))
                .willReturn(ok()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"embedding\": " + embeddingAsFloatArray + "}")));
        return wireMockServer;
    }

    private void createIndexWithDenseVectorField() {
        createIndexWithMapping(
                "{\n" +
                "   \"properties\": {" +
                "      \"vector\": {" +
                "        \"type\" : \"dense_vector\"," +
                "        \"dims\": 3," +
                "        \"index\": true," +
                "        \"similarity\": \"l2_norm\"" +
                "      }" +
                "    }" +
                "}");
    }

    private void createRewriter(String embeddingServerEndpoint, String rewriterName) throws ExecutionException, InterruptedException {
        // the request body expected by a (mock) embedding service
        final Map<String, Object> serviceRequestTemplate = Map.of(
                "text", "{{text}}",
                "output_format", "float_list",
                "separator", ",",
                "normalize", true);

        final Map<String, Object> modelConfig = new HashMap<>();
        modelConfig.put("class", "querqy.elasticsearch.rewriter.embeddings.ServiceEmbeddingModelES");
        modelConfig.put("url", embeddingServerEndpoint);
        modelConfig.put("request_template", serviceRequestTemplate);
        modelConfig.put("response_path", "$.embedding");

        final Map<String, Object> config = new HashMap<>();
        config.put("model", modelConfig);

        final Map<String, Object> content = new HashMap<>();
        content.put("class", "querqy.elasticsearch.rewriter.embeddings.EmbeddingsRewriterFactory");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest(rewriterName, content);
        client().execute(PutRewriterAction.INSTANCE, request).get();
    }
}
