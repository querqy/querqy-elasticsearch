package querqy.elasticsearch;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheAction;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheRequest;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheResponse;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@ESIntegTestCase.ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numClientNodes = 1, numDataNodes = 2)
public class RewriterShardContextsTest extends ESIntegTestCase {

    private static final int NUM_DOT_QUERQY_REPLICAS = 1;


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal, final Settings otherSettings) {

        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, NUM_DOT_QUERQY_REPLICAS)
                .build();
    }

    /**
     * See {@link #queryClient()} why we set an _only_nodes preference in the search queries.
     * @throws Exception
     */
    public void testClearRewritersFromCache() throws Exception {
        internalCluster().clearDisruptionScheme(true);
        index();

        // create rewriter
        final Map<String, Object> payload1 = new HashMap<>();
        payload1.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config1 = new HashMap<>();
        config1.put("p1", 1L);
        payload1.put("config", config1);

        queryClient().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r2", payload1)).get();

        // assure we can use the rewriter in the query
        QuerqyQueryBuilder query = new QuerqyQueryBuilder();

        query.setMatchingQuery(new MatchingQuery("a"));
        query.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        query.setRewriters(Collections.singletonList(new Rewriter("r2")));

        final SearchResponse response1 = queryClient().prepareSearch("idx").setPreference("_only_nodes:node_s1,node_s2")
                .setQuery(query).setRequestCache(false).execute().get();
        assertEquals(0, response1.getFailedShards());

        // clear loaded rewriters
        final NodesClearRewriterCacheResponse clearRewriterCacheResponse1 = client()
                .execute(NodesClearRewriterCacheAction.INSTANCE, new NodesClearRewriterCacheRequest()).get();
        assertFalse(clearRewriterCacheResponse1.hasFailures());

        QuerqyQueryBuilder query3 = new QuerqyQueryBuilder();

        query3.setMatchingQuery(new MatchingQuery("a"));
        query3.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        query3.setRewriters(Collections.singletonList(new Rewriter("r2")));

        // search with the rewriter again
        final SearchResponse response2 = queryClient().prepareSearch("idx").setPreference("_only_nodes:node_s1,node_s2")
                .setQuery(query3).setRequestCache(false).execute().get();
        assertEquals(0, response2.getFailedShards()); // rewriter probably reloaded

        // delete rewriter config from .query index - this should never be done directly (use a delete rewriter action)
        final DeleteResponse deleteResponse = client().prepareDelete(".querqy", "r2").execute().get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());


        QuerqyQueryBuilder query2 = new QuerqyQueryBuilder();

        query2.setMatchingQuery(new MatchingQuery("a"));
        query2.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        query2.setRewriters(Collections.singletonList(new Rewriter("r2")));


        // query again - the rewriter should still be cached
        final SearchResponse response3 = queryClient().prepareSearch("idx")
                .setPreference("_only_nodes:node_s1,node_s2").setQuery(query2).execute().get();
        assertEquals(0, response3.getFailedShards());

        // clear loaded rewriters
        final NodesClearRewriterCacheResponse clearRewriterCacheResponse2 = client().
                execute(NodesClearRewriterCacheAction.INSTANCE, new NodesClearRewriterCacheRequest()).get();
        assertFalse(clearRewriterCacheResponse2.hasFailures());

        // now we should crash: rewriters are neither loaded nor will there be a config in the .querqy index

        try {

            queryClient().prepareSearch("idx").setPreference("_only_nodes:node_s1,node_s2")
                    .setQuery(query).execute().get();
            fail("Rewriter must not exist");

        } catch (final ExecutionException e) {

            final Throwable cause1 = e.getCause();
            assertTrue(cause1 instanceof SearchPhaseExecutionException);
            final Throwable cause2 = cause1.getCause();
            cause2.printStackTrace();
            assertTrue(cause2 instanceof ResourceNotFoundException);
            assertEquals("Rewriter not found: r2", cause2.getMessage());

        }

    }

    public void index() {
        final String indexName = "idx";
        client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
                .put("index.shard.check_on_startup", false)
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 1)).get();
        client().prepareIndex(indexName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "a b", "field2", "a c")
                .get();
        client().prepareIndex(indexName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "b c")
                .get();
    }

    private static final Random RAND = new Random();

    /**
     * The first (= 0th) node provided by the test framework would just use the passed in QuerqyQueryBuilder object
     * so that the builder would not have its QuerqyProcessor set by the plugin. We always return a client of the
     * first node (instead of a random node) and query the other nodes via preferences which will generate a new
     * QueryBuilder and allow to set set the QuerqyProcessor
     *
     * @return A client of node node_s0
     */
    private static Client queryClient() {
        return client("node_s0");
    }
}