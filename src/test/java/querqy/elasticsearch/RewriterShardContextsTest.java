package querqy.elasticsearch;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.RemoteTransportException;
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
import java.util.concurrent.ExecutionException;

@ESIntegTestCase.ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numClientNodes = 1, minNumDataNodes = 4,
        maxNumDataNodes = 6, transportClientRatio = 1.0)
public class RewriterShardContextsTest extends ESIntegTestCase {

    private static final int NUM_DOT_QUERY_REPLICAS = 1;


    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    @Override
    protected Settings nodeSettings(final int nodeOrdinal) {

        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, NUM_DOT_QUERY_REPLICAS)
                .build();
    }


    public void testClearRewritersFromCache() throws Exception {

        index();

        // create rewriter
        final Map<String, Object> payload1 = new HashMap<>();
        payload1.put("class", DummyESRewriterFactory.class.getName());
        final Map<String, Object> config1 = new HashMap<>();
        config1.put("p1", 1L);
        payload1.put("config", config1);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("r2", payload1)).get();

        // assure we can use the rewriter in the query
        QuerqyQueryBuilder query = new QuerqyQueryBuilder();

        query.setMatchingQuery(new MatchingQuery("a"));
        query.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        query.setRewriters(Collections.singletonList(new Rewriter("r2")));
        final SearchResponse response1 = client().prepareSearch("idx").setQuery(query).execute().get();
        assertEquals(0, response1.getFailedShards());

        // clear loaded rewriters
        final NodesClearRewriterCacheResponse clearRewriterCacheResponse1 = client()
                .execute(NodesClearRewriterCacheAction.INSTANCE, new NodesClearRewriterCacheRequest()).get();
        assertFalse(clearRewriterCacheResponse1.hasFailures());

        // search with the rewriter again
        final SearchResponse response2 = client().prepareSearch("idx").setQuery(query).execute().get();
        assertEquals(0, response2.getFailedShards()); // rewriter probably reloaded

        // delete rewriter config from .query index - this should never be done directly (use a delete rewriter action)
        final DeleteResponse deleteResponse = client().prepareDelete(".querqy", null, "r2").execute().get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

        // query again - the rewriter should still be cached
        final SearchResponse response3 = client().prepareSearch("idx").setQuery(query).execute().get();
        assertEquals(0, response3.getFailedShards());

        // clear loaded rewriters
        final NodesClearRewriterCacheResponse clearRewriterCacheResponse2 = client().
                execute(NodesClearRewriterCacheAction.INSTANCE, new NodesClearRewriterCacheRequest()).get();
        assertFalse(clearRewriterCacheResponse2.hasFailures());

        // now we should crash: rewriters are neither loaded nor will there be a config in the .querqy index

        try {

            client().prepareSearch("idx").setQuery(query).execute().get();
            fail("Rewriter must not exist");

        } catch (final ExecutionException e) {

            final Throwable cause1 = e.getCause();
            assertTrue(cause1 instanceof RemoteTransportException);
            final Throwable cause2 = cause1.getCause();
            assertTrue(cause2 instanceof SearchPhaseExecutionException);
            final Throwable cause3 = cause2.getCause();
            assertTrue(cause3 instanceof ResourceNotFoundException);
            assertEquals("Rewriter not found: r2", cause3.getMessage());

        }

    }

    public void index() {
        final String indexName = "idx";
        client().admin().indices().prepareCreate(indexName).setSettings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)).get();
        client().prepareIndex(indexName, null)
                .setSource("field1", "a b", "field2", "a c")
                .get();
        client().prepareIndex(indexName, null)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "b c")
                .get();
    }
}