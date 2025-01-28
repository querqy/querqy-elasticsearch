package querqy.elasticsearch.infologging;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import querqy.elasticsearch.QuerqyPlugin;
import querqy.elasticsearch.query.InfoLoggingSpec;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ESIntegTestCase.ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numDataNodes = 2)
public class InfoLoggingMultiShardIntegrationTest extends ESIntegTestCase {

    private final String INDEX_NAME = "test_index";

    private static final int NUM_DOT_QUERY_REPLICAS = 1;
    private static ListAppender APPENDER;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }


    @Override
    protected Settings nodeSettings(final int nodeOrdinal, final Settings otherSettings) {

        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, NUM_DOT_QUERY_REPLICAS)
                .build();
    }

    @BeforeClass
    public static void addAppender() {

        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        APPENDER = new ListAppender("list2");
        APPENDER.start();
        config.addAppender(APPENDER);
        AppenderRef ref = AppenderRef.createAppenderRef("list2", null, null);
        AppenderRef[] refs = new AppenderRef[] {ref};
        LoggerConfig loggerConfig = LoggerConfig.createLogger(false, Level.INFO, Log4jSink.class.getName(),
                "true", refs, null, config, null );
        loggerConfig.addAppender(APPENDER, null, null);
        config.addLogger(Log4jSink.class.getName(), loggerConfig);
        ctx.updateLoggers();

    }

    @AfterClass
    public static void removeAppender() {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final Configuration config = ctx.getConfiguration();
        config.removeLogger(Log4jSink.class.getName());
        ctx.updateLoggers();
    }

    @After
    @Before
    public void clearAppender() {
        APPENDER.clear();
    }

    @After
    public void deleteRewriterIndex() {
        try {
            client().admin().indices().prepareDelete(".querqy").get();
        } catch (final IndexNotFoundException e) {
            // Ignore
        }
    }

    public void index() {
        client().admin().indices().prepareCreate(INDEX_NAME)
                .setSettings("{\"index\" : {\n" +
                "            \"number_of_shards\" : 2, \n" +
                "            \"number_of_replicas\" : 1 \n" +
                "    }}", XContentType.JSON).get().decRef();
        client().prepareIndex(INDEX_NAME)
                .setSource("field1", "a b", "field2", "a c")
                .get().decRef();
        client().prepareIndex(INDEX_NAME)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field1", "b c")
                .get().decRef();
    }

    @Test
    public void testThatMessageGetsLoggedOncePerShard() throws Exception {

        index();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory.class.getName());
        final Map<String, Object> loggingConf = new HashMap<>();
        loggingConf.put("sinks", "log4j");
        content.put("info_logging", loggingConf);

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "k =>\nSYNONYM: c\n@_log: \"msg1\"");
        config.put("ignoreCase", true);
        config.put("querqyParser", querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory.class.getName());
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("common_rules", content);

        client().execute(PutRewriterAction.INSTANCE, request).get().decRef();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder();
        querqyQuery.setRewriters(Collections.singletonList(new Rewriter("common_rules")));
        querqyQuery.setMatchingQuery(new MatchingQuery("a k"));
        querqyQuery.setQueryFieldsAndBoostings(Arrays.asList("field1", "field2"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setInfoLoggingSpec(new InfoLoggingSpec(LogPayloadType.DETAIL, "query-detail"));

        int attempts = 10;
        while (attempts > 0) {
            APPENDER.clear();
            final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
            searchRequestBuilder.setQuery(querqyQuery);
            SearchResponse response = searchRequestBuilder.execute().get();
            final boolean shouldBreak = response.getHits().getTotalHits().value == 2L;
            response.decRef();
            if (shouldBreak) {
                break;
            }
            attempts--;
            if (attempts < 1) {
                fail("Could not get rewritten results");
            } else {
                synchronized (this) {
                    wait(100L);
                }
            }
        }

        final List<LogEvent> events = APPENDER.getEvents();
        assertNotNull(events);
        // max 1 event per shard
        assertTrue(2 >= events.size() && !events.isEmpty());
        LogEvent event = events.get(0);
        assertEquals("{\"id\":\"query-detail\",\"msg\":{\"common_rules\":[[{\"message\":\"msg1\",\"match\":" +
                        "{\"term\":\"k\",\"type\":\"exact\"},\"instructions\":[{\"type\":\"synonym\"," +
                        "\"value\":\"c\"}]}]]}}",
                event.getMessage().getFormattedMessage());

        assertEquals(Log4jSink.MARKER_QUERQY_REWRITER_DETAIL, event.getMarker());

    }
}
