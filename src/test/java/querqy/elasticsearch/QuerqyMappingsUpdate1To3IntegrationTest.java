package querqy.elasticsearch;

import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static querqy.elasticsearch.rewriterstore.Constants.QUERQY_INDEX_NAME;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Test;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class QuerqyMappingsUpdate1To3IntegrationTest extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }


    @After
    public void deleteRewriterIndex() {
        try {
            client().admin().indices().prepareDelete(".querqy").get();
        } catch (final IndexNotFoundException e) {
            // Ignore
        }
    }

    @Test
    public void testUpdate1To3() throws Exception {

        final String v1Mapping = "{\n" +
                "    \"properties\": {\n" +
                "      \"class\": {\"type\": \"keyword\"},\n" +
                "      \"type\": {\"type\": \"keyword\"},\n" +
                "      \"config\": {\n" +
                "        \"type\" : \"keyword\",\n" +
                "        \"index\": false\n" +
                "      }\n" +
                "\n" +
                "    }\n" +
                "}";

        final IndicesAdminClient indicesClient = client().admin().indices();

        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesClient.prepareCreate(QUERQY_INDEX_NAME);
        final CreateIndexRequest createIndexRequest = createIndexRequestBuilder
                .addMapping("querqy-rewriter", v1Mapping, XContentType.JSON)
                .setSettings(Settings.builder().put("number_of_replicas", 2))
                .request();
        indicesClient.create(createIndexRequest).get();

        final Map<String, Object> content = new HashMap<>();
        content.put("class", querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory.class.getName());

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "k =>\nSYNONYM: c\n@_log: \"msg1\"");
        config.put("ignoreCase", true);
        config.put("querqyParser", querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory.class.getName());
        content.put("config", config);

        client().execute(PutRewriterAction.INSTANCE, new PutRewriterRequest("common_rules", content)).get();

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(".querqy");
        final ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> mappings = indicesClient
                .getMappings(getMappingsRequest).get().getMappings();
        final Map<String, Object> properties = (Map<String, Object>) mappings.get(QUERQY_INDEX_NAME)
                .get("querqy-rewriter").getSourceAsMap().get("properties");
        assertNotNull(properties);
        final Map<String, Object> info_logging = (Map<String, Object>) properties.get("info_logging");
        assertNotNull(info_logging);
        final Map<String, Object> info_logging_props = (Map<String, Object>) info_logging.get("properties");
        assertNotNull(info_logging_props);

        assertThat( (Map<String, Object>) info_logging_props.get("sinks"), hasEntry("type", "keyword"));

        final Map<String, Object> config_v_003_mapping = (Map<String, Object>) properties.get("config_v_003");
        assertNotNull(config_v_003_mapping);
        assertEquals(false, config_v_003_mapping.get("doc_values"));

    }
}
