package querqy.elasticsearch.rewriterstore;

import static org.elasticsearch.action.ActionListener.wrap;
import static querqy.elasticsearch.rewriterstore.Constants.DEFAULT_QUERQY_INDEX_NUM_REPLICAS;
import static querqy.elasticsearch.rewriterstore.Constants.QUERQY_INDEX_NAME;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;
import static querqy.elasticsearch.rewriterstore.PutRewriterAction.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class TransportPutRewriterAction extends HandledTransportAction<PutRewriterRequest, PutRewriterResponse> {

    private static final Logger LOGGER = LogManager.getLogger(TransportPutRewriterAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private boolean mappingsVersionChecked = false;

    @Inject
    public TransportPutRewriterAction(final TransportService transportService, final ActionFilters actionFilters,
                                      final ClusterService clusterService, final Client client, final Settings settings)
    {
        super(NAME, false, transportService, actionFilters, PutRewriterRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.settings = settings;
    }

    @Override
    protected void doExecute(final Task task, final PutRewriterRequest request,
                             final ActionListener<PutRewriterResponse> listener) {

        final IndicesAdminClient indicesClient = client.admin().indices();

        indicesClient.prepareGetMappings(QUERQY_INDEX_NAME).execute(new ActionListener<GetMappingsResponse>() {

            @Override
            public void onResponse(final GetMappingsResponse getMappingsResponse) {

                final Map<String, MappingMetadata> mappings = getMappingsResponse.getMappings();

                if (!mappingsVersionChecked) {

                    final Map<String, Object> properties = (Map<String, Object>) mappings.get(QUERQY_INDEX_NAME)
                            .getSourceAsMap().get("properties");
                    if (!properties.containsKey("info_logging")) {
                        try {
                            update1To3(indicesClient);
                            mappingsVersionChecked = true;
                        } catch (final Exception e) {
                            listener.onFailure(e);
                            return;
                        }

                    } else if (!properties.containsKey(RewriterConfigMapping.CURRENT.getConfigStringProperty())) {
                        try {
                            update2To3(indicesClient);
                            mappingsVersionChecked = true;
                        } catch (final Exception e) {
                            listener.onFailure(e);
                            return;
                        }

                    }
                }
                try {
                    saveRewriter(task, request, listener);
                } catch (final IOException e) {
                    listener.onFailure(e);
                }

            }

            @Override
            public void onFailure(final Exception e) {
                if ((e instanceof IndexNotFoundException) || (e.getCause() instanceof IndexNotFoundException)) {

                    indicesClient.create(buildCreateQuerqyIndexRequest(indicesClient),
                            new ActionListener<CreateIndexResponse>() {

                                @Override
                                public void onResponse(final CreateIndexResponse createIndexResponse) {
                                    LOGGER.info("Created index {}", QUERQY_INDEX_NAME);
                                    mappingsVersionChecked = true;
                                    try {
                                        saveRewriter(task, request, listener);
                                    } catch (final IOException e) {
                                        listener.onFailure(e);
                                    }
                                }

                                @Override
                                public void onFailure(final Exception e) {
                                    listener.onFailure(e);
                                }
                            });

                } else {
                    listener.onFailure(e);
                }
            }
        });

    }

    protected void update1To3(final IndicesAdminClient indicesClient ) throws ExecutionException,
            InterruptedException {
        final PutMappingRequest request = new PutMappingRequest(QUERQY_INDEX_NAME).source(
                "{\n" +
                        "    \"properties\": {\n" +
                        "      \"info_logging\": {\n" +
                        "        \"properties\": {\n" +
                        "          \"sinks\": {\"type\" : \"keyword\" }\n" +
                        "        }\n" +
                        "      },\n" +
                        "      \"config_v_003\": {\n" +
                        "        \"type\" : \"keyword\",\n" +
                        "        \"doc_values\": false,\n" +
                        "        \"index\": false\n" +
                        "      }" +
                        "    }\n" +
                        "}", XContentType.JSON
        );

        if (!indicesClient.putMapping(request).get().isAcknowledged()) {
            throw new IllegalStateException("Adding info_logging to mappings not " +
                    "acknowledged");
        }

        LOGGER.info("Added info_logging property and config_v_003 to index {}", QUERQY_INDEX_NAME);

    }

    protected void update2To3(final IndicesAdminClient indicesClient ) throws ExecutionException,
            InterruptedException {
        final PutMappingRequest request = new PutMappingRequest(QUERQY_INDEX_NAME).source(
                "{\n" +
                        "    \"properties\": {\n" +
                        "      \"config_v_003\": {\n" +
                        "        \"type\" : \"keyword\",\n" +
                        "        \"doc_values\": false,\n" +
                        "        \"index\": false\n" +
                        "      }" +
                        "    }\n" +
                        "}", XContentType.JSON
        );

        if (!indicesClient.putMapping(request).get().isAcknowledged()) {
            throw new IllegalStateException("Adding config_v_003 to mappings not " +
                    "acknowledged");
        }

        LOGGER.info("Added config_v_003 property to index {}", QUERQY_INDEX_NAME);

    }

    protected CreateIndexRequest buildCreateQuerqyIndexRequest(final IndicesAdminClient indicesClient) {

        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesClient.prepareCreate(QUERQY_INDEX_NAME);
        final int numReplicas = settings.getAsInt(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, DEFAULT_QUERQY_INDEX_NUM_REPLICAS);
        return  createIndexRequestBuilder.setMapping(readUtf8Resource("querqy-mapping.json"))
                .setSettings(Settings.builder().put("number_of_replicas", numReplicas))
                .request();
    }


    protected void saveRewriter(final Task task, final PutRewriterRequest request,
                                final ActionListener<PutRewriterResponse> listener) throws IOException {
        final IndexRequest indexRequest = buildIndexRequest(task, request);
        client.execute(IndexAction.INSTANCE, indexRequest,

                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(final IndexResponse indexResponse) {
                        LOGGER.info("Saved rewriter {}", request.getRewriterId());
                        client.execute(NodesReloadRewriterAction.INSTANCE,
                                new NodesReloadRewriterRequest(request.getRewriterId()),
                                wrap(
                                        (reloadResponse) -> listener
                                                .onResponse(new PutRewriterResponse(indexResponse, reloadResponse)),
                                        listener::onFailure
                                ));
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        LOGGER.error("Could not save rewriter " + request.getRewriterId(), e);
                        listener.onFailure(e);
                    }
                })
        ;
    }

    private IndexRequest buildIndexRequest(final Task parentTask, final PutRewriterRequest request) throws IOException {

        final IndexRequest indexRequest = client.prepareIndex(QUERQY_INDEX_NAME)
                .setId(request.getRewriterId())
                .setCreate(false)
                .setSource(RewriterConfigMapping.toLuceneSource(request.getContent()))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .request();
        indexRequest.setParentTask(clusterService.localNode().getId(), parentTask.getId());
        return indexRequest;
    }





    private static String readUtf8Resource(final String name) {
        final Scanner scanner = new Scanner(TransportPutRewriterAction.class.getClassLoader().getResourceAsStream(name),
                Charset.forName("utf-8").name()).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }



}
