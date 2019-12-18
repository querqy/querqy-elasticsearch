package querqy.elasticsearch.rewriterstore;

import static org.elasticsearch.action.ActionListener.wrap;
import static querqy.elasticsearch.rewriterstore.Constants.DEFAULT_QUERQY_INDEX_NUM_REPLICAS;
import static querqy.elasticsearch.rewriterstore.Constants.QUERQY_INDEX_NAME;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;
import static querqy.elasticsearch.rewriterstore.PutRewriterAction.*;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TransportPutRewriterAction extends HandledTransportAction<PutRewriterRequest, PutRewriterResponse> {

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;

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
        final IndicesExistsRequestBuilder existsRequestBuilder = indicesClient.prepareExists(QUERQY_INDEX_NAME);

        indicesClient.exists(existsRequestBuilder.request(), new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse indicesExistsResponse) {
                if (!indicesExistsResponse.isExists()) {

                    indicesClient.create(buildCreateQuerqyIndexRequest(indicesClient),
                            new ActionListener<CreateIndexResponse>() {

                        @Override
                        public void onResponse(final CreateIndexResponse createIndexResponse) {
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
                    try {
                        saveRewriter(task, request, listener);
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                }
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        });
    }

    protected CreateIndexRequest buildCreateQuerqyIndexRequest(final IndicesAdminClient indicesClient) {

        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesClient.prepareCreate(QUERQY_INDEX_NAME);
        final int numReplicas = settings.getAsInt(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, DEFAULT_QUERQY_INDEX_NUM_REPLICAS);
        return  createIndexRequestBuilder
                .addMapping("querqy-rewriter", readUtf8Resource("querqy-mapping.json"), XContentType.JSON)
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
                        listener.onFailure(e);
                    }
                })
        ;
    }

    private IndexRequest buildIndexRequest(final Task parentTask, final PutRewriterRequest request) throws IOException {

        final Map<String, Object> source = new HashMap<>(request.getContent());
        source.put("type", "rewriter");
        final Map<String, Object> config = (Map<String, Object>) source.get("config");
        if (config != null) {
            source.put("config", mapToJsonString(config));
        }

        final IndexRequest indexRequest = client.prepareIndex(QUERQY_INDEX_NAME, null, request.getRewriterId())
                .setCreate(false)
                .setSource(source)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .request();
        indexRequest.setParentTask(clusterService.localNode().getId(), parentTask.getId());
        return indexRequest;
    }


    private static String mapToJsonString(final Map<String, Object> map) throws IOException {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            final XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), bos);
            builder.value(map);
            builder.flush();
            builder.close();
            return new String(bos.toByteArray(), Charset.forName("utf-8"));
        }
    }


    private static String readUtf8Resource(final String name) {
        final Scanner scanner = new Scanner(TransportPutRewriterAction.class.getClassLoader().getResourceAsStream(name),
                Charset.forName("utf-8").name()).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }



}
