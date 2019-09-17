package querqy.elasticsearch.rewriterstore;

import static org.elasticsearch.action.ActionListener.wrap;
import static querqy.elasticsearch.rewriterstore.PutRewriteChainAction.NAME;

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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TransportPutRewriteChainAction extends HandledTransportAction<PutRewriteChainRequest, PutRewriteChainResponse> {

    // TODO: configurable?
    public static final String QUERQY_INDEX_NAME = ".querqy";

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportPutRewriteChainAction(final TransportService transportService, final ActionFilters actionFilters,
                                      final ClusterService clusterService, final Client client) {
        super(NAME, false, transportService, actionFilters, PutRewriteChainRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(final Task task, final PutRewriteChainRequest request,
                             final ActionListener<PutRewriteChainResponse> listener) {

        final IndicesAdminClient indicesClient = client.admin().indices();
        final IndicesExistsRequestBuilder existsRequestBuilder = indicesClient.prepareExists(QUERQY_INDEX_NAME);

        indicesClient.exists(existsRequestBuilder.request(), new ActionListener<IndicesExistsResponse>() {
            @Override
            public void onResponse(final IndicesExistsResponse indicesExistsResponse) {
                if (!indicesExistsResponse.isExists()) {

                    final CreateIndexRequestBuilder createIndexRequestBuilder = indicesClient
                            .prepareCreate(QUERQY_INDEX_NAME);
                    final CreateIndexRequest createIndexRequest = createIndexRequestBuilder
                            .addMapping("querqy-rewriter", readUtf8Resource("querqy-mapping.json"), XContentType.JSON).request();

                    indicesClient.create(createIndexRequest, new ActionListener<CreateIndexResponse>() {

                        @Override
                        public void onResponse(final CreateIndexResponse createIndexResponse) {
                            saveRewriteChain(task, request, listener);
                        }

                        @Override
                        public void onFailure(final Exception e) {
                            listener.onFailure(e);
                        }
                    });

                } else {
                    saveRewriteChain(task, request, listener);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        });
//
//        indicesClient.exists()


    }


    protected void saveRewriteChain(final Task task, final PutRewriteChainRequest request,
                                final ActionListener<PutRewriteChainResponse> listener) {
        final IndexRequest indexRequest = buildIndexRequest(task, request);
        client.execute(IndexAction.INSTANCE, indexRequest,

                new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(final IndexResponse indexResponse) {

                        client.execute(NodesReloadRewriteChainAction.INSTANCE,
                                new NodesReloadRewriteChainRequest(request.getChainId()),
                                wrap(
                                        (reloadResponse) -> listener
                                                .onResponse(new PutRewriteChainResponse(indexResponse, reloadResponse)),
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

    private IndexRequest buildIndexRequest(final Task parentTask, final PutRewriteChainRequest request) {

        final Map<String, Object> source = request.getContent();
        source.put("type", "chain");
//        source.put("rewriters", request.getRewriterIds());
//        final PutRewriteChainRequest.TermQueryCacheInfo termQueryCacheInfo = request.getTermQueryCacheInfo();
//        if (termQueryCacheInfo != null) {
//            final Map<String, Object> cacheInfo = new HashMap<>();
//            cacheInfo.put("fields", termQueryCacheInfo.fields);
//            cacheInfo.put("updatable", termQueryCacheInfo.updatable);
//            cacheInfo.put("size", termQueryCacheInfo.size);
//            source.put("termQueryCache", cacheInfo);
//        }


        final IndexRequest indexRequest = client.prepareIndex(".querqy", null, request.getChainId())
                .setCreate(false)
                .setRouting(request.getRouting())
                .setSource(source)
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
