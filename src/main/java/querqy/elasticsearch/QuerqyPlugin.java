package querqy.elasticsearch;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.rewriterstore.NodesReloadRewriteChainAction;
import querqy.elasticsearch.rewriterstore.NodesReloadRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriteChainAction;
import querqy.elasticsearch.rewriterstore.RestPutRewriteChainAction;
import querqy.elasticsearch.rewriterstore.RestPutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.TransportNodesReloadRewriteChainAction;
import querqy.elasticsearch.rewriterstore.TransportNodesReloadRewriterAction;
import querqy.elasticsearch.rewriterstore.TransportPutRewriteChainAction;
import querqy.elasticsearch.rewriterstore.TransportPutRewriterAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class QuerqyPlugin extends Plugin implements SearchPlugin, ActionPlugin {

    //private final Rewriters rewriters;

    private final QuerqyProcessor querqyProcessor;
    private final RewriterShardContexts rewriterShardContexts;

    public QuerqyPlugin(final Settings settings) {
        // rewriters = new Rewriters(settings);
        rewriterShardContexts = new RewriterShardContexts();
        querqyProcessor = new QuerqyProcessor(rewriterShardContexts);
    }

    @Override
    public void onIndexModule(final IndexModule indexModule) {

        indexModule.addIndexEventListener(rewriterShardContexts);

        /*
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void shardRoutingChanged(IndexShard indexShard, ShardRouting oldRouting, ShardRouting newRouting) {

            }

            @Override
            public void afterIndexShardCreated(IndexShard indexShard) {
                System.out.println("created");
            }

            @Override
            public void afterIndexShardStarted(IndexShard indexShard) {
                System.out.println("started");
            }

            @Override
            public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                System.out.println("closing");
            }

            @Override
            public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
                System.out.println("closed");
            }

            @Override
            public void indexShardStateChanged(IndexShard indexShard, IndexShardState previousState, IndexShardState currentState, String reason) {
                System.out.println("changed");
            }

            @Override
            public void onShardInactive(IndexShard indexShard) {
                System.out.println("inactive");
            }

            @Override
            public void beforeIndexCreated(Index index, Settings indexSettings) {

            }

            @Override
            public void afterIndexCreated(IndexService indexService) {
                System.out.println("after create");
            }

            @Override
            public void beforeIndexRemoved(IndexService indexService, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason) {

            }

            @Override
            public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason) {

            }

            @Override
            public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {

            }

            @Override
            public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {

            }

            @Override
            public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {

            }

            @Override
            public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
                System.out.println("before added to cluster");
            }

            @Override
            public void onStoreCreated(ShardId shardId) {
                System.out.println("store created");
            }

            @Override
            public void onStoreClosed(ShardId shardId) {
                System.out.println("store closed");
            }
        });

        */

//        indexModule.addIndexOperationListener(new IndexingOperationListener() {
//            @Override
//            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
//                return null;
//            }
//
//            @Override
//            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
//
//            }
//
//            @Override
//            public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
//
//            }
//
//            @Override
//            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
//                return null;
//            }
//
//            @Override
//            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
//
//            }
//
//            @Override
//            public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
//
//            }
//        });
    }

    /**
     * The new {@link QuerySpec}s defined by this plugin.
     */
    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
                new QuerySpec<QueryBuilder>(
                        QuerqyQueryBuilder.NAME,
                        (in) -> new QuerqyQueryBuilder(in, querqyProcessor),
                        (parser) -> QuerqyQueryBuilder.fromXContent(parser, querqyProcessor)));
    }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController,
                                             final ClusterSettings clusterSettings,
                                             final IndexScopedSettings indexScopedSettings,
                                             final SettingsFilter settingsFilter,
                                             final IndexNameExpressionResolver indexNameExpressionResolver,
                                             final Supplier<DiscoveryNodes> nodesInCluster) {
        final RestPutRewriterAction rewriterRestHandler = new RestPutRewriterAction(settings);
        restController.registerHandler(RestRequest.Method.PUT, "/_querqy/rewriter/{rewriterId}",  rewriterRestHandler);

        final RestPutRewriteChainAction rewriteChainAction = new RestPutRewriteChainAction(settings);
        restController.registerHandler(RestRequest.Method.PUT, "/_querqy/rewritechain/{chainId}",  rewriteChainAction);

        return Collections.singletonList(rewriterRestHandler);

    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return unmodifiableList(asList(
                new ActionHandler<>(PutRewriterAction.INSTANCE, TransportPutRewriterAction.class),
                new ActionHandler<>(NodesReloadRewriterAction.INSTANCE, TransportNodesReloadRewriterAction.class),
                new ActionHandler<>(PutRewriteChainAction.INSTANCE, TransportPutRewriteChainAction.class),
                new ActionHandler<>(NodesReloadRewriteChainAction.INSTANCE, TransportNodesReloadRewriteChainAction.class)

        ));
    }

    @Override
    public Collection<Object> createComponents(final Client client, final ClusterService clusterService,
                                               final ThreadPool threadPool,
                                               final ResourceWatcherService resourceWatcherService,
                                               final ScriptService scriptService,
                                               final NamedXContentRegistry xContentRegistry,
                                               final Environment environment, final NodeEnvironment nodeEnvironment,
                                               final NamedWriteableRegistry namedWriteableRegistry) {
        return Arrays.asList(rewriterShardContexts);
    }
}
