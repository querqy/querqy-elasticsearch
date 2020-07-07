package querqy.elasticsearch;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
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
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.rewriterstore.DeleteRewriterAction;
import querqy.elasticsearch.rewriterstore.NodesClearRewriterCacheAction;
import querqy.elasticsearch.rewriterstore.NodesReloadRewriterAction;
import querqy.elasticsearch.rewriterstore.RestDeleteRewriterAction;
import querqy.elasticsearch.rewriterstore.RestPutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.TransportDeleteRewriterAction;
import querqy.elasticsearch.rewriterstore.TransportNodesClearRewriterCacheAction;
import querqy.elasticsearch.rewriterstore.TransportNodesReloadRewriterAction;
import querqy.elasticsearch.rewriterstore.TransportPutRewriterAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class QuerqyPlugin extends Plugin implements SearchPlugin, ActionPlugin {


    private final QuerqyProcessor querqyProcessor;
    private final RewriterShardContexts rewriterShardContexts;

    public QuerqyPlugin(final Settings settings) {
        rewriterShardContexts = new RewriterShardContexts(settings);
        querqyProcessor = new QuerqyProcessor(rewriterShardContexts);
    }

    @Override
    public void onIndexModule(final IndexModule indexModule) {

        indexModule.addIndexEventListener(rewriterShardContexts);

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

        return Arrays.asList(new RestPutRewriterAction(), new RestDeleteRewriterAction());

    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return unmodifiableList(asList(
                new ActionHandler<>(PutRewriterAction.INSTANCE, TransportPutRewriterAction.class),
                new ActionHandler<>(NodesReloadRewriterAction.INSTANCE, TransportNodesReloadRewriterAction.class),
                new ActionHandler<>(DeleteRewriterAction.INSTANCE, TransportDeleteRewriterAction.class),
                new ActionHandler<>(NodesClearRewriterCacheAction.INSTANCE, TransportNodesClearRewriterCacheAction
                        .class)

        ));
    }

    @Override
    public Collection<Object> createComponents(final Client client, final ClusterService clusterService,
                                               final ThreadPool threadPool,
                                               final ResourceWatcherService resourceWatcherService,
                                               final ScriptService scriptService,
                                               final NamedXContentRegistry xContentRegistry,
                                               final Environment environment, final NodeEnvironment nodeEnvironment,
                                               final NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        return Arrays.asList(rewriterShardContexts, querqyProcessor);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(Setting.intSetting(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, 1, 0,
                Setting.Property.NodeScope));

    }
}
