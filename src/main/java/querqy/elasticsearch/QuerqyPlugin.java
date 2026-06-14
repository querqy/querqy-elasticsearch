/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2019 Querqy for Elasticsearch Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package querqy.elasticsearch;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static querqy.elasticsearch.rewriterstore.Constants.SETTINGS_QUERQY_INDEX_NUM_REPLICAS;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;

import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import querqy.elasticsearch.infologging.Log4jSink;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

public class QuerqyPlugin extends Plugin implements SearchPlugin, ActionPlugin {
    private final RewriterShardContexts rewriterShardContexts;
    private final QuerqyProcessor querqyProcessor;

    public QuerqyPlugin(final Settings settings) {
        rewriterShardContexts = new RewriterShardContexts(settings);
        querqyProcessor = new QuerqyProcessor(rewriterShardContexts, new Log4jSink());
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
    public List<RestHandler> getRestHandlers(
    	    final Settings settings,
    	    final NamedWriteableRegistry namedWriteableRegistry, // This parameter was missing
    	    final RestController restController,
    	    final ClusterSettings clusterSettings,
    	    final IndexScopedSettings indexScopedSettings,
    	    final SettingsFilter settingsFilter,
    	    final IndexNameExpressionResolver indexNameExpressionResolver,
    	    final Supplier<DiscoveryNodes> nodesInCluster,
    	    final Predicate<NodeFeature> clusterSupportsFeature  // This parameter was missing
    	) {
	    return Arrays.asList(new RestPutRewriterAction(), new RestDeleteRewriterAction());
	}

    @Override
    public List<ActionHandler> getActions() {
        return List.of(new ActionHandler(PutRewriterAction.INSTANCE, TransportPutRewriterAction.class),
                new ActionHandler(NodesReloadRewriterAction.INSTANCE, TransportNodesReloadRewriterAction.class),
                new ActionHandler(DeleteRewriterAction.INSTANCE, TransportDeleteRewriterAction.class),
                new ActionHandler(NodesClearRewriterCacheAction.INSTANCE, TransportNodesClearRewriterCacheAction.class));
    }

    @Override
    public Collection<Object> createComponents(PluginServices services) {
        return Arrays.asList(rewriterShardContexts, querqyProcessor);
    }
    
    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(Setting.intSetting(SETTINGS_QUERQY_INDEX_NUM_REPLICAS, 1, 0,
                Setting.Property.NodeScope));

    }
}
