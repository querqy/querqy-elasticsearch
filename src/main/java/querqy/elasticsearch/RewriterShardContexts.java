package querqy.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RewriterShardContexts implements IndexEventListener {

    private static final Logger LOGGER = LogManager.getLogger(RewriterShardContexts.class);


    private final Map<ShardId, RewriterShardContext> shardContexts;

    private IndicesService indicesService;
    private Settings settings;

    public RewriterShardContexts(final Settings settings) {
        this.settings = settings;
        shardContexts = new ConcurrentHashMap<>();
    }

    public RewriteChainAndLogging getRewriteChain(final List<String> rewriterIds,
                                                  final QueryShardContext queryShardContext) {

        final ShardId shardId = new ShardId(queryShardContext.getFullyQualifiedIndex(), queryShardContext.getShardId());
        RewriterShardContext shardContext = shardContexts.get(shardId);

        if (shardContext == null) {
            shardContext = loadShardContext(shardId, queryShardContext);
        }

        return shardContext.getRewriteChain(rewriterIds);
    }

    protected synchronized RewriterShardContext loadShardContext(final ShardId shardId,
                                                                 final QueryShardContext queryShardContext) {
        RewriterShardContext shardContext = shardContexts.get(shardId);

        if (shardContext == null) {
            shardContext = new RewriterShardContext(shardId, indicesService.indexService(shardId.getIndex()),  settings,
                    queryShardContext.getClient());
            shardContexts.put(shardId, shardContext);
        }

        return shardContext;
    }

    public synchronized void reloadRewriter(final String rewriterId) {
        shardContexts.values().forEach(ctx -> {
            try {
                ctx.reloadRewriter(rewriterId);
            } catch (final Exception e) {
                LOGGER.error("Error reloading rewriter " + rewriterId, e);
                throw new ElasticsearchException("Could not reload rewriter " + rewriterId, e);
            }
        });
    }

    public void clearRewriter(final String rewriterId) {
        shardContexts.values().forEach(ctx -> ctx.clearRewriter(rewriterId));
    }

    public void clearRewriters() {
        shardContexts.values().forEach(RewriterShardContext::clearRewriters);
    }

    @Override
    public synchronized void shardRoutingChanged(final IndexShard indexShard, final ShardRouting oldRouting,
                                                 final ShardRouting newRouting) {
        shardContexts.remove(indexShard.shardId());
    }

    @Override
    public synchronized void afterIndexShardClosed(final ShardId shardId, final IndexShard indexShard, final Settings indexSettings) {
        shardContexts.remove(shardId);
    }

    @Inject
    public void setIndicesService(final IndicesService indicesService) {
        this.indicesService = indicesService;
    }

}
