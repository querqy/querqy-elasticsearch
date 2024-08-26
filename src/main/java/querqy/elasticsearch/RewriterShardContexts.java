package querqy.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.SearchExecutionContext;
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
    private final Settings settings;

    public RewriterShardContexts(final Settings settings) {
        this.settings = settings;
        this.shardContexts = new ConcurrentHashMap<>();
    }

    public RewriteChainAndLogging getRewriteChain(final List<String> rewriterIds,
                                                  final SearchExecutionContext context) {

        final ShardId shardId = new ShardId(context.getFullyQualifiedIndex(), context.getShardId());
        RewriterShardContext shardContext = shardContexts.get(shardId);

        if (shardContext == null) {
            shardContext = loadShardContext(shardId, context);
        }

        return shardContext.getRewriteChain(rewriterIds);
    }

    protected synchronized RewriterShardContext loadShardContext(final ShardId shardId,
                                                                 final SearchExecutionContext context) {
        RewriterShardContext shardContext = shardContexts.get(shardId);

        if (shardContext == null) {
            shardContext = new RewriterShardContext(shardId, indicesService.indexService(shardId.getIndex()),  settings,
                    context.getClient());
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
