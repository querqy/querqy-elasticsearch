package querqy.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.InvalidTypeNameException;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.RewriterFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RewriterShardContext {


    public static final Setting<TimeValue> CACHE_EXPIRE_AFTER_WRITE = Setting.timeSetting(
            "querqy.caches.rewriter.expire_after_write",
            TimeValue.timeValueNanos(0), // do not expire by default
            TimeValue.timeValueNanos(0),
            Setting.Property.NodeScope);

    public static final Setting<TimeValue> CACHE_EXPIRE_AFTER_READ = Setting.timeSetting(
            "querqy.caches.rewriter.expire_after_read",
            TimeValue.timeValueNanos(0), // do not expire by default
            TimeValue.timeValueNanos(0),
            Setting.Property.NodeScope);

    private static final Logger LOGGER = LogManager.getLogger(RewriterShardContext.class);

    final Cache<String, RewriterFactory> factories;
    final Client client;
    final IndexService indexService;
    final ShardId shardId;

    public RewriterShardContext(final ShardId shardId, final IndexService indexService, final Settings settings,
                                final Client client) {
        this.indexService = indexService;
        this.shardId = shardId;
        this.client = client;
        factories = Caches.buildCache(CACHE_EXPIRE_AFTER_READ.get(settings), CACHE_EXPIRE_AFTER_WRITE.get(settings));
        LOGGER.info("Context loaded for shard {} {}", shardId, shardId.getIndex());
    }

    public RewriteChain getRewriteChain(final List<String> rewriterIds) {
        final List<RewriterFactory> rewriterFactories = new ArrayList<>(rewriterIds.size());
        for (final String id : rewriterIds) {

            RewriterFactory factory = factories.get(id);
            if (factory == null) {
                factory = loadFactory(id, false);
            }
            rewriterFactories.add(factory);

        }

        return new RewriteChain(rewriterFactories);
    }

    public void clearRewriter(final String rewriterId) {
        factories.invalidate(rewriterId);
    }

    public void clearRewriters() {
        factories.invalidateAll();
    }

    public void reloadRewriter(final String rewriterId) {
        if (factories.get(rewriterId) != null) {
            loadFactory(rewriterId, true);
        }
    }

    public synchronized RewriterFactory loadFactory(final String rewriterId, final boolean forceLoad) {

        RewriterFactory factory = factories.get(rewriterId);

        if (forceLoad || (factory == null)) {

            final GetResponse response;

            try {
                response = client.prepareGet(".querqy", null, rewriterId).execute().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ElasticsearchException("Could not load rewriter " + rewriterId, e);
            }

            final Map<String, Object> source = response.getSource();

            if (source == null) {
                throw new ResourceNotFoundException("Rewriter not found: " + rewriterId);
            }

            if (!"rewriter".equals(source.get("type"))) {
                throw new InvalidTypeNameException("Not a rewriter: " + rewriterId);
            }
            factory = ESRewriterFactory.loadConfiguredInstance(rewriterId, source, "class")
                    .createRewriterFactory(indexService.getShard(shardId.id()));
            factories.put(rewriterId, factory);


        }

        return factory;

    }


}
