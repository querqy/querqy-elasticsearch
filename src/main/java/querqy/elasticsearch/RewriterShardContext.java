package querqy.elasticsearch;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.monitor.jvm.JvmInfo;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.RewriterFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RewriterShardContext {


    public static final Setting<ByteSizeValue> CACHE_MEM_SETTING = Setting.memorySizeSetting("querqy.caches.max_mem",
            (s) -> new ByteSizeValue(Math.min(RamUsageEstimator.ONE_MB * 10, JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()/10)).toString(),
            Setting.Property.NodeScope);
    public static final Setting<TimeValue> CACHE_EXPIRE_AFTER_WRITE = Setting.timeSetting("querqy.caches.expire_after_write",
            TimeValue.timeValueHours(1),
            TimeValue.timeValueNanos(0),
            Setting.Property.NodeScope);
    public static final Setting<TimeValue> CACHE_EXPIRE_AFTER_READ = Setting.timeSetting("querqy.caches.expire_after_read",
            TimeValue.timeValueHours(1),
            TimeValue.timeValueNanos(0),
            Setting.Property.NodeScope);

    final Cache<String, RewriterFactory> factories;
    final Client client;
    final IndexService indexService;
    final ShardId shardId;

    public RewriterShardContext(final ShardId shardId, final IndexService indexService, final Settings settings,
                                final Client client) {
        this.indexService = indexService;
        this.shardId = shardId;
        this.client = client;
        final CacheBuilder<String, RewriterFactory> builder = CacheBuilder.builder();
        factories = configureCache(builder,
                CACHE_EXPIRE_AFTER_READ.get(settings),
                CACHE_EXPIRE_AFTER_WRITE.get(settings),
                CACHE_MEM_SETTING.get(settings)).build();
    }

    public RewriteChain getRewriteChain(final List<String> rewriterIds) throws Exception {
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

    public ESRewriteChain getRewriteChain(final String chainId, final QueryShardContext queryShardContext) {
        return null;
    }

    public void clearRewriter(final String rewriterId) {
        factories.invalidate(rewriterId);
        System.out.println("Cleared " + rewriterId);
    }

    public void clearRewriters() {
        factories.invalidateAll();
    }

    public void reloadRewriter(final String rewriterId) throws Exception {
        if (factories.get(rewriterId) != null) {
            loadFactory(rewriterId, true);
        }
    }

    public synchronized RewriterFactory loadFactory(final String rewriterId, final boolean forceLoad) throws Exception {

        RewriterFactory factory = factories.get(rewriterId);

        if (forceLoad || (factory == null)) {

            final GetResponse response = client.prepareGet(".querqy", null, rewriterId).execute().get();

            final Map<String, Object> source = response.getSource();
            if (!"rewriter".equals(source.get("type"))) {
                throw new IllegalArgumentException("Not a rewriter: " + rewriterId);
            }
            factory = ESRewriterFactory.loadConfiguredInstance(rewriterId, source, "class")
                    .createRewriterFactory(indexService.getShard(shardId.id()));
            factories.put(rewriterId, factory);


        }

        return factory;

    }




    private static <K, V> CacheBuilder<K, V> configureCache(final CacheBuilder<K, V> builder,
                                                            final TimeValue expireAfterWrite,
                                                            final TimeValue expireAfterAccess,
                                                            final ByteSizeValue maxWeight) {
        if (expireAfterWrite.nanos() > 0) {
            builder.setExpireAfterWrite(expireAfterWrite);
        }
        if (expireAfterAccess.nanos() > 0) {
            builder.setExpireAfterAccess(expireAfterAccess);
        }
        builder.setMaximumWeight(maxWeight.getBytes());
        return builder;
    }



}
