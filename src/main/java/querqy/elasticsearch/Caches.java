package querqy.elasticsearch;

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.unit.TimeValue;

public class Caches {

    public static <K, V> Cache<K, V> buildCache(final TimeValue expireAfterWrite, final TimeValue expireAfterAccess) {

        final CacheBuilder<K, V> builder = CacheBuilder.builder();
        if (expireAfterWrite.nanos() > 0) {
            builder.setExpireAfterWrite(expireAfterWrite);
        }
        if (expireAfterAccess.nanos() > 0) {
            builder.setExpireAfterAccess(expireAfterAccess);
        }
        return builder.build();
    }

}
