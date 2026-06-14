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

import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.TimeValue;

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
