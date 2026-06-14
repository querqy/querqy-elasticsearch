/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2020 Querqy for Elasticsearch Contributors
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
package querqy.elasticsearch.rewriter;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import querqy.elasticsearch.QuerqyPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;

public abstract class AbstractRewriterIntegrationTest extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return singleton(QuerqyPlugin.class);
    }

    private static final String INDEX_NAME = "test_index";

    protected static String getIndexName() {
        return INDEX_NAME;
    }

    public Map<String, Object> doc(Object... kv) {
        if (kv.length % 2 != 0) {
            throw new RuntimeException("Input size must be even");
        }

        Map<String, Object> doc = new HashMap<>();
        for (int i = 0; i < kv.length; i = i + 2) {
            doc.put((String) kv[i], kv[i + 1]);
        }

        return doc;
    }

    @SafeVarargs
    public final void indexDocs(Map<String, Object>... docs) {
        client().admin().indices().prepareCreate(getIndexName()).get();

        Arrays.stream(docs).forEach(doc ->
                client().prepareIndex(getIndexName())
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                        .setSource(doc)
                        .get());
    }
}
