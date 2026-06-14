/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2026 Querqy for Elasticsearch Contributors
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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.rewrite.RewriterFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RegexReplaceRewriterFactory extends ESRewriterFactory {

    private static final boolean DEFAULT_IGNORE_CASE = true;

    public static final String PROP_RULES = "rules";
    public static final String PROP_IGNORE_CASE = "ignoreCase";

    private querqy.rewrite.replace.RegexReplaceRewriterFactory delegate;

    public RegexReplaceRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) throws ElasticsearchException {

        final String rules = (String) config.get(PROP_RULES);
        final InputStreamReader rulesReader = new InputStreamReader(
                new ByteArrayInputStream(rules.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
        final boolean ignoreCase = ConfigUtils.getArg(config, PROP_IGNORE_CASE, DEFAULT_IGNORE_CASE);

        try {
            delegate = new querqy.rewrite.replace.RegexReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase);
        } catch (final IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {

        final String rules = (String) config.get(PROP_RULES);
        if (rules == null) {
            return Collections.singletonList("Property " + PROP_RULES + " not configured");
        }

        final InputStreamReader rulesReader = new InputStreamReader(
                new ByteArrayInputStream(rules.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);

        final boolean ignoreCase = ConfigUtils.getArg(config, PROP_IGNORE_CASE, DEFAULT_IGNORE_CASE);

        try {
            new querqy.rewrite.replace.RegexReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase);
        } catch (final IOException e) {
            return Collections.singletonList("Cannot create rewriter: " + e.getMessage());
        }

        return List.of();
    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard) {
        return delegate;
    }
}
