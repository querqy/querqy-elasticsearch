/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2021 Querqy for Elasticsearch Contributors
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
package querqy.elasticsearch.infologging;

import querqy.lucene.rewrite.infologging.InfoLogging;
import querqy.lucene.rewrite.infologging.Sink;
import querqy.rewrite.SearchEngineRequestAdapter;

import java.util.Collections;
import java.util.Set;

public class SingleSinkInfoLogging implements InfoLogging {

    private final Sink sink;
    private final Set<String> enabledRewriterIds;

    public SingleSinkInfoLogging(final Sink sink, final Set<String> enabledRewriterIds) {
        this.sink = sink;
        this.enabledRewriterIds = enabledRewriterIds == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(enabledRewriterIds);
    }

    @Override
    public void log(final Object message, final String rewriterId,
                    final SearchEngineRequestAdapter searchEngineRequestAdapter) {
        if (enabledRewriterIds.contains(rewriterId)) {
            sink.log(message, rewriterId, searchEngineRequestAdapter);
        }
    }

    @Override
    public void endOfRequest(final SearchEngineRequestAdapter searchEngineRequestAdapter) {
        sink.endOfRequest(searchEngineRequestAdapter);
    }

    @Override
    public boolean isLoggingEnabledForRewriter(final String rewriterId) {
        return enabledRewriterIds.contains(rewriterId);
    }
}
