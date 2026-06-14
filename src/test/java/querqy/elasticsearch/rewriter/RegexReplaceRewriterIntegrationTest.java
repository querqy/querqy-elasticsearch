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


import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHits;
import querqy.elasticsearch.QuerqyProcessor;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;
import querqy.elasticsearch.rewriterstore.PutRewriterAction;
import querqy.elasticsearch.rewriterstore.PutRewriterRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;

public class RegexReplaceRewriterIntegrationTest extends AbstractRewriterIntegrationTest {


    public void testRegexReplaceRewriterRules() throws ExecutionException, InterruptedException {
        indexDocs(
                doc("id", "1", "field1", "162x12"),
                doc("id", "2", "field1", "162 x 12")
        );

        final Map<String, Object> content = new HashMap<>();
        content.put("class", RegexReplaceRewriterFactory.class.getName());

        final Map<String, Object> config = new HashMap<>();
        config.put("rules", "(\\d+) ?x ?(\\d+) => ${1}x${2}");
        content.put("config", config);

        final PutRewriterRequest request = new PutRewriterRequest("regex_replace", content);

        client().execute(PutRewriterAction.INSTANCE, request).get();

        QuerqyQueryBuilder querqyQuery = new QuerqyQueryBuilder(getInstanceFromNode(QuerqyProcessor.class));
        querqyQuery.setRewriters(singletonList(new Rewriter("regex_replace")));
        querqyQuery.setMatchingQuery(new MatchingQuery("162x 12"));
        querqyQuery.setMinimumShouldMatch("1");
        querqyQuery.setQueryFieldsAndBoostings(singletonList("field1"));

        final SearchRequestBuilder searchRequestBuilder = client().prepareSearch(getIndexName());
        searchRequestBuilder.setQuery(querqyQuery);

        final SearchResponse response = client().search(searchRequestBuilder.request()).get();
        SearchHits hits = response.getHits();

        assertEquals(1L, hits.getTotalHits().value());
        assertEquals("1", hits.getAt(0).getSourceAsMap().get("id"));
        response.decRef();

    }

}
