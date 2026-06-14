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
package querqy.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import querqy.elasticsearch.query.MatchingQuery;
import querqy.elasticsearch.query.QuerqyQueryBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;

public class LuceneQueryBuildingIntegrationTest extends ESSingleNodeTestCase {

    private final String INDEX_NAME = "test_index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(QuerqyPlugin.class);
    }

    public void testThatQueryAnalyzerIsApplied() throws Exception {

        index();

        final QuerqyQueryBuilder queryAnalyzerMatch = new QuerqyQueryBuilder(
                getInstanceFromNode(QuerqyProcessor.class));
        queryAnalyzerMatch.setMatchingQuery(new MatchingQuery("abc."));
        queryAnalyzerMatch.setQueryFieldsAndBoostings(Collections.singletonList("field0"));

        SearchRequestBuilder searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(queryAnalyzerMatch);

        SearchResponse response = client().search(searchRequestBuilder.request()).get();
        assertEquals(1L, response.getHits().getTotalHits().value());
        response.decRef();

        final QuerqyQueryBuilder queryAnalyzerMismatch = new QuerqyQueryBuilder(
                getInstanceFromNode(QuerqyProcessor.class));
        queryAnalyzerMismatch.setMatchingQuery(new MatchingQuery("abc."));
        queryAnalyzerMismatch.setQueryFieldsAndBoostings(Collections.singletonList("field1"));

        searchRequestBuilder = client().prepareSearch(INDEX_NAME);
        searchRequestBuilder.setQuery(queryAnalyzerMismatch);

        response = client().search(searchRequestBuilder.request()).get();
        assertEquals(0L, response.getHits().getTotalHits().value());
        response.decRef();
    }

    public void index() {
        createIndex(INDEX_NAME,
            client().admin().indices().prepareCreate(INDEX_NAME)
                    .setMapping(readUtf8Resource("lucene-query-it-mapping.json")));


        client().prepareIndex(INDEX_NAME)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("field0", "abc. def", "field1", "abc. def")
                .get();
    }

    private static String readUtf8Resource(final String name) {
        final Scanner scanner = new Scanner(LuceneQueryBuildingIntegrationTest.class.getClassLoader()
                .getResourceAsStream(name),
                StandardCharsets.UTF_8.name()).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }
}
