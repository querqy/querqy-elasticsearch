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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static querqy.elasticsearch.query.AbstractLuceneQueryTest.*;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import querqy.lucene.LuceneQueries;

import java.util.Arrays;
import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class QuerqyProcessorTest {


    @Test
    public void testThatAppendFilterQueriesForNullFilterQueries() {
        final LuceneQueries queries = new LuceneQueries(
                new TermQuery(new Term("f1", "a")),
                null,
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))),
                new TermQuery(new Term("f1", "a")), null, false, false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class), null);
        querqyProcessor.appendFilterQueries(queries, builder);
        final BooleanQuery booleanQuery = builder.build();
        assertThat(booleanQuery.clauses(), everyItem(not(anyFilter())));
        assertThat(booleanQuery.clauses(), everyItem(not(anyMustNot())));
        assertThat(booleanQuery.clauses(), everyItem(not(anyMust())));

    }

    @Test
    public void testThatAppendFilterQueriesForNoFilterQueries() {

        final LuceneQueries queries = new LuceneQueries(
                new TermQuery(new Term("f1", "a")),
                Collections.emptyList(),
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))), new TermQuery(new Term("f1", "a")),
                null, false, false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class), null);
        querqyProcessor.appendFilterQueries(queries, builder);
        final BooleanQuery booleanQuery = builder.build();
        assertThat(booleanQuery.clauses(), everyItem(not(anyFilter())));
        assertThat(booleanQuery.clauses(), everyItem(not(anyMustNot())));
        assertThat(booleanQuery.clauses(), everyItem(not(anyMust())));

    }

    @Test
    public void testThatAllNegativeFilterQueryGetsAppended() {
        final DisjunctionMaxQuery dmqNeg = new DisjunctionMaxQuery(Arrays.asList(new TermQuery(new Term("f1", "filter_a")),
                new TermQuery(new Term("f2", "filter_a"))), 1f);

        final BooleanQuery.Builder container = new BooleanQuery.Builder();
        container.add(dmqNeg, BooleanClause.Occur.MUST_NOT);

        final LuceneQueries queries = new LuceneQueries(
                new TermQuery(new Term("f1", "u")),
                Collections.singletonList(container.build()),
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))), new TermQuery(new Term("f1", "u")),
                null, false, false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("f1", "u")), BooleanClause.Occur.MUST);


        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class), null);
        querqyProcessor.appendFilterQueries(queries, builder);

        final BooleanQuery booleanQuery = builder.build();

        assertThat(booleanQuery,
                bq(
                        tq(BooleanClause.Occur.MUST, "f1", "u"),
                        dmq(BooleanClause.Occur.MUST_NOT,
                            tq("f1", "filter_a"),
                            tq("f2", "filter_a")
                        )
                )

        );

    }
}