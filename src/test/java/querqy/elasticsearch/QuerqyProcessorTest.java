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
import org.mockito.runners.MockitoJUnitRunner;
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
                Collections.singletonList(new TermQuery(new Term("f1", "boost"))), new TermQuery(new Term("f1", "a")),
                false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class));
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
                false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class));
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
                false);

        final BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("f1", "u")), BooleanClause.Occur.MUST);


        final QuerqyProcessor querqyProcessor = new QuerqyProcessor(mock(RewriterShardContexts.class));
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