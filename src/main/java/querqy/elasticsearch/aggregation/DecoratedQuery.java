package querqy.elasticsearch.aggregation;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class DecoratedQuery extends Query {

    final private BooleanQuery booleanQuery;
    final private List<Object> decorations;

    private DecoratedQuery(BooleanQuery booleanQuery, List<Object> decorations) {
        this.booleanQuery = Objects.requireNonNull(booleanQuery);
        this.decorations = Objects.requireNonNull(decorations);
    }

    public static Query from(BooleanQuery booleanQuery, List<Object> decorations) {
        return new DecoratedQuery(booleanQuery, decorations);
    }

    public BooleanQuery getBooleanQuery() {
        return booleanQuery;
    }

    public List<Object> getDecorations() {
        return decorations;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return booleanQuery.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return booleanQuery.rewrite(reader);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        booleanQuery.visit(visitor);
    }

    @Override
    public String toString(String field) {
        return booleanQuery.toString();
    }

    @Override
    public boolean equals(Object o) {
        return sameClassAs(o) &&
                equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(DecoratedQuery other) {
        return booleanQuery.getMinimumNumberShouldMatch() == other.getBooleanQuery().getMinimumNumberShouldMatch() &&
                booleanQuery.clauses().equals(other.getBooleanQuery().clauses()) &&
                decorations.equals(other.getDecorations());
    }

    private int computeHashCode() {
        int hashCode = Objects.hash(booleanQuery, decorations);
        if (hashCode == 0) {
            hashCode = 1;
        }
        return hashCode;
    }

    // cached hash code is ok since boolean queries are immutable
    private int hashCode;

    @Override
    public int hashCode() {
        // no need for synchronization, in the worst case we would just compute the hash several times.
        if (hashCode == 0) {
            hashCode = computeHashCode();
            assert hashCode != 0;
        }
        assert hashCode == computeHashCode();
        return hashCode;
    }

}