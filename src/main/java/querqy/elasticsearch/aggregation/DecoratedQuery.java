package querqy.elasticsearch.aggregation;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class DecoratedQuery<T extends Query> extends Query {

    final private T query;
    final private Set<Object> decorations;

    public DecoratedQuery(final T query, final Set<Object> decorations) {
        this.query = Objects.requireNonNull(query);
        this.decorations = Objects.requireNonNull(decorations);
    }

    public T getQuery() {
        return query;
    }

    public Set<Object> getDecorations() {
        return decorations;
    }

    @Override
    public Weight createWeight(final IndexSearcher searcher, final ScoreMode scoreMode, final float boost) throws IOException {
        return query.createWeight(searcher, scoreMode, boost);
    }

    @Override
    public Query rewrite(final IndexReader reader) throws IOException {
        return query.rewrite(reader);
    }

    @Override
    public String toString(final String field) {
        return query.toString(field);
    }

    @Override
    public boolean equals(final Object object) {
        if (!sameClassAs(object)) return false;
        final DecoratedQuery<?> other = castObject(object);
        return isEqualQueriesAndDecorations(other);
    }

    private boolean isEqualQueriesAndDecorations(final DecoratedQuery<?> other) {
        final Query otherQuery = other.getQuery();
        final Set<Object> otherDecorations = other.getDecorations();
        return getQuery().equals(otherQuery) && getDecorations().equals(otherDecorations);
    }

    private DecoratedQuery<?> castObject(final Object object) {
        return getClass().cast(object);
    }

    private int computeHashCode() {
        int hashCode = Objects.hash(query, decorations);
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
        }
        return hashCode;
    }

}