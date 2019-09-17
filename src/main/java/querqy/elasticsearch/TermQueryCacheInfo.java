package querqy.elasticsearch;

import java.util.Objects;
import java.util.Set;

public class TermQueryCacheInfo {
    public final Set<String> fields;
    public final int size;
    public final boolean updatable;

    public TermQueryCacheInfo(final Set<String> fields, final int size, final boolean updatable) {
        this.fields = fields;
        this.size = size;
        this.updatable = updatable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TermQueryCacheInfo)) return false;
        TermQueryCacheInfo that = (TermQueryCacheInfo) o;
        return size == that.size &&
                updatable == that.updatable &&
                Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {

        return Objects.hash(fields, size, updatable);
    }
}
