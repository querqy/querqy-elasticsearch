package querqy.elasticsearch.rewriterstore;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import querqy.elasticsearch.TermQueryCacheInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PutRewriteChainRequest extends ActionRequest {

    private final static TermQueryCacheInfo NO_CACHE = new TermQueryCacheInfo(Collections.emptySet(), 0, false);

    //private List<String> rewriterIds;
    private Map<String, Object> content;
    private String chainId;
    private String routing;
    //private TermQueryCacheInfo termQueryCacheInfo = NO_CACHE;


    public PutRewriteChainRequest() {
        super();
    }

    public PutRewriteChainRequest(final String chainId, final Map<String, Object> content, final String routing) {
        super();
        this.chainId = chainId;
//        this.rewriterIds = rewriterIds == null ? Collections.emptyList() : rewriterIds;
        this.routing = routing;
        this.content = content;
//        this.termQueryCacheInfo = termQueryCacheInfo;
    }


    @Override
    public ActionRequestValidationException validate() {

        final List<String> errors = new ArrayList<>(4);
        final Object rewriters = content.get("rewriters");
        if (rewriters == null) {
            errors.add("Rewrite chain must have one or more 'rewriters'");
        } else {
            if (!(rewriters instanceof Collection)) {
                errors.add("Collection 'rewriters' expected");
            } else {
                if (((Collection) rewriters).isEmpty()) {
                    errors.add("'rewriters' must not be empty");
                }
            }
        }


        final Object cacheInfo = content.get("term_query_cache");
        if (cacheInfo != null) {
            if (!(cacheInfo instanceof Map)) {
                errors.add("'term_query_cache' must be an object");
            } else {
                final Map<String, Object> cacheConf = (Map<String, Object>) cacheInfo;
                final Object fields = cacheConf.get("fields");
                if (fields == null) {
                    errors.add("'term_query_cache' must have a 'fields' property");
                } else {
                    if (!(fields instanceof Collection)) {
                        errors.add("Collection 'term_query_cache.fields' expected");
                    } else {
                        if (((Collection) fields).isEmpty()) {
                            errors.add("'term_query_cache.fields' must not be empty");
                        }
                    }
                }
            }
        }

        if (errors.isEmpty()) {
            return null;
        }
        final ActionRequestValidationException actionRequestValidationException = new ActionRequestValidationException();
        actionRequestValidationException.addValidationErrors(errors);
        return actionRequestValidationException;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(chainId);
        out.writeOptionalString(routing);
        out.writeMap(content);
//        out.writeStringCollection(rewriterIds);

    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        super.readFrom(in);
        chainId = in.readString();
        routing = in.readOptionalString();
        content = in.readMap();
//        rewriterIds = in.readStringList();

    }

//    public List<String> getRewriterIds() {
//        return rewriterIds;
//    }


    public Map<String, Object> getContent() {
        return content;
    }

    public String getChainId() {
        return chainId;
    }

    public String getRouting() {
        return routing;
    }

//    public TermQueryCacheInfo getTermQueryCacheInfo() {
//        return termQueryCacheInfo;
//    }
/*
    public static class TermQueryCacheInfo implements Streamable, Writeable {
        public Set<String> fields;
        public int size;
        public boolean updatable;

        public TermQueryCacheInfo() {}

        public TermQueryCacheInfo(final Set<String> fields, final int size, final boolean updatable) {
            this.fields = fields;
            this.size = size;
            this.updatable = updatable;
        }

        public boolean isValid() {
            return size == 0 || (fields != null && !fields.isEmpty());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof querqy.elasticsearch.TermQueryCacheInfo)) return false;
            querqy.elasticsearch.TermQueryCacheInfo that = (querqy.elasticsearch.TermQueryCacheInfo) o;
            return size == that.size &&
                    updatable == that.updatable &&
                    Objects.equals(fields, that.fields);
        }

        @Override
        public int hashCode() {

            return Objects.hash(fields, size, updatable);
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            size = in.readInt();
            if (size != 0) {
                updatable = in.readBoolean();
                fields = new HashSet<>(in.readStringList());
            }
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeInt(size);
            if (size != 0) {
                out.writeBoolean(updatable);
                out.writeStringCollection(fields);
            }

        }
    }

*/




}
