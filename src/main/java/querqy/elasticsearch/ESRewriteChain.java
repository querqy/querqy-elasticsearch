package querqy.elasticsearch;

import querqy.lucene.rewrite.cache.TermQueryCache;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.RewriterFactory;

import java.util.List;

public class ESRewriteChain extends RewriteChain {


    private final TermQueryCache termQueryCache;


    public ESRewriteChain(final List<RewriterFactory> factories, final TermQueryCache termQueryCache) {
        super(factories);
        this.termQueryCache = termQueryCache;
    }

    public TermQueryCache getTermQueryCache() {
        return termQueryCache;
    }
}
