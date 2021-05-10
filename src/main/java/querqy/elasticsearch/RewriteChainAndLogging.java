package querqy.elasticsearch;

import querqy.rewrite.RewriteChain;

import java.util.Set;

public class RewriteChainAndLogging {

    public final RewriteChain rewriteChain;
    public final Set<String> rewritersEnabledForLogging;

    public RewriteChainAndLogging(final RewriteChain rewriteChain, final Set<String> rewritersEnabledForLogging) {
        this.rewriteChain = rewriteChain;
        this.rewritersEnabledForLogging = rewritersEnabledForLogging;
    }
}
