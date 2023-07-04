package querqy.elasticsearch.infologging;

import querqy.lucene.rewrite.infologging.InfoLogging;
import querqy.lucene.rewrite.infologging.Sink;
import querqy.rewrite.SearchEngineRequestAdapter;

import java.util.Collections;
import java.util.Set;

public class SingleSinkInfoLogging implements InfoLogging {

    private final Sink sink;
    private final Set<String> enabledRewriterIds;

    public SingleSinkInfoLogging(final Sink sink, final Set<String> enabledRewriterIds) {
        this.sink = sink;
        this.enabledRewriterIds = enabledRewriterIds == null
                ? Collections.emptySet()
                : Collections.unmodifiableSet(enabledRewriterIds);
    }

    @Override
    public void log(final Object message, final String rewriterId,
                    final SearchEngineRequestAdapter searchEngineRequestAdapter) {
        if (enabledRewriterIds.contains(rewriterId)) {
            sink.log(message, rewriterId, searchEngineRequestAdapter);
        }
    }

    @Override
    public void endOfRequest(final SearchEngineRequestAdapter searchEngineRequestAdapter) {
        sink.endOfRequest(searchEngineRequestAdapter);
    }

    @Override
    public boolean isLoggingEnabledForRewriter(final String rewriterId) {
        return enabledRewriterIds.contains(rewriterId);
    }
}
