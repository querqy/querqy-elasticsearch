package querqy.elasticsearch.infologging;

import querqy.lucene.rewrite.infologging.InfoLogging;
import querqy.lucene.rewrite.infologging.InfoLoggingContext;
import querqy.rewrite.SearchEngineRequestAdapter;

public class ESInfoLoggingContext extends InfoLoggingContext {


    public ESInfoLoggingContext(final InfoLogging infoLogging,
                                final SearchEngineRequestAdapter searchEngineRequestAdapter) {
        super(infoLogging, searchEngineRequestAdapter);
    }

    @Override
    public void log(final Object message) {
        if (isEnabledForRewriter()) {
            super.log(message);
        }
    }


}
