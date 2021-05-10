package querqy.elasticsearch.infologging;

import querqy.elasticsearch.query.InfoLoggingSpec;

import java.util.Optional;

public interface InfoLoggingSpecProvider {

    Optional<InfoLoggingSpec> getInfoLoggingSpec();
}
