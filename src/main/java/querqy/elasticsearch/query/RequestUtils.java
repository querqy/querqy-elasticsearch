package querqy.elasticsearch.query;

import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;

import org.elasticsearch.common.ParseField;
import querqy.lucene.QuerySimilarityScoring;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RequestUtils {

    static Map<String, Float> paramToQueryFieldsAndBoosting(final List<String> param) {

        if (param == null) {
            return Collections.emptyMap();
        }

        final Map<String, Float> qf = new HashMap<>();

        for (final String fieldElement: param) {

            final int weightPos = fieldElement.indexOf('^');
            if (weightPos == 0) {
                throw new IllegalArgumentException("field cannot start with ^: " + fieldElement);
            }

            if (weightPos > -1) {
                qf.put(fieldElement.substring(0, weightPos), Float.parseFloat(fieldElement.substring(weightPos + 1)));
            } else {
                qf.put(fieldElement, DEFAULT_BOOST);
            }

        }

        return qf;
    }


    static QuerySimilarityScoring paramToQuerySimilarityScoring(final String paramValue, final ParseField field) {
        if (paramValue == null) {
            return null;
        } else {
            switch (paramValue) {
                case "dfc":
                    return QuerySimilarityScoring.DFC;
                case "off":
                    return QuerySimilarityScoring.SIMILARITY_SCORE_OFF;
                case "on":
                    return QuerySimilarityScoring.SIMILARITY_SCORE_ON;
                default:
                    throw new IllegalArgumentException("Invalid value for " + field.getPreferredName() + ": " +
                            paramValue);
            }
        }
    }

    static Optional<String> querySimilarityScoringToString(final QuerySimilarityScoring querySimilarityScoring) {
        if (querySimilarityScoring == null) {
            return Optional.empty();
        }
        switch(querySimilarityScoring) {
            case DFC:
                return Optional.of("dfc"); // FIXME: use constants
            case SIMILARITY_SCORE_ON:
                return Optional.of("on"); // FIXME: use constants
            case SIMILARITY_SCORE_OFF:
                return Optional.of("off"); // FIXME: use constants
            default:
                throw new IllegalStateException("querySimilarityScoring set to unknown value: " +
                        querySimilarityScoring);

        }
    }
}
