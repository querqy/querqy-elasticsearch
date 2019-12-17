package querqy.elasticsearch.query;

import static org.elasticsearch.index.query.AbstractQueryBuilder.DEFAULT_BOOST;
import static querqy.lucene.rewrite.SearchFieldsAndBoosting.*;

import org.elasticsearch.common.ParseField;
import querqy.lucene.QuerySimilarityScoring;
import querqy.lucene.rewrite.SearchFieldsAndBoosting;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface RequestUtils {

    static Map<String, Float> paramToQueryFieldsAndBoosting(final Iterable<String> param) {

        if (param == null) {
            return Collections.emptyMap();
        }

        final Map<String, Float> qf = new HashMap<>();

        for (final String fieldname: param) {

            final int weightPos = fieldname.indexOf('^');
            if (weightPos == 0) {
                throw new IllegalArgumentException("field cannot start with ^: " + fieldname);
            }

            if (weightPos > -1) {
                if (qf.put(fieldname.substring(0, weightPos),
                        Float.parseFloat(fieldname.substring(weightPos + 1))) != null) {
                    throw new IllegalArgumentException("Duplicate field: " + fieldname);
                }
            } else {
                if (qf.put(fieldname, DEFAULT_BOOST)!= null) {
                    throw new IllegalArgumentException("Duplicate field: " + fieldname);
                }
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

    static FieldBoostModel paramToFieldBoostModel(final String paramValue, final ParseField field) {
        if (paramValue == null) {
            return null;
        } else {
            switch (paramValue) {
                case "prms":
                    return FieldBoostModel.PRMS;
                case "fixed":
                    return FieldBoostModel.FIXED;
                // TODO: can we handle FieldBoostModel.NONE?;
                default:
                    throw new IllegalArgumentException("Invalid value for " + field.getPreferredName() + ": " +
                            paramValue);
            }
        }
    }

    static Optional<String> fieldBoostModelToString(final FieldBoostModel fieldBoostModel) {
        if (fieldBoostModel == null) {
            return Optional.empty();
        }
        switch(fieldBoostModel) {
            case FIXED:
                return Optional.of("fixed"); // FIXME: use constants
            case PRMS:
                return Optional.of("prms"); // FIXME: use constants
            // TODO: can we handle FieldBoostModel.NONE?;
            default:
                throw new IllegalStateException("fieldBoostModel set to unknown value: " +
                        fieldBoostModel);

        }
    }


}
