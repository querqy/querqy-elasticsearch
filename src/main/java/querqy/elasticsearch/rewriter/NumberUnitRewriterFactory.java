package querqy.elasticsearch.rewriter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.elasticsearch.rewriter.numberunit.NumberUnitConfigObject;
import querqy.elasticsearch.rewriter.numberunit.NumberUnitConfigObject.NumberUnitDefinitionObject;
import querqy.elasticsearch.rewriter.numberunit.NumberUnitQueryCreatorElasticsearch;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.contrib.numberunit.model.FieldDefinition;
import querqy.rewrite.contrib.numberunit.model.NumberUnitDefinition;
import querqy.rewrite.contrib.numberunit.model.UnitDefinition;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class NumberUnitRewriterFactory extends ESRewriterFactory {

    private static final String EXCEPTION_MESSAGE = "NumberUnitRewriter not properly configured. " +
            "At least one unit and one field need to be properly defined, e. g. \n" +
            "{\n" +
            "  \"numberUnitDefinitions\": [\n" +
            "    {\n" +
            "      \"units\": [ { \"term\": \"cm\" } ],\n" +
            "      \"fields\": [ { \"fieldName\": \"weight\" } ]\n" +
            "    }\n" +
            "  ]\n" +
            "}\n";

    private static final int DEFAULT_UNIT_MULTIPLIER = 1;

    private static final int DEFAULT_SCALE_FOR_LINEAR_FUNCTIONS = 5;
    private static final int DEFAULT_FIELD_SCALE = 0;

    private static final float DEFAULT_BOOST_MAX_SCORE_FOR_EXACT_MATCH = 200;
    private static final float DEFAULT_BOOST_MIN_SCORE_AT_UPPER_BOUNDARY = 100;
    private static final float DEFAULT_BOOST_MIN_SCORE_AT_LOWER_BOUNDARY = 100;
    private static final float DEFAULT_BOOST_ADDITIONAL_SCORE_FOR_EXACT_MATCH = 100;

    private static final float DEFAULT_BOOST_PERCENTAGE_UPPER_BOUNDARY = 20;
    private static final float DEFAULT_BOOST_PERCENTAGE_LOWER_BOUNDARY = 20;
    private static final float DEFAULT_BOOST_PERCENTAGE_UPPER_BOUNDARY_EXACT_MATCH = 0;
    private static final float DEFAULT_BOOST_PERCENTAGE_LOWER_BOUNDARY_EXACT_MATCH = 0;

    private static final float DEFAULT_FILTER_PERCENTAGE_LOWER_BOUNDARY = 20;
    private static final float DEFAULT_FILTER_PERCENTAGE_UPPER_BOUNDARY = 20;

    private static final String KEY_CONFIG_PROPERTY = "config";

    private querqy.rewrite.contrib.NumberUnitRewriterFactory delegate;

    public NumberUnitRewriterFactory(String rewriterId) {
        super(rewriterId);
    }

    @Override
    public RewriterFactory createRewriterFactory(IndexShard indexShard) {
        return delegate;
    }

    @Override
    public void configure(Map<String, Object> config) {
        final Object numberUnitConfig = config.get(KEY_CONFIG_PROPERTY);

        final NumberUnitConfigObject numberUnitConfigObject;
        try {
            numberUnitConfigObject = new ObjectMapper().readValue(
                    (String) numberUnitConfig, NumberUnitConfigObject.class);
        } catch (IOException e) {
            // checked in this::validateConfiguration
            return;
        }

        final int scale = getOrDefaultInt(numberUnitConfigObject::getScaleForLinearFunctions,
                DEFAULT_SCALE_FOR_LINEAR_FUNCTIONS);

        this.delegate = new querqy.rewrite.contrib.NumberUnitRewriterFactory(
                rewriterId, parseConfig(numberUnitConfigObject), new NumberUnitQueryCreatorElasticsearch(scale));

    }

    @Override
    public List<String> validateConfiguration(Map<String, Object> config) {

        final Object numberUnitConfig = config.get(KEY_CONFIG_PROPERTY);
        if (!(numberUnitConfig instanceof String)) {
            return Collections.singletonList("Property 'config' not or not properly configured");
        }

        // resource InputStream will be closed by Jackson Json Parser
        final NumberUnitConfigObject numberUnitConfigObject;
        try {
            numberUnitConfigObject = new ObjectMapper().readValue(
                    (String) numberUnitConfig, NumberUnitConfigObject.class);

            final List<NumberUnitDefinition> numberUnitDefinitions = parseConfig(numberUnitConfigObject);
            numberUnitDefinitions.stream()
                    .filter(this::numberUnitDefinitionHasDuplicateUnitDefinition)
                    .findFirst()
                    .ifPresent(numberUnitDefinition -> {
                        throw new IllegalArgumentException("Units must only defined once per NumberUnitDefinition");});

        } catch (IOException | IllegalArgumentException e) {
            return Collections.singletonList(e.getMessage());
        }

        return Collections.emptyList();
    }

    protected boolean numberUnitDefinitionHasDuplicateUnitDefinition(final NumberUnitDefinition numberUnitDefinition) {
        final Set<String> observedUnits = new HashSet<>();
        for (final UnitDefinition unitDefinition : numberUnitDefinition.unitDefinitions) {
            if (!observedUnits.add(unitDefinition.term)) {
                return true;
            }
        }
        return false;
    }

    protected List<NumberUnitDefinition> parseConfig(final NumberUnitConfigObject numberUnitConfigObject) {

        final List<NumberUnitDefinitionObject> numberUnitDefinitionObjects =
                numberUnitConfigObject.getNumberUnitDefinitions();

        if (numberUnitDefinitionObjects == null || numberUnitDefinitionObjects.isEmpty()) {
            throw new IllegalArgumentException(EXCEPTION_MESSAGE);
        }

        return numberUnitDefinitionObjects.stream().map(this::parseNumberUnitDefinition).collect(Collectors.toList());

    }

    private NumberUnitDefinition parseNumberUnitDefinition(final NumberUnitDefinitionObject defObj) {

        final NumberUnitDefinition.Builder builder = NumberUnitDefinition.builder()
                .addUnits(this.parseUnitDefinitions(defObj))
                .addFields(this.parseFieldDefinitions(defObj));

        final NumberUnitConfigObject.BoostObject boost = defObj.getBoost() != null
                ? defObj.getBoost()
                : new NumberUnitConfigObject.BoostObject();

        builder
                .setMaxScoreForExactMatch(getOrDefaultBigDecimalForFloat(
                        boost::getMaxScoreForExactMatch, DEFAULT_BOOST_MAX_SCORE_FOR_EXACT_MATCH))
                .setMinScoreAtUpperBoundary(getOrDefaultBigDecimalForFloat(
                        boost::getMinScoreAtUpperBoundary, DEFAULT_BOOST_MIN_SCORE_AT_UPPER_BOUNDARY))
                .setMinScoreAtLowerBoundary(getOrDefaultBigDecimalForFloat(
                        boost::getMinScoreAtLowerBoundary, DEFAULT_BOOST_MIN_SCORE_AT_LOWER_BOUNDARY))
                .setAdditionalScoreForExactMatch(getOrDefaultBigDecimalForFloat(
                        boost::getAdditionalScoreForExactMatch, DEFAULT_BOOST_ADDITIONAL_SCORE_FOR_EXACT_MATCH))
                .setBoostPercentageUpperBoundary(getOrDefaultBigDecimalForFloat(
                        boost::getPercentageUpperBoundary, DEFAULT_BOOST_PERCENTAGE_UPPER_BOUNDARY))
                .setBoostPercentageLowerBoundary(getOrDefaultBigDecimalForFloat(
                        boost::getPercentageLowerBoundary, DEFAULT_BOOST_PERCENTAGE_LOWER_BOUNDARY))
                .setBoostPercentageUpperBoundaryExactMatch(getOrDefaultBigDecimalForFloat(
                        boost::getPercentageUpperBoundaryExactMatch, DEFAULT_BOOST_PERCENTAGE_UPPER_BOUNDARY_EXACT_MATCH))
                .setBoostPercentageLowerBoundaryExactMatch(getOrDefaultBigDecimalForFloat(
                        boost::getPercentageLowerBoundaryExactMatch, DEFAULT_BOOST_PERCENTAGE_LOWER_BOUNDARY_EXACT_MATCH));

        final NumberUnitConfigObject.FilterObject filter = defObj.getFilter() != null
                ? defObj.getFilter()
                : new NumberUnitConfigObject.FilterObject();

        builder
                .setFilterPercentageUpperBoundary(getOrDefaultBigDecimalForFloat(
                        filter::getPercentageUpperBoundary, DEFAULT_FILTER_PERCENTAGE_UPPER_BOUNDARY))
                .setFilterPercentageLowerBoundary(getOrDefaultBigDecimalForFloat(
                        filter::getPercentageLowerBoundary, DEFAULT_FILTER_PERCENTAGE_LOWER_BOUNDARY));

        return builder.build();
    }


    private List<UnitDefinition> parseUnitDefinitions(final NumberUnitDefinitionObject numberUnitDefinitionObject) {
        final List<NumberUnitConfigObject.UnitObject> unitObjects = numberUnitDefinitionObject.getUnits();
        if (unitObjects == null || unitObjects.isEmpty()) {
            throw new IllegalArgumentException(EXCEPTION_MESSAGE);
        }

        return unitObjects.stream()
                .peek(unitObject -> {
                    if (isBlank(unitObject.getTerm())) {
                        throw new IllegalArgumentException("Unit definition requires a term to be defined");
                    }})
                .map(unitObject -> new UnitDefinition(
                        unitObject.getTerm(),
                        getOrDefaultBigDecimalForFloat(unitObject::getMultiplier, DEFAULT_UNIT_MULTIPLIER)))
                .collect(Collectors.toList());
    }

    private List<FieldDefinition> parseFieldDefinitions(final NumberUnitDefinitionObject numberUnitDefinitionObject) {
        final List<NumberUnitConfigObject.FieldObject> fieldObjects = numberUnitDefinitionObject.getFields();
        if (fieldObjects == null || fieldObjects.isEmpty()) {
            throw new IllegalArgumentException(EXCEPTION_MESSAGE);
        }

        return fieldObjects.stream()
                .peek(fieldObject -> {
                    if (isBlank(fieldObject.getFieldName())) {
                        throw new IllegalArgumentException("Unit definition requires a term to be defined");
                    }})
                .map(fieldObject -> new FieldDefinition(
                        fieldObject.getFieldName(),
                        getOrDefaultInt(fieldObject::getScale, DEFAULT_FIELD_SCALE)))
                .collect(Collectors.toList());
    }

    private BigDecimal getOrDefaultBigDecimalForFloat(final Supplier<Float> supplier, final float defaultValue) {
        final Float value = supplier.get();
        return value != null ? BigDecimal.valueOf(value) : BigDecimal.valueOf(defaultValue);
    }

    private int getOrDefaultInt(final Supplier<Integer> supplier, final int defaultValue) {
        final Integer value = supplier.get();
        return value != null ? value : defaultValue;
    }

    public static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs != null && (strLen = cs.length()) != 0) {
            for(int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(cs.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

}
