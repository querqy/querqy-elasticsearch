package querqy.elasticsearch.aggregation;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ParsedDecorationAggregation extends ParsedAggregation implements DecorationAggregation {

    private List<Object> aggregation;

    @Override
    public String getType() {
        return QuerqyDecorationAggregationBuilder.NAME;
    }

    @Override
    public Object aggregation() {
        assert aggregation.size() == 1;
        return aggregation.get(0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder.field(CommonFields.VALUE.getPreferredName(), aggregation());
    }

    private static final ObjectParser<ParsedDecorationAggregation, Void> PARSER = new ObjectParser<>(
            ParsedDecorationAggregation.class.getSimpleName(),
            true,
            ParsedDecorationAggregation::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareField(
                (agg, value) -> agg.aggregation = Collections.singletonList(value),
                ParsedDecorationAggregation::parseValue,
                CommonFields.VALUE,
                ObjectParser.ValueType.VALUE_OBJECT_ARRAY
        );
    }

    private static Object parseValue(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Object value = null;
        if (token == XContentParser.Token.VALUE_NULL) {
            value = null;
        } else if (token.isValue()) {
            if (token == XContentParser.Token.VALUE_STRING) {
                // binary values will be parsed back and returned as base64 strings when reading from json and yaml
                value = parser.text();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                value = parser.numberValue();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                value = parser.booleanValue();
            } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                // binary values will be parsed back and returned as BytesArray when reading from cbor and smile
                value = new BytesArray(parser.binaryValue());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            value = parser.map();
        } else if (token == XContentParser.Token.START_ARRAY) {
            value = parser.list();
        }
        return value;
    }

    public static ParsedDecorationAggregation fromXContent(XContentParser parser, final String name) {
        ParsedDecorationAggregation aggregation = PARSER.apply(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

}
