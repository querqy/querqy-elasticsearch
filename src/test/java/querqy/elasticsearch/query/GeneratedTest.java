package querqy.elasticsearch.query;

import static org.junit.Assert.*;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GeneratedTest {

    @Test
    public void testStreamSerialization() throws IOException {
        final Generated generated1 = new Generated(Arrays.asList("field1^20.1", "field2^3", "field3"));
        generated1.setFieldBoostFactor(0.8f);
        final BytesStreamOutput output = new BytesStreamOutput();
        generated1.writeTo(output);
        output.flush();

        final Generated generated2 = new Generated(output.bytes().streamInput());
        assertEquals(generated1, generated2);

        // do not trust equals
        assertEquals(generated1.getFieldBoostFactor(), generated2.getFieldBoostFactor());
        assertEquals(generated2.getQueryFieldsAndBoostings(), generated2.getQueryFieldsAndBoostings());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testToJson() throws IOException {
        final Generated generated = new Generated(Arrays.asList("field1^20.1", "field2^3", "field3"));
        generated.setFieldBoostFactor(0.8f);

        final Map<String, Object> parsed;
        try (InputStream stream = XContentHelper.toXContent(generated, XContentType.JSON, true).streamInput()) {
            parsed = XContentHelper.convertToMap(XContentFactory.xContent(XContentType.JSON), stream, false);
        }

        assertNotNull(parsed);
        final List<String> queryFields = (List<String>) parsed.get("query_fields");
        assertThat(queryFields, Matchers.containsInAnyOrder("field1^20.1", "field2^3.0", "field3"));

        final Double fieldBoost = (Double) parsed.get("field_boost_factor");
        assertNotNull(fieldBoost);
        assertEquals(0.8, fieldBoost, 0.000001f);

    }

}