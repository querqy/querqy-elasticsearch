package querqy.elasticsearch.query;

import static org.elasticsearch.common.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION;
import static org.junit.Assert.*;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Test;
import querqy.elasticsearch.infologging.LogPayloadType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class InfoLoggingSpecTest {

    @Test
    public void testThatRewriterIdIsDefaultPayloadType() {
        assertEquals(LogPayloadType.NONE, new InfoLoggingSpec().getPayloadType());
    }

    @Test
    public void testEqualsHashCode() {
        InfoLoggingSpec spec1 = new InfoLoggingSpec();
        assertFalse(spec1.equals(null));
        InfoLoggingSpec spec2 = new InfoLoggingSpec();

        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());

        spec1.setId("idx");
        assertNotEquals(spec1, spec2);
        spec2.setId("idx");
        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());
        spec2.setPayloadType("REWRITER_ID");
        assertNotEquals(spec1, spec2);
        spec1.setPayloadType("REWRITER_ID");
        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());

        // logged property must not be considered
        spec1.setLogged(true);
        spec2.setLogged(false);

        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());

    }


    @Test
    public void testWriteReadJson() throws IOException {

        final InfoLoggingSpec spec =  new InfoLoggingSpec();
        spec.setId("ID1");
        spec.setPayloadType("REWRITER_ID");
        spec.setLogged(true);

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        final XContentBuilder xContentBuilder = new XContentBuilder(JsonXContent.jsonXContent, os);
        spec.toXContent(xContentBuilder, null);
        xContentBuilder.flush();
        xContentBuilder.close();

        final InfoLoggingSpec spec2 = fromJsonInnerObject(os.toByteArray());

        assertEquals(spec, spec2);
        assertEquals(spec.isLogged(), spec2.isLogged());
    }

    @Test
    public void testWriteReadStream() throws IOException {
        final InfoLoggingSpec spec =  new InfoLoggingSpec();
        spec.setId("ID2");
        spec.setPayloadType("DETAIL");
        spec.setLogged(true);

        final BytesStreamOutput out = new BytesStreamOutput();
        spec.writeTo(out);
        out.flush();
        out.close();

        final InfoLoggingSpec spec2 = new InfoLoggingSpec(out.bytes().streamInput());

        assertEquals(spec, spec2);
        assertEquals(spec.isLogged(), spec2.isLogged());

    }

    private InfoLoggingSpec fromJsonInnerObject(final byte[] bytes) throws IOException {
        XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, THROW_UNSUPPORTED_OPERATION, bytes);
        return InfoLoggingSpec.PARSER.parse(parser, null);
    }

}