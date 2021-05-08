package querqy.elasticsearch.query;

import static org.junit.Assert.*;

import org.junit.Test;
import querqy.elasticsearch.infologging.LogPayloadType;

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


    }




}