package querqy.elasticsearch.rewriterstore;

import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class RewriterConfigMappingTest {


    @Test
    public void testStringToSourceValue() {
        String s = "123456789ä";
        if (new BytesRef(s).length != 11) {
            throw new IllegalStateException("Test assumptions are wrong: unexpected encoding size: " +
                    new BytesRef(s).length);
        }
        assertEquals(s, RewriterConfigMapping.stringToSourceValue(s, 20));
        assertEquals(s, RewriterConfigMapping.stringToSourceValue(s, 11));

        final Object o1 = RewriterConfigMapping.stringToSourceValue(s, 10);
        assertTrue(o1.getClass().isArray());
        final String[] splits = (String[]) o1;
        assertEquals(2, splits.length);
        assertEquals(s, splits[0] + splits[1]);
        assertTrue(new BytesRef(splits[0]).length <= 10);
        assertTrue(new BytesRef(splits[1]).length <= 10);

        final Object o2 = RewriterConfigMapping.stringToSourceValue(s, 3);
        assertTrue(o2.getClass().isArray());
        final String[] arr = (String[]) o2;
        assertThat(Arrays.stream(arr).map(BytesRef::new).map(bytesRef -> bytesRef.length).collect(Collectors.toList()),
                everyItem(Matchers.lessThanOrEqualTo(3)));
        assertEquals(s, String.join("", arr));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringToSourceValueWithIllegalLimit() {
        RewriterConfigMapping.stringToSourceValue("12345", 2);
    }

}
