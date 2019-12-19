package querqy.elasticsearch.query;

import static org.elasticsearch.common.xcontent.XContentHelper.*;
import static org.elasticsearch.common.xcontent.XContentType.*;
import static org.junit.Assert.*;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class RewriterTest {

    @Test
    public void testStreamSerializationWithParams() throws IOException {

        final Rewriter rewriter1 = new Rewriter("rewriter_1");

        final Map<String, Object> params = new HashMap<>();

        final Map<String, Object> criteria = new HashMap<>();
        criteria.put("sort", "prio desc");
        criteria.put("limit", 1);
        params.put("criteria", criteria);
        rewriter1.setParams(params);

        final BytesStreamOutput output = new BytesStreamOutput();
        rewriter1.writeTo(output);
        output.flush();

        final Rewriter rewriter2 = new Rewriter(output.bytes().streamInput());
        assertEquals(rewriter1, rewriter2);
        assertEquals(rewriter1.hashCode(), rewriter2.hashCode());
        assertEquals(rewriter1.getName(), rewriter2.getName());
        assertEquals(rewriter1.getParams(), rewriter2.getParams());

    }

    @Test
    public void testStreamSerializationWithoutParams() throws IOException {

        final Rewriter rewriter1 = new Rewriter("rewriter_1");

        final BytesStreamOutput output = new BytesStreamOutput();
        rewriter1.writeTo(output);
        output.flush();

        final Rewriter rewriter2 = new Rewriter(output.bytes().streamInput());
        assertEquals(rewriter1, rewriter2);
        assertEquals(rewriter1.hashCode(), rewriter2.hashCode());
        assertEquals(rewriter1.getName(), rewriter2.getName());
        assertEquals(rewriter1.getParams(), rewriter2.getParams());

    }

    @Test
    public void testToJsonWithParams() throws IOException {

        final Rewriter rewriter1 = new Rewriter("some_rewriter");

        final Map<String, Object> params = new HashMap<>();

        final Map<String, Object> criteria = new HashMap<>();
        criteria.put("sort", "prio desc");
        criteria.put("limit", 1);
        params.put("criteria", criteria);
        rewriter1.setParams(params);

        assertFalse(rewriter1.isFragment());

        final XContentParser parser = createParser(null, null, toXContent(rewriter1, JSON, true), JSON);
        parser.nextToken();

        final Rewriter rewriter2 = Rewriter.PARSER.parse(parser, null);

        assertEquals(rewriter1, rewriter2);
        assertEquals(rewriter1.hashCode(), rewriter2.hashCode());
        assertEquals(rewriter1.getName(), rewriter2.getName());
        assertEquals(rewriter1.getParams(), rewriter2.getParams());

    }

    @Test
    public void testToFragmentWithoutParams() {

        final Rewriter rewriter1 = new Rewriter("some_rewriter");
        assertTrue(rewriter1.isFragment());

    }
}