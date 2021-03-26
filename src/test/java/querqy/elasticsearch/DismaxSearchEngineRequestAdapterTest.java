package querqy.elasticsearch;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;
import static org.junit.Assert.*;

import org.junit.Test;
import querqy.elasticsearch.query.QuerqyQueryBuilder;
import querqy.elasticsearch.query.Rewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/*
 * Rewriters will access params using prefix 'querqy.<rewriter id>.....'
 */
public class DismaxSearchEngineRequestAdapterTest {

    @Test
    public void testThatGetParamsReturnsNoneIfItsNotARewriterParamName() {
        // no rewriter at all
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();
        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null, null);
        assertEquals(Optional.empty(), adapter.getParam("not.a.rewriter"));

        // some rewriter
        builder.setRewriters(Collections.singletonList(new Rewriter("rid1")));
        assertEquals(Optional.empty(), adapter.getParam("not.a.rewriter"));

    }


    @Test
    public void testGetParamForUnknownRewriter() {
        // no rewriter at all
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();
        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1"));
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1.x"));

        // some rewriter
        builder.setRewriters(Collections.singletonList(new Rewriter("rid1")));
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid2"));
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid2.x"));
    }

    @Test
    public void testGetParamForKnownRewriterWithoutParams() {

        final Rewriter rewriter = new Rewriter("rid1");
        // no rewriter at all
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();
        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1"));
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1.x"));


    }

    @Test
    public void testGetParamForKnownRewriterWithEmptyParams() {

        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        rewriter.setParams(new HashMap<>());
        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1"));
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1.x"));

    }


    @Test
    public void testGetStringParam() {

        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        final Map<String, Object> params = new HashMap<>();
        params.put("x", "Value1");
        rewriter.setParams(params);

        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getParam("querqy.rid1"));
        assertEquals(Optional.of("Value1"), adapter.getParam("querqy.rid1.x"));

        final Rewriter rewriter2 = new Rewriter("rid2");
        final Map<String, Object> params2 = new HashMap<>();
        params2.put("x", "Value2x");
        params2.put("y", "Value2y");

        rewriter2.setParams(params2);

        builder.setRewriters(Arrays.asList(rewriter, rewriter2));

        assertEquals(Optional.of("Value1"), adapter.getParam("querqy.rid1.x"));
        assertEquals(Optional.of("Value2x"), adapter.getParam("querqy.rid2.x"));
        assertEquals(Optional.of("Value2y"), adapter.getParam("querqy.rid2.y"));

    }

    @Test
    public void testGetRequestParams() {
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        final Map<String, Object> params = new HashMap<>();
        params.put("x", new String[] {"Value11", "Value12"});
        rewriter.setParams(params);

        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertThat(adapter.getRequestParams("querqy.rid1"), emptyArray());
        assertThat(adapter.getRequestParams("querqy.rid1.x"), arrayContaining("Value11", "Value12"));

        final Rewriter rewriter2 = new Rewriter("rid2");
        final Map<String, Object> params2 = new HashMap<>();
        params2.put("x", new String[] {"ValueX-1", "ValueX-2"});
        params2.put("y", new String[] {"ValueY-1", "ValueY-2"});

        rewriter2.setParams(params2);

        builder.setRewriters(Arrays.asList(rewriter, rewriter2, new Rewriter("rid3")));

        assertThat(adapter.getRequestParams("querqy.rid1.x"), arrayContaining("Value11", "Value12"));
        assertThat(adapter.getRequestParams("querqy.rid2.x"), arrayContaining("ValueX-1", "ValueX-2"));
        assertThat(adapter.getRequestParams("querqy.rid2.y"), arrayContaining("ValueY-1", "ValueY-2"));

        assertThat(adapter.getRequestParams("querqy.rid2.z"), emptyArray());
        assertThat(adapter.getRequestParams("querqy.rid3.x"), emptyArray());

    }

    @Test
    public void testGetBooleanRequestParam() {
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        final Map<String, Object> params = new HashMap<>();
        params.put("x", Boolean.FALSE);
        rewriter.setParams(params);

        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getBooleanRequestParam("querqy.rid1"));
        assertEquals(Optional.of(Boolean.FALSE), adapter.getBooleanRequestParam("querqy.rid1.x"));

        final Rewriter rewriter2 = new Rewriter("rid2");
        final Map<String, Object> params2 = new HashMap<>();
        params2.put("x", Boolean.TRUE);
        params2.put("y", Boolean.FALSE);

        rewriter2.setParams(params2);

        builder.setRewriters(Arrays.asList(rewriter, rewriter2));

        assertEquals(Optional.of(Boolean.FALSE), adapter.getBooleanRequestParam("querqy.rid1.x"));
        assertEquals(Optional.of(Boolean.TRUE), adapter.getBooleanRequestParam("querqy.rid2.x"));
        assertEquals(Optional.of(Boolean.FALSE), adapter.getBooleanRequestParam("querqy.rid2.y"));
    }

    @Test
    public void testGetIntegerRequestParam() {
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        final Map<String, Object> params = new HashMap<>();
        params.put("x", 8);
        rewriter.setParams(params);

        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getIntegerRequestParam("querqy.rid1"));
        assertEquals(Optional.of(8), adapter.getIntegerRequestParam("querqy.rid1.x"));

        final Rewriter rewriter2 = new Rewriter("rid2");
        final Map<String, Object> params2 = new HashMap<>();
        params2.put("x", 42);
        params2.put("y", 24);

        rewriter2.setParams(params2);

        builder.setRewriters(Arrays.asList(rewriter, rewriter2));

        assertEquals(Optional.of(8), adapter.getIntegerRequestParam("querqy.rid1.x"));
        assertEquals(Optional.of(42), adapter.getIntegerRequestParam("querqy.rid2.x"));
        assertEquals(Optional.of(24), adapter.getIntegerRequestParam("querqy.rid2.y"));
    }

    @Test
    public void testGetFloatRequestParam() {
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        final Map<String, Object> params = new HashMap<>();
        params.put("x", 0.01f);
        rewriter.setParams(params);

        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getFloatRequestParam("querqy.rid1"));
        assertEquals(Optional.of(0.01f), adapter.getFloatRequestParam("querqy.rid1.x"));

        final Rewriter rewriter2 = new Rewriter("rid2");
        final Map<String, Object> params2 = new HashMap<>();
        params2.put("x", 23.4f);
        params2.put("y", 10008f);

        rewriter2.setParams(params2);

        builder.setRewriters(Arrays.asList(rewriter, rewriter2));

        assertEquals(Optional.of(0.01f), adapter.getFloatRequestParam("querqy.rid1.x"));
        assertEquals(Optional.of(23.4f), adapter.getFloatRequestParam("querqy.rid2.x"));
        assertEquals(Optional.of(10008f), adapter.getFloatRequestParam("querqy.rid2.y"));
    }

    @Test
    public void testGetDoubleRequestParam() {
        final QuerqyQueryBuilder builder = new QuerqyQueryBuilder();

        final Rewriter rewriter = new Rewriter("rid1");
        final Map<String, Object> params = new HashMap<>();
        params.put("x", 0.01);
        rewriter.setParams(params);

        builder.setRewriters(Collections.singletonList(rewriter));

        final DismaxSearchEngineRequestAdapter adapter = new DismaxSearchEngineRequestAdapter(builder, null, null,
                null);
        assertEquals(Optional.empty(), adapter.getDoubleRequestParam("querqy.rid1"));
        assertEquals(Optional.of(0.01), adapter.getDoubleRequestParam("querqy.rid1.x"));

        final Rewriter rewriter2 = new Rewriter("rid2");
        final Map<String, Object> params2 = new HashMap<>();
        params2.put("x", 23.4);
        params2.put("y", 10008.0);

        rewriter2.setParams(params2);

        builder.setRewriters(Arrays.asList(rewriter, rewriter2));

        assertEquals(Optional.of(0.01), adapter.getDoubleRequestParam("querqy.rid1.x"));
        assertEquals(Optional.of(23.4), adapter.getDoubleRequestParam("querqy.rid2.x"));
        assertEquals(Optional.of(10008.0), adapter.getDoubleRequestParam("querqy.rid2.y"));
    }
}