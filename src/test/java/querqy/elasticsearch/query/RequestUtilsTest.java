package querqy.elasticsearch.query;

import static org.junit.Assert.*;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class RequestUtilsTest {

    @Test
    public void testParamToQueryFieldsAndBoostingReturnsEmptyMapForNullParam() {
        final Map<String, Float> qf = RequestUtils.paramToQueryFieldsAndBoosting(null);
        assertNotNull(qf);
        assertTrue(qf.isEmpty());
    }

    @Test
    public void testParamToQueryFieldsAndBoostingReturnsEmptyMapForEmptyParam() {
        final Map<String, Float> qf = RequestUtils
                .paramToQueryFieldsAndBoosting(Collections.emptyList());
        assertNotNull(qf);
        assertTrue(qf.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamToQueryFieldsAndBoostingDoesntAcceptWeightAtBeginning() {
        RequestUtils.paramToQueryFieldsAndBoosting(Arrays.asList("f1", "^32"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamToQueryFieldsAndBoostingDoesntHatchAtEnd() {
        RequestUtils.paramToQueryFieldsAndBoosting(Arrays.asList("f1", "f2^"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamToQueryFieldsAndBoostingDoesntAcceptDuplicateFieldnameWithWeightOnOne() {
        RequestUtils.paramToQueryFieldsAndBoosting(Arrays.asList("f0", "f1", "f1^0.3"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamToQueryFieldsAndBoostingDoesntAcceptDuplicateFieldnameWithWeightOnBoth() {
        RequestUtils.paramToQueryFieldsAndBoosting(Arrays.asList("f0", "f1^0.3", "f1^0.3"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParamToQueryFieldsAndBoostingDoesntAcceptDuplicateFieldnameWithWeightOnNone() {
        RequestUtils.paramToQueryFieldsAndBoosting(Arrays.asList("f0", "f1", "f1"));
    }

    @Test
    public void testParamToQueryFieldsAndBoostingReturnCorrectFieldsAndWeights() {
        final Map<String, Float> qf = RequestUtils.paramToQueryFieldsAndBoosting(
                Arrays.asList("f0", "f1^0.4", "f2", "f3^20"));
        assertNotNull(qf);
        assertEquals(4, qf.size());
        assertEquals(AbstractQueryBuilder.DEFAULT_BOOST, qf.get("f0"), 0.0001f);
        assertEquals(0.4f, qf.get("f1"), 0.0001f);
        assertEquals(AbstractQueryBuilder.DEFAULT_BOOST, qf.get("f2"), 0.0001f);
        assertEquals(20.0f, qf.get("f3"), 0.0001f);
    }

}