package querqy.elasticsearch.aggregation;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregation.CommonFields;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import querqy.elasticsearch.QuerqyPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class InternalDecorationAggregationTests extends InternalAggregationTestCase<InternalDecorationAggregation> {

    private Supplier<Object>[] valueTypes;
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private final Supplier<Object>[] leafValueSuppliers = new Supplier[] {
        () -> randomInt(),
        () -> randomLong(),
        () -> randomDouble(),
        () -> randomFloat(),
        () -> randomBoolean(),
        () -> randomAlphaOfLength(5),
        () -> new GeoPoint(randomDouble(), randomDouble()),
        () -> null };
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private final Supplier<Object>[] nestedValueSuppliers = new Supplier[] { () -> new HashMap<String, Object>(), () -> new ArrayList<>() };

    private static final List<NamedXContentRegistry.Entry> namedXContents = getDefaultNamedXContents();
    static {
        Map<String, ContextParser<Object, ? extends Aggregation>> map = new HashMap<>();
        map.put(QuerqyDecorationAggregationBuilder.NAME, (p, c) -> ParsedDecorationAggregation.fromXContent(p, (String) c));

        List<NamedXContentRegistry.Entry> namedXContentsToAdd = map.entrySet()
                .stream()
                .map(entry -> new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(entry.getKey()), entry.getValue()))
                .collect(Collectors.toList());
        namedXContents.addAll(namedXContentsToAdd);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setUp() throws Exception {
        super.setUp();
        // we want the same value types (also for nested lists, maps) for all random aggregations
        int levels = randomIntBetween(1, 3);
        valueTypes = new Supplier[levels];
        for (int i = 0; i < levels; i++) {
            if (i < levels - 1) {
                valueTypes[i] = randomFrom(nestedValueSuppliers);
            } else {
                // the last one needs to be a leaf value, not map or list
                valueTypes[i] = randomFrom(leafValueSuppliers);
            }
        }
    }

    @Override
    protected SearchPlugin registerPlugin() {
        return new QuerqyPlugin(Settings.EMPTY);
    }

    @Override
    protected InternalDecorationAggregation createTestInstance(String name, Map<String, Object> metadata) {
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            params.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
        }
        return new InternalDecorationAggregation(name, randomAggregations(), metadata);
    }

    private List<Object> randomAggregations() {
        return randomList(randomBoolean() ? 1 : 5, this::randomAggregation);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Object randomAggregation() {
        int levels = randomIntBetween(1, 3);
        Supplier[] valueTypes = new Supplier[levels];
        for (int l = 0; l < levels; l++) {
            if (l < levels - 1) {
                valueTypes[l] = randomFrom(nestedValueSuppliers);
            } else {
                // the last one needs to be a leaf value, not map or
                // list
                valueTypes[l] = randomFrom(leafValueSuppliers);
            }
        }
        return randomValue(valueTypes, 0);
    }

    @SuppressWarnings("unchecked")
    private static Object randomValue(Supplier<Object>[] valueTypes, int level) {
        Object value = valueTypes[level].get();
        if (value instanceof Map) {
            int elements = randomIntBetween(1, 5);
            Map<String, Object> map = (Map<String, Object>) value;
            for (int i = 0; i < elements; i++) {
                map.put(randomAlphaOfLength(5), randomValue(valueTypes, level + 1));
            }
        } else if (value instanceof List) {
            int elements = randomIntBetween(1, 5);
            List<Object> list = (List<Object>) value;
            for (int i = 0; i < elements; i++) {
                list.add(randomValue(valueTypes, level + 1));
            }
        }
        return value;
    }

    @Override
    protected void assertReduced(InternalDecorationAggregation reduced, List<InternalDecorationAggregation> inputs) {
        InternalDecorationAggregation firstAgg = inputs.get(0);
        assertEquals(firstAgg.getName(), reduced.getName());
        assertEquals(firstAgg.getMetadata(), reduced.getMetadata());
        int size = (int) inputs.stream().mapToLong(i -> i.aggregationsList().size()).sum();
        assertEquals(size, ((List<?>) reduced.aggregation()).size());
    }

    @Override
    public InternalDecorationAggregation createTestInstanceForXContent() {
        InternalDecorationAggregation aggregation = createTestInstance();
        return (InternalDecorationAggregation) aggregation.reduce(
            singletonList(aggregation),
            ReduceContext.forFinalReduction(null, mockScriptService(), null, PipelineTree.EMPTY, () -> false)
        );
    }

    @Override
    protected void assertFromXContent(InternalDecorationAggregation aggregation, ParsedAggregation parsedAggregation) throws IOException {
        assertTrue(parsedAggregation instanceof ParsedDecorationAggregation);
        ParsedDecorationAggregation parsed = (ParsedDecorationAggregation) parsedAggregation;

        assertValues(aggregation.aggregation(), parsed.aggregation());
    }

    private static void assertValues(Object expected, Object actual) {
        if (expected instanceof Long) {
            // longs that fit into the integer range are parsed back as integer
            if (actual instanceof Integer) {
                assertEquals(((Long) expected).intValue(), actual);
            } else {
                assertEquals(expected, actual);
            }
        } else if (expected instanceof Float) {
            // based on the xContent type, floats are sometimes parsed back as doubles
            if (actual instanceof Double) {
                assertEquals(expected, ((Double) actual).floatValue());
            } else {
                assertEquals(expected, actual);
            }
        } else if (expected instanceof GeoPoint) {
            assertTrue(actual instanceof Map);
            GeoPoint point = (GeoPoint) expected;
            @SuppressWarnings("unchecked")
            Map<String, Object> pointMap = (Map<String, Object>) actual;
            assertEquals(point.getLat(), pointMap.get("lat"));
            assertEquals(point.getLon(), pointMap.get("lon"));
        } else if (expected instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> expectedMap = (Map<String, Object>) expected;
            @SuppressWarnings("unchecked")
            Map<String, Object> actualMap = (Map<String, Object>) actual;
            assertEquals(expectedMap.size(), actualMap.size());
            for (String key : expectedMap.keySet()) {
                assertValues(expectedMap.get(key), actualMap.get(key));
            }
        } else if (expected instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> expectedList = (List<Object>) expected;
            @SuppressWarnings("unchecked")
            List<Object> actualList = (List<Object>) actual;
            assertEquals(expectedList.size(), actualList.size());
            Iterator<Object> actualIterator = actualList.iterator();
            for (Object element : expectedList) {
                assertValues(element, actualIterator.next());
            }
        } else {
            assertEquals(expected, actual);
        }
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.contains(CommonFields.VALUE.getPreferredName());
    }

}
