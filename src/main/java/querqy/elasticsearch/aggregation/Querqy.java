package querqy.elasticsearch.aggregation;

import org.elasticsearch.search.aggregations.Aggregation;

/**
 * A {@code Querqy} aggregation. Defines a single bucket the holds all the querqy info in the search context.
 */
public interface Querqy extends Aggregation {

    /**
     * The result of the aggregation. The type of the object depends on the aggregation that was run.
     */
    Object aggregation();

}
