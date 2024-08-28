/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.agg;

import apoc.util.Util;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.procedure.*;

/**
 * Aggregation functions for collecting items with only the minimal or maximal values.
 * This is meant to replace queries like this:
 *
 * <pre>
 * MATCH (p:Person)
 * WHERE p.born &gt;= 1930
 * WITH p.born as born, collect(p.name) as persons
 * WITH min(born) as minBorn, collect({born:born, persons:persons}) as bornInfoList
 * UNWIND [info in bornInfoList WHERE info.born = minBorn] as bornInfo
 * RETURN bornInfo.born as born, [person in bornInfo.persons | person.name] as persons
 * </pre>
 *
 * with an aggregation like this:
 *
 * <pre>
 * MATCH (p:Person)
 * WHERE p.born &gt;= 1930
 * WITH apoc.agg.minItems(p, p.born) as minResult
 * RETURN minResult.value as born, [person in minResult.items | person.name] as persons
 * </pre>
 *
 * returns {born:1930, persons:["Gene Hackman", "Richard Harris", "Clint Eastwood"]}
 *
 */
public class MaxAndMinItems {

    @UserAggregationFunction("apoc.agg.maxItems")
    @Description(
            "Returns a `MAP` `{items: LIST<ANY>, value: ANY}` where the `value` key is the maximum value present, and `items` represent all items with the same value. The size of the list of items can be limited to a given max size.")
    public MaxItemsFunction maxItems() {
        return new MaxItemsFunction();
    }

    @UserAggregationFunction("apoc.agg.minItems")
    @Description(
            "Returns a `MAP` `{items: LIST<ANY>, value: ANY}` where the `value` key is the minimum value present, and `items` represent all items with the same value. The size of the list of items can be limited to a given max size.")
    public MinItemsFunction minItems() {
        return new MinItemsFunction();
    }

    public static class MaxItemsFunction {
        private final List<Object> items = new ArrayList<>();
        private Comparable value;

        private MaxItemsFunction() {}

        @UserAggregationUpdate
        public void maxOrMinItems(
                @Name(value = "item", description = "A value to be aggregated.") final Object item,
                @Name(value = "value", description = "The value from which the max is selected.")
                        final Object inputValue,
                @Name(
                                value = "groupLimit",
                                defaultValue = "-1",
                                description = "The limit on the number of items returned.")
                        final Long groupLimitParam) {
            int groupLimit = groupLimitParam.intValue();
            boolean noGroupLimit = groupLimit < 0;

            if (item != null && inputValue != null) {
                int result = value == null ? -1 : value.compareTo(inputValue);
                if (result == 0) {
                    if (noGroupLimit || items.size() < groupLimit) {
                        items.add(item);
                    }
                } else if (result < 0) {
                    // xnor logic, interested value should replace current value
                    items.clear();
                    items.add(item);
                    value = (Comparable) inputValue;
                }
            }
        }

        @UserAggregationResult
        public Object result() {
            return Util.map("items", items, "value", value);
        }
    }

    public static class MinItemsFunction {
        private final List<Object> items = new ArrayList<>();
        private Comparable value;

        private MinItemsFunction() {}

        @UserAggregationUpdate
        public void maxOrMinItems(
                @Name(value = "item", description = "A value to be aggregated.") final Object item,
                @Name(value = "value", description = "The value from which the min is selected.")
                        final Object inputValue,
                @Name(
                                value = "groupLimit",
                                defaultValue = "-1",
                                description = "The limit on the number of items returned.")
                        final Long groupLimitParam) {
            int groupLimit = groupLimitParam.intValue();
            boolean noGroupLimit = groupLimit < 0;

            if (item != null && inputValue != null) {
                int result = value == null ? 1 : value.compareTo(inputValue);
                if (result == 0) {
                    if (noGroupLimit || items.size() < groupLimit) {
                        items.add(item);
                    }
                } else if (result >= 0) {
                    // xnor logic, interested value should replace current value
                    items.clear();
                    items.add(item);
                    value = (Comparable) inputValue;
                }
            }
        }

        @UserAggregationResult
        public Object result() {
            return Util.map("items", items, "value", value);
        }
    }
}
