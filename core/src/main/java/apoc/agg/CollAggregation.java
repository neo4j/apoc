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

import java.util.ArrayList;
import java.util.List;
import org.neo4j.procedure.*;

/**
 * @author mh
 * @since 18.12.17
 */
public class CollAggregation {
    @UserAggregationFunction("apoc.agg.nth")
    @Description(
            "Returns the nth value in the given collection (to fetch the last item of an unknown length collection, -1 can be used).")
    public NthFunction nthFunction() {
        return new NthFunction();
    }

    @UserAggregationFunction("apoc.agg.first")
    @Description("Returns the first value from the given collection.")
    public FirstFunction first() {
        return new FirstFunction();
    }

    @UserAggregationFunction("apoc.agg.last")
    @Description("Returns the last value from the given collection.")
    public LastFunction last() {
        return new LastFunction();
    }

    @UserAggregationFunction("apoc.agg.slice")
    @Description(
            "Returns a subset of non-null values from the given collection (the collection is considered to be zero-indexed).\n"
                    + "To specify the range from start until the end of the collection, the length should be set to -1.")
    public SliceFunction slice() {
        return new SliceFunction();
    }

    public static class NthFunction {

        private Object value;
        private int index;

        @UserAggregationUpdate
        public void nth(
                @Name(value = "value", description = "A value to be aggregated.") Object value,
                @Name(
                                value = "offset",
                                description = "The index of the value to be returned, or -1 to return the last item.")
                        long target) {
            if (value != null) {
                if (target == index++ || target == -1) {
                    this.value = value;
                }
            }
        }

        @UserAggregationResult
        public Object result() {
            return value;
        }
    }

    public static class SliceFunction {

        private List<Object> values = new ArrayList<>();
        private int index;

        @UserAggregationUpdate
        public void nth(
                @Name(value = "value", description = "A value to be multiplied in the aggregate.") Object value,
                @Name(
                                value = "from",
                                defaultValue = "0",
                                description = "The index from which to start returning values in the specified range.")
                        long from,
                @Name(
                                value = "to",
                                defaultValue = "-1",
                                description = "The non-inclusive index of the final value in the range.")
                        long len) {
            if (value != null) {
                if (index >= from && (len == -1 || index < from + len)) {
                    this.values.add(value);
                }
                index++;
            }
        }

        @UserAggregationResult
        public List<Object> result() {
            return values;
        }
    }

    public static class FirstFunction {
        private Object value;

        @UserAggregationUpdate
        public void first(@Name(value = "value", description = "A value to be aggregated.") Object value) {
            if (value != null && this.value == null) {
                this.value = value;
            }
        }

        @UserAggregationResult
        public Object result() {
            return value;
        }
    }

    public static class LastFunction {
        private Object value;

        @UserAggregationUpdate
        public void last(@Name(value = "value", description = "A value to be aggregated.") Object value) {
            if (value != null) {
                this.value = value;
            }
        }

        @UserAggregationResult
        public Object result() {
            return value;
        }
    }
}
