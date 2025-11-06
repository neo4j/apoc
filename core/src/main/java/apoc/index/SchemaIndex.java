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
package apoc.index;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndex {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Transaction tx;

    public record SchemaListResult(
            @Description("The list of distinct values for the given property.") List<Object> value) {}

    @NotThreadSafe
    @Procedure("apoc.schema.properties.distinct")
    @Description("Returns all distinct `NODE` property values for the given key.")
    public Stream<SchemaListResult> distinct(
            @Name(value = "label", description = "The node label to find distinct properties on.") String label,
            @Name(value = "key", description = "The name of the property to find distinct values of.") String key) {
        List<Object> values = distinctCount(label, key)
                .map(propertyValueCount -> propertyValueCount.value)
                .collect(Collectors.toList());
        return Stream.of(new SchemaListResult(values));
    }

    @NotThreadSafe
    @Procedure("apoc.schema.properties.distinctCount")
    @Description("Returns all distinct property values and counts for the given key.")
    public Stream<PropertyValueCount> distinctCount(
            @Name(value = "label", defaultValue = "", description = "The node label to count distinct properties on.")
                    String labelName,
            @Name(
                            value = "key",
                            defaultValue = "",
                            description = "The name of the property to count distinct values of.")
                    String keyName) {
        String query;

        if (labelName.isEmpty() && keyName.isEmpty()) {
            query =
                    """
                    WITH COLLECT { CALL db.labels() YIELD label RETURN label } AS labels,
                      COLLECT { CALL db.propertyKeys() YIELD propertyKey AS propertyKey } AS keys
                    UNWIND labels AS label
                    UNWIND keys AS key
                    WITH DISTINCT label AS uniqueLabel, key AS uniqueKey
                    MATCH (n:$(uniqueLabel))
                    WHERE n[uniqueKey] IS NOT NULL
                    RETURN uniqueLabel AS label, uniqueKey AS key, n[uniqueKey] AS value, count(n[uniqueKey]) AS count
                    """;
        } else if (labelName.isEmpty()) {
            query =
                    """
                    WITH COLLECT { CALL db.labels() YIELD label RETURN label } AS labels
                    UNWIND labels AS label
                    WITH DISTINCT label AS uniqueLabel
                    MATCH (n:$(uniqueLabel))
                    WHERE n[$key] IS NOT NULL
                    RETURN uniqueLabel AS label, $key AS key, n[$key] AS value, count(n[$key]) AS count
                    """;
        } else if (keyName.isEmpty()) {
            query =
                    """
                    WITH COLLECT { CALL db.propertyKeys() YIELD propertyKey AS propertyKey } AS keys
                    UNWIND keys AS key
                    WITH DISTINCT key AS uniqueKey
                    MATCH (n:$($label))
                    WHERE n[uniqueKey] IS NOT NULL
                    RETURN $label AS label, uniqueKey AS key, n[uniqueKey] AS value, count(n[uniqueKey]) AS count
                    """;
        } else {
            query =
                    """
                    MATCH (n :$($label))
                    WHERE n[$key] IS NOT NULL
                    RETURN $label AS label, $key AS key, n[$key] AS value, count(n[$key]) AS count
                    """;
        }

        return tx
                .execute(query,  Map.of("label", labelName, "key", keyName))
                .map(row -> new PropertyValueCount(
                        (String) row.get("label"), (String) row.get("key"), row.get("value"), (long) row.get("count")))
                .stream();
    }

    public static class PropertyValueCount {
        @Description("The label of the node.")
        public String label;

        @Description("The name of the property key.")
        public String key;

        @Description("The distinct value.")
        public Object value;

        @Description("The number of occurrences of the value.")
        public long count;

        public PropertyValueCount(String label, String key, Object value, long count) {
            this.label = label;
            this.key = key;
            this.value = value;
            this.count = count;
        }

        @Override
        public String toString() {
            return "PropertyValueCount{" + "label='"
                    + label + '\'' + ", key='"
                    + key + '\'' + ", value='"
                    + value + '\'' + ", count="
                    + count + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PropertyValueCount that = (PropertyValueCount) o;

            return count == that.count
                    && Objects.equals(label, that.label)
                    && Objects.equals(key, that.key)
                    && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, key, value, count);
        }
    }
}
