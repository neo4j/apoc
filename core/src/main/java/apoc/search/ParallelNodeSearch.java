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
package apoc.search;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.groupingBy;

import apoc.util.Util;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;

public class ParallelNodeSearch {

    private static final Set<String> OPERATORS = new HashSet<>(
            asList("exact", "starts with", "ends with", "contains", "<", ">", "=", "<>", "<=", ">=", "=~"));

    @Context
    public GraphDatabaseService api;

    @Context
    public Log log;

    @Context
    public Transaction tx;

    @NotThreadSafe
    @Procedure("apoc.search.nodeAllReduced")
    @Description(
            "Returns a reduced representation of the `NODE` values found after a parallel search over multiple indexes.\n"
                    + "The reduced `NODE` values representation includes: node id, node labels, and the searched properties.")
    public Stream<NodeReducedResult> multiSearchAll(
            @Name(
                            value = "labelPropertyMap",
                            description =
                                    "A map that pairs labels with lists of properties. This can also be represented as a JSON string.")
                    final Object labelProperties,
            @Name(
                            value = "operator",
                            description =
                                    "The search operator, can be one of: [\"exact\", \"starts with\", \"ends with\", \"contains\", \"<\", \">\", \"=\", \"<>\", \"<=\", \">=\", \"=~\"].")
                    final String operator,
            @Name(value = "value", description = "The search value.") final Object value)
            throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value).flatMap(QueryWorker::queryForData);
    }

    private NodeReducedResult merge(NodeReducedResult a, NodeReducedResult b) {
        a.values.putAll(b.values);
        for (String label : b.labels) if (!a.labels.contains(label)) a.labels.add(label);
        return a;
    }

    @NotThreadSafe
    @Procedure("apoc.search.nodeReduced")
    @Description(
            "Returns a reduced representation of the distinct `NODE` values found after a parallel search over multiple indexes.\n"
                    + "The reduced `NODE` values representation includes: node id, node labels, and the searched properties.")
    public Stream<NodeReducedResult> multiSearch(
            @Name(
                            value = "labelPropertyMap",
                            description =
                                    "A map that pairs labels with lists of properties. This can also be represented as a JSON string.")
                    final Object labelProperties,
            @Name(
                            value = "operator",
                            description =
                                    "The search operator, can be one of: [\"exact\", \"starts with\", \"ends with\", \"contains\", \"<\", \">\", \"=\", \"<>\", \"<=\", \">=\", \"=~\"].")
                    final String operator,
            @Name(value = "value", description = "The search value.") final String value)
            throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value)
                .flatMap(QueryWorker::queryForData)
                .collect(groupingBy(res -> res.id, Collectors.reducing(this::merge)))
                .values()
                .stream()
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    @NotThreadSafe
    @Procedure("apoc.search.multiSearchReduced")
    @Description(
            "Returns a reduced representation of the `NODE` values found after a parallel search over multiple indexes.\n"
                    + "The reduced `NODE` values representation includes: node id, node labels, and the searched properties.")
    public Stream<NodeReducedResult> multiSearchOld(
            @Name(
                            value = "labelPropertyMap",
                            description =
                                    "A map that pairs labels with lists of properties. This can also be represented as a JSON string.")
                    final Object labelProperties,
            @Name(
                            value = "operator",
                            description =
                                    "The search operator, can be one of: [\"exact\", \"starts with\", \"ends with\", \"contains\", \"<\", \">\", \"=\", \"<>\", \"<=\", \">=\", \"=~\"].")
                    final String operator,
            @Name(value = "value", description = "The search value.") final String value)
            throws Exception {
        return createWorkersFromValidInput(labelProperties, operator, value)
                .flatMap(QueryWorker::queryForData)
                .collect(groupingBy(res -> res.id))
                .values()
                .stream()
                .map(list -> list.stream().reduce(this::merge))
                .filter(Optional::isPresent)
                .map(Optional::get);
    }

    @NotThreadSafe
    @Procedure("apoc.search.nodeAll")
    @Description("Returns all the `NODE` values found after a parallel search over multiple indexes.")
    public Stream<SearchNodeResult> multiSearchNodeAll(
            @Name(
                            value = "labelPropertyMap",
                            description =
                                    "A map that pairs labels with lists of properties. This can also be represented as a JSON string.")
                    final Object labelProperties,
            @Name(
                            value = "operator",
                            description =
                                    "The search operator, can be one of: [\"exact\", \"starts with\", \"ends with\", \"contains\", \"<\", \">\", \"=\", \"<>\", \"<=\", \">=\", \"=~\"].")
                    final String operator,
            @Name(value = "value", description = "The search value.") final String value)
            throws Exception {
        final var ids = createWorkersFromValidInput(labelProperties, operator, value)
                .flatMapToLong(w -> w.queryForNodeId().mapToLong(i -> i))
                .toArray();
        // It's not safe to access a transaction from multiple threads so this part needs to be sequential
        return Arrays.stream(ids).mapToObj(id -> new SearchNodeResult(tx.getNodeById(id)));
    }

    public record SearchNodeResult(@Description("The found node.") Node node) {}

    @NotThreadSafe
    @Procedure("apoc.search.node")
    @Description("Returns all the distinct `NODE` values found after a parallel search over multiple indexes.")
    public Stream<SearchNodeResult> multiSearchNode(
            @Name(
                            value = "labelPropertyMap",
                            description =
                                    "A map that pairs labels with lists of properties. This can also be represented as a JSON string.")
                    final Object labelProperties,
            @Name(
                            value = "operator",
                            description =
                                    "The search operator, can be one of: [\"exact\", \"starts with\", \"ends with\", \"contains\", \"<\", \">\", \"=\", \"<>\", \"<=\", \">=\", \"=~\"].")
                    final String operator,
            @Name(value = "value", description = "The search value.") final String value)
            throws Exception {
        final var ids = createWorkersFromValidInput(labelProperties, operator, value)
                .flatMapToLong(w -> w.queryForNodeId().mapToLong(i -> i))
                .toArray();
        // It's not safe to access a transaction from multiple threads so this part needs to be sequential
        return Arrays.stream(ids).boxed().distinct().map(id -> new SearchNodeResult(tx.getNodeById(id)));
    }

    private Stream<QueryWorker> createWorkersFromValidInput(
            final Object labelPropertiesInput, String operatorInput, final Object value) throws Exception {
        String operatorNormalized = operatorInput.trim().toLowerCase();
        if (operatorInput == null || !OPERATORS.contains(operatorNormalized)) {
            throw new Exception(format(
                    "operator `%s` invalid, it must have one of the following values (case insensitive): %s.",
                    operatorInput, OPERATORS));
        }
        String operator = operatorNormalized.equals("exact") ? "=" : operatorNormalized;

        if (labelPropertiesInput == null
                || labelPropertiesInput instanceof String
                        && labelPropertiesInput.toString().trim().isEmpty()) {
            throw new Exception(
                    "LabelProperties cannot be empty. example { Person: [\"fullName\",\"lastName\"],Company:\"name\", Event : \"Description\"}");
        }
        Map<String, Object> labelProperties = labelPropertiesInput instanceof Map
                ? (Map<String, Object>) labelPropertiesInput
                : Util.readMap(labelPropertiesInput.toString());

        return labelProperties.entrySet().parallelStream().flatMap(e -> {
            String label = e.getKey();
            Object properties = e.getValue();
            if (properties instanceof String) {
                return Stream.of(new QueryWorker(api, label, (String) properties, operator, value, log));
            } else if (properties instanceof List) {
                return ((List<String>) properties)
                        .stream().map(prop -> new QueryWorker(api, label, prop, operator, value, log));
            }
            throw new RuntimeException("Invalid type for properties " + properties + ": "
                    + (properties == null ? "null" : properties.getClass()));
        });
    }

    public static class QueryWorker {
        private GraphDatabaseService db;
        private String label, prop, operator;
        Object value;
        private Log log;

        public QueryWorker(GraphDatabaseService db, String label, String prop, String operator, Object value, Log log) {
            this.db = db;
            this.label = label;
            this.prop = prop;
            this.value = value;
            this.operator = operator;
            this.log = log;
        }

        public Stream<NodeReducedResult> queryForData() {
            List<String> labels = singletonList(label);
            String query = format(
                    "match (n:`%s`) where n.`%s` %s $value return id(n) as id,  n.`%s` as value",
                    label, prop, operator, prop);
            return queryForNode(
                    query,
                    (row) -> new NodeReducedResult((long) row.get("id"), labels, singletonMap(prop, row.get("value"))))
                    .stream();
        }

        public Stream<Long> queryForNodeId() {
            String query = format("match (n:`%s`) where n.`%s` %s $value return id(n) AS id", label, prop, operator);
            return queryForNode(query, (row) -> (long) row.get("id")).stream();
        }

        public <T> List<T> queryForNode(String query, Function<Map<String, Object>, T> transformer) {
            long start = currentTimeMillis();
            try (Transaction tx = db.beginTx()) {
                try (Result nodes = tx.execute(query, singletonMap("value", value))) {
                    return nodes.stream().map(transformer).collect(Collectors.toList());
                } finally {
                    tx.commit();
                    if (log.isDebugEnabled())
                        log.debug(format(
                                "(%s) search on label:%s and prop:%s took %d",
                                Thread.currentThread(), label, prop, currentTimeMillis() - start));
                }
            }
        }
    }

    public static class NodeReducedResult {
        @Description("The id of the found node.")
        public final long id;

        @Description("The labels of the found node.")
        public final List<String> labels;

        @Description("The matched values of the found node.")
        public final Map<String, Object> values;

        public NodeReducedResult(long id, List<String> labels, Map<String, Object> val) {
            this.labels = labels;
            this.id = id;
            this.values = val;
        }
    }
}
