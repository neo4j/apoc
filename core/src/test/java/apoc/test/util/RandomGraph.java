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
package apoc.test.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.eclipse.collections.api.bag.Bag;
import org.eclipse.collections.impl.factory.Bags;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.RandomSupport;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

public class RandomGraph {

    public static final List<ValueType> SUPPORTED_TYPES_CYPHER = Arrays.stream(ValueType.values())
            .filter(t -> switch (t) {
                case BYTE_ARRAY,
                        CHAR_ARRAY,
                        SHORT_ARRAY,
                        FLOAT_ARRAY,
                        FLOAT32VECTOR,
                        FLOAT64VECTOR,
                        INT8VECTOR,
                        INT16VECTOR,
                        INT32VECTOR,
                        INT64VECTOR,
                        FLOAT,
                        CHAR-> false;
                default -> true;
            })
            .toList();
    public static final Conf DEFAULT_CONF = new Conf(
            50, 0.25, List.of("A", "B", "C", "D", "E"), List.of("a", "b", "c", "d", "e"), SUPPORTED_TYPES_CYPHER);
    private final RandomSupport rand;
    private final Conf conf;

    public RandomGraph(RandomSupport rand, Conf conf) {
        this.rand = rand;
        this.conf = conf;
    }

    public void commitRandomGraph(GraphDatabaseService db) {
        final var nodeCount = rand.intBetween(1, conf.maxNodeCount);
        final var relationshipProb = rand.nextDouble() * conf.relationshipProbabilityMax;

        final var nodes = new ArrayList<Node>(nodeCount);
        try (final var tx = db.beginTx()) {
            for (int i = 0; i < nodeCount; ++i) {
                final var node = tx.createNode();
                setRandomProperties(node);
                final var labelCount = rand.intBetween(0, conf.allLabels.size() - 1);
                for (int j = 0; j < labelCount; ++j) {
                    node.addLabel(Label.label(rand.among(conf.allLabels)));
                }
                nodes.add(tx.createNode());
            }
            for (final var nodeA : nodes) {
                for (final var nodeB : nodes) {
                    if (rand.nextDouble() < relationshipProb) {
                        final var relCount = rand.intBetween(1, 5);
                        for (int i = 0; i < relCount; ++i) {
                            final var rel = nodeA.createRelationshipTo(
                                    nodeB, RelationshipType.withName(rand.among(conf.allLabels)));
                            setRandomProperties(rel);
                        }
                    }
                }
            }
            tx.commit();
        }
    }

    private void setRandomProperties(Entity entity) {
        final var propCount = rand.intBetween(0, conf.allPropertyKeys.size() - 1);
        for (int i = 0; i < propCount; ++i) {
            entity.setProperty(
                    rand.among(conf.allPropertyKeys),
                    rand.nextValue(rand.among(conf.supportedTypes)).asObject());
        }
    }

    public static void assertEqualGraph(GraphDatabaseService actual, GraphDatabaseService expected) {
        final var actualGraph = readGraph(actual);
        final var expectedGraph = readGraph(expected);
        if (!actualGraph.equals(expectedGraph)) {
            // Better failure messages
            assertThat(actualGraph.nodes()).containsExactlyInAnyOrderElementsOf(expectedGraph.nodes());
            assertThat(actualGraph.rels()).containsExactlyInAnyOrderElementsOf(expectedGraph.rels());
        }
        assertThat(actualGraph).isEqualTo(expectedGraph);
    }

    private static AssertGraph readGraph(GraphDatabaseService db) {
        try (final var tx = db.beginTx()) {
            final var nodes = Bags.mutable.<AssertNode>empty();
            final var rels = Bags.mutable.<AssertRel>empty();
            tx.getAllNodes().forEach(n -> nodes.add(readNode(n)));
            tx.getAllRelationships().forEach(r -> rels.add(readRel(r)));
            return new AssertGraph(nodes, rels);
        }
    }

    private static AssertNode readNode(Node node) {
        final var labels = StreamSupport.stream(node.getLabels().spliterator(), false)
                .map(Label::name)
                .collect(Collectors.toSet());
        final var degree = node.getDegree();
        final var degrees = Bags.mutable.<String>empty();
        node.getRelationships().forEach(rel -> degrees.add(rel.getType().name()));
        return new AssertNode(labels, props(node), degree, degrees);
    }

    private static AssertRel readRel(Relationship rel) {
        final var from = readNode(rel.getStartNode());
        final var to = readNode(rel.getEndNode());
        return new AssertRel(rel.getType().name(), props(rel), from, to);
    }

    private static Map<String, Value> props(Entity entity) {
        return entity.getAllProperties().entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> Values.of(e.getValue())));
    }

    record AssertNode(Set<String> labels, Map<String, Value> props, int degree, Bag<String> degrees) {
        @Override
        public String toString() {
            final var result = new StringBuilder();
            result.append("(:").append(String.join(":", labels)).append(" ");
            kindOfCypherMap(result, props);
            result.append(")");
            return result.toString();
        }
    }

    record AssertRel(String type, Map<String, Value> props, AssertNode from, AssertNode to) {
        @Override
        public String toString() {
            final var result = new StringBuilder();
            result.append(from);
            result.append("-[:").append(type).append(" ");
            kindOfCypherMap(result, props);
            result.append("]->");
            result.append(to);
            return result.toString();
        }
    }

    record AssertGraph(Bag<AssertNode> nodes, Bag<AssertRel> rels) {
        @Override
        public String toString() {
            final var result = new StringBuilder();
            result.append("Nodes:\n");
            nodes.forEach(n -> result.append(n).append("\n"));
            result.append("\n\n\nRelationships:\n");
            rels.forEach(r -> result.append(r).append("\n"));
            return result.toString();
        }
    }

    private static void kindOfCypherMap(StringBuilder builder, Map<String, Value> map) {
        builder.append("{");
        map.forEach(
                (key, value) -> builder.append(key).append(": ").append(value).append(", "));
        if (!map.isEmpty()) builder.setLength(builder.length() - 2);
        builder.append("}");
    }

    record Conf(
            int maxNodeCount,
            double relationshipProbabilityMax,
            List<String> allLabels,
            List<String> allPropertyKeys,
            List<ValueType> supportedTypes) {}
}
