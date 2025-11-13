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
package apoc.refactor;

import static apoc.util.MapUtil.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.algo.Cover;
import apoc.coll.Coll;
import apoc.map.Maps;
import apoc.meta.Meta;
import apoc.meta.MetaRestricted;
import apoc.path.PathExplorer;
import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension()
public class CloneSubgraphTest {
    private static final String STANDIN_SYNTAX_EXCEPTION_MSG = "Expected pair of nodes but got";

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setup() {
        TestUtil.registerProcedure(
                db,
                GraphRefactoring.class,
                Coll.class,
                PathExplorer.class,
                Cover.class,
                Meta.class,
                Maps.class,
                MetaRestricted.class);
    }

    @BeforeEach
    void beforeEach() {
        // tree structure, testing clone of branches and standins
        db.executeTransactionally("CREATE (rA:Root{name:'A'}), \n" + "(rB:Root{name:'B'}),\n"
                + "(n1:Node{name:'node1', id:1}),\n"
                + "(n2:Node{name:'node2', id:2}),\n"
                + "(n3:Node{name:'node3', id:3}),\n"
                + "(n4:Node{name:'node4', id:4}),\n"
                + "(n5:Node:Oddball{name:'node5', id:5}),\n"
                + "(n6:Node{name:'node6', id:6}),\n"
                + "(n7:Node{name:'node7', id:7}),\n"
                + "(n8:Node{name:'node8', id:8}),\n"
                + "(n9:Node{name:'node9', id:9}),\n"
                + "(n10:Node{name:'node10', id:10}),\n"
                + "(n11:Node{name:'node11', id:11}),\n"
                + "(n12:Node{name:'node12', id:12})\n"
                + // 12 on its own
                "CREATE (rA)-[:LINK{id:'rA->n1'}]->(n1)-[:LINK{id:'n1->n2'}]->(n2)-[:LINK{id:'n2->n3'}]->(n3)-[:LINK{id:'n3->n4'}]->(n4)\n"
                + "CREATE               (n1)-[:LINK{id:'n1->n5'}]->(n5)-[:LINK{id:'n5->n6'}]->(n6)<-[:LINK{id:'n6->n7'}]-(n7)\n"
                + "CREATE                             (n5)-[:LINK{id:'n5->n8'}]->(n8)\n"
                + "CREATE                             (n5)-[:LINK{id:'n5->n9'}]->(n9)-[:DIFFERENT_LINK{id:'n9->n10'}]->(n10)\n"
                + "CREATE (rB)-[:LINK{id:'rB->n11'}]->(n11)");
    }

    private static final String cloneWithEmptyRels =
            """
                MATCH (rootA:Root{name:'A'})
                CALL apoc.path.subgraphAll(rootA, {}) YIELD nodes, relationships
                WITH
                  nodes[1..] as nodes,
                  relationships,
                  [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames
                CALL apoc.refactor.cloneSubgraph(nodes, [], {createNodesInNewTransactions: $newTx}) YIELD input, output, error
                WITH
                  relNames,
                  collect(output) as clones,
                  collect(output.name) as cloneNames
                CALL apoc.algo.cover(clones) YIELD rel
                WITH
                  relNames,
                  cloneNames,
                  [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames
                WITH
                  cloneNames,
                  cloneRelNames,
                  apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified
                RETURN
                  cloneNames,
                  cloneRelNames,
                  clonedRelsVerified
                """;

    private static final String cloneWithEmptyRelsMeta =
            """
            CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap
            CALL db.relationshipTypes() YIELD relationshipType
            WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl
            RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount
            """;

    private void cloneWithEmptyRelsTest(boolean newTx, boolean commit) {
        try (final var tx = db.beginTx();
                final var res = tx.execute(cloneWithEmptyRels, Map.of("newTx", newTx))) {
            assertThat(res.stream())
                    .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                    .hasEntrySatisfying("cloneNames", n -> assertThat(n)
                            .asInstanceOf(list(String.class))
                            .containsExactlyInAnyOrder(
                                    "node1", "node2", "node3", "node4", "node5", "node8", "node9", "node10", "node6",
                                    "node7"))
                    .hasEntrySatisfying("cloneRelNames", n -> assertThat(n)
                            .asInstanceOf(list(String.class))
                            .containsExactlyInAnyOrder(
                                    "node1 LINK node5",
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node7 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9",
                                    "node9 DIFFERENT_LINK node10"))
                    .containsEntry("clonedRelsVerified", true);

            if (commit) tx.commit();
            else tx.rollback();
        }
    }

    @Test
    void testCloneSubgraphFromRootAWithEmptyRelsShouldCloneAllRelationshipsBetweenNodesNewTx() {
        cloneWithEmptyRelsTest(true, true);
        try (final var tx = db.beginTx();
                final var res = tx.execute(cloneWithEmptyRelsMeta)) {
            assertThat(res.stream())
                    .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsEntry("nodeCount", 24L)
                    .containsEntry("relCount", 20L)
                    .containsEntry("labels", map("Root", 2L, "Oddball", 2L, "Node", 22L))
                    .containsEntry("relTypesCount", map("LINK", 18L, "DIFFERENT_LINK", 2L));
        }
    }

    @Test
    void testCloneSubgraphFromRootAWithEmptyRelsShouldCloneAllRelationshipsBetweenNodesNewTxRollback() {
        cloneWithEmptyRelsTest(true, false);
        try (final var tx = db.beginTx();
                final var res = tx.execute(cloneWithEmptyRelsMeta)) {
            assertThat(res.stream())
                    .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsEntry("nodeCount", 24L) // Committed in inner tx
                    .containsEntry("relCount", 11L) // Rolled back, not created (weird, but behaves as before)
                    .containsEntry("labels", map("Root", 2L, "Oddball", 2L, "Node", 22L))
                    .containsEntry("relTypesCount", map("LINK", 10L, "DIFFERENT_LINK", 1L));
        }
    }

    @Test
    void testCloneSubgraphFromRootAWithEmptyRelsShouldCloneAllRelationshipsBetweenNodes() {
        cloneWithEmptyRelsTest(false, true);
        try (final var tx = db.beginTx();
                final var res = tx.execute(cloneWithEmptyRelsMeta)) {
            assertThat(res.stream())
                    .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsEntry("nodeCount", 24L)
                    .containsEntry("relCount", 20L)
                    .containsEntry("labels", map("Root", 2L, "Oddball", 2L, "Node", 22L))
                    .containsEntry("relTypesCount", map("LINK", 18L, "DIFFERENT_LINK", 2L));
        }
    }

    @Test
    void testCloneSubgraphFromRootAWithEmptyRelsShouldCloneAllRelationshipsBetweenNodesRollback() {
        cloneWithEmptyRelsTest(false, false);
        try (final var tx = db.beginTx();
                final var res = tx.execute(cloneWithEmptyRelsMeta)) {
            assertThat(res.stream())
                    .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                    .containsEntry("nodeCount", 14L) // Rolled back
                    .containsEntry("relCount", 11L) // Rolled back
                    .containsEntry("labels", map("Root", 2L, "Oddball", 1L, "Node", 12L))
                    .containsEntry("relTypesCount", map("LINK", 10L, "DIFFERENT_LINK", 1L));
        }
    }

    @Test
    void testCloneSubgraphFromRootAWithoutRelsShouldCloneAllRelationshipsBetweenNodes() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'})" + "CALL apoc.path.subgraphAll(rootA, {}) YIELD nodes, relationships "
                        + "WITH nodes[1..] as nodes, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph(nodes) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + // was seeing odd incorrect behavior with yielded relTypesCount from apoc.meta.stats()
                        "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(List.of(
                                    "node1", "node2", "node3", "node4", "node5", "node8", "node9", "node10", "node6",
                                    "node7")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node1 LINK node5",
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node7 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9",
                                    "node9 DIFFERENT_LINK node10")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 24L); // original was 14, 10 nodes cloned
                    assertEquals(row.get("relCount"), 20L); // original was 11, 9 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 22L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 18L, "DIFFERENT_LINK", 2L));
                });
    }

    @Test
    void testCloneSubgraphFromRootAShouldOnlyIncludeRelsBetweenClones() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'})" + "CALL apoc.path.subgraphAll(rootA, {}) YIELD nodes, relationships "
                        + "WITH nodes[1..] as nodes, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph(nodes, relationships) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + // was seeing odd incorrect behavior with yielded relTypesCount from apoc.meta.stats()
                        "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(List.of(
                                    "node1", "node2", "node3", "node4", "node5", "node8", "node9", "node10", "node6",
                                    "node7")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node1 LINK node5",
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node7 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9",
                                    "node9 DIFFERENT_LINK node10")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 24L); // original was 14, 10 nodes cloned
                    assertEquals(row.get("relCount"), 20L); // original was 11, 9 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 22L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 18L, "DIFFERENT_LINK", 2L));
                });
    }

    @Test
    void testCloneSubgraphWithStandinsForRootAShouldHaveRootB() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'}), (rootB:Root{name:'B'}) "
                        + "CALL apoc.path.subgraphAll(rootA, {}) YIELD nodes, relationships "
                        + "WITH rootA, rootB, nodes, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph(nodes, relationships, {standinNodes:[[rootA, rootB]]}) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(List.of(
                                    "node1", "node2", "node3", "node4", "node5", "node8", "node9", "node10", "node6",
                                    "node7")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node1 LINK node5",
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node7 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9",
                                    "node9 DIFFERENT_LINK node10")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "MATCH (:Root{name:'B'})-[:LINK]->(node:Node) " + "RETURN collect(node.name) as bLinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("bLinkedNodeNames")).containsAll(List.of("node1", "node11")));
                    assertEquals(((List<String>) row.get("bLinkedNodeNames")).size(), 2);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 24L); // original was 14, 10 nodes cloned
                    assertEquals(row.get("relCount"), 21L); // original was 11, 10 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 22L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 19L, "DIFFERENT_LINK", 2L));
                });
    }

    @Test
    void testCloneSubgraphWithStandinsForNode1ShouldHaveRootB() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'})--(node1), (rootB:Root{name:'B'}) "
                        + "CALL apoc.path.subgraphAll(node1, {denylistNodes:[rootA]}) YIELD nodes, relationships "
                        + "WITH node1, rootB, nodes, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph(nodes, relationships, {standinNodes:[[node1, rootB]]}) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(List.of(
                                    "node2", "node3", "node4", "node5", "node8", "node9", "node10", "node6", "node7")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node7 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9",
                                    "node9 DIFFERENT_LINK node10")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "MATCH (:Root{name:'B'})-[:LINK]->(node:Node) " + "RETURN collect(node.name) as bLinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("bLinkedNodeNames"))
                            .containsAll(List.of("node5", "node2", "node11")));
                    assertEquals(((List<String>) row.get("bLinkedNodeNames")).size(), 3);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 23L); // original was 14, 9 nodes cloned
                    assertEquals(row.get("relCount"), 20L); // original was 11, 9 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 21L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 18L, "DIFFERENT_LINK", 2L));
                });
    }

    @Test
    void testCloneSubgraphWithStandinsForRootAWithSkippedPropertiesShouldNotIncludeSkippedProperties() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'}), (rootB:Root{name:'B'}) "
                        + "CALL apoc.path.subgraphAll(rootA, {}) YIELD nodes, relationships "
                        + "WITH rootA, rootB, nodes, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph(nodes, relationships, {standinNodes:[[rootA, rootB]], skipProperties:['id']}) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(List.of(
                                    "node1", "node2", "node3", "node4", "node5", "node8", "node9", "node10", "node6",
                                    "node7")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node1 LINK node5",
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node7 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9",
                                    "node9 DIFFERENT_LINK node10")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "MATCH (:Root{name:'B'})-[:LINK]->(node:Node) " + "RETURN collect(node.name) as bLinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("bLinkedNodeNames")).containsAll(List.of("node1", "node11")));
                    assertEquals(((List<String>) row.get("bLinkedNodeNames")).size(), 2);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 24L); // original was 14, 10 nodes cloned
                    assertEquals(row.get("relCount"), 21L); // original was 11, 10 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 22L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 19L, "DIFFERENT_LINK", 2L));
                });

        TestUtil.testCall(
                db, "MATCH (node:Node) " + "WHERE 0 < node.id < 15 " + "RETURN count(node) as nodesWithId", (row) -> {
                    assertEquals(row.get("nodesWithId"), 12L); // 12 original nodes + 10 clones
                });

        TestUtil.testCall(
                db, "MATCH ()-[r]->() " + "WHERE r.id IS NULL " + "RETURN count(r) as relsWithNoId", (row) -> {
                    assertEquals(row.get("relsWithNoId"), 10L); // 10 cloned rels with skipped id property
                });
    }

    @Test
    void testCloneSubgraphWithPropertiesOnRelationshipsPreserved() {
        try (final var tx = db.beginTx();
                final var clean = tx.execute("MATCH (n) DETACH DELETE n");
                final var create = tx.execute("CREATE (:A)-[:R{id:\"ID1\"}]->(:B)");
                final var clone = tx.execute(
                        """
                MATCH (n)-[r:R]->(oldB:B)
                WITH oldB, COLLECT(DISTINCT n) AS nodes
                CREATE (newB:B)

                WITH oldB, newB, nodes
                MATCH (m)-[r]-(n) WHERE n IN nodes

                WITH oldB, newB, nodes, COLLECT(DISTINCT r) AS rels
                MATCH (s) WHERE NOT s:B

                WITH oldB, newB, nodes, rels, COLLECT(DISTINCT [s, s]) + [[oldB, newB]] AS standinNodes
                CALL apoc.refactor.cloneSubgraph(nodes, rels, {standinNodes:standinNodes})
                YIELD input, output
                MATCH (old) WHERE ID(old) = input
                CREATE (output)-[r:importedFrom{created_on:datetime()}]->(old)

                RETURN output
                """);
                final var res = tx.execute("MATCH (n)-[r]->(m) RETURN r.id AS id"); ) {
            assertThat(res.stream()).containsExactlyInAnyOrder(Map.of("id", "ID1"), Map.of("id", "ID1"));
        }
    }

    @Test
    void testCloneSubgraphWithStandinsForRootAAndOddballShouldHaveRootBAndUseNode12InPlaceOfOddball() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'}), (rootB:Root{name:'B'}), (node12:Node{name:'node12'}), (oddball:Oddball) "
                        + "CALL apoc.path.subgraphAll(rootA, {}) YIELD nodes, relationships "
                        + "WITH rootA, rootB, node12, oddball, nodes, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph(nodes, relationships, {standinNodes:[[rootA, rootB], [oddball, node12]]}) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(List.of(
                                    "node1", "node2", "node3", "node4", "node8", "node9", "node10", "node6", "node7")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node7 LINK node6",
                                    "node9 DIFFERENT_LINK node10")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "MATCH (:Root{name:'B'})-[:LINK]->(node:Node) " + "RETURN collect(node.name) as bLinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("bLinkedNodeNames")).containsAll(List.of("node1", "node11")));
                    assertEquals(((List<String>) row.get("bLinkedNodeNames")).size(), 2);
                });

        TestUtil.testCall(
                db,
                "MATCH (node12:Node{name:'node12'})-[:LINK]-(node:Node) "
                        + "RETURN collect(node.name) as node12LinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("node12LinkedNodeNames"))
                            .containsAll(List.of("node1", "node9", "node8", "node6")));
                    assertEquals(((List<String>) row.get("node12LinkedNodeNames")).size(), 4);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 23L); // original was 14, 9 nodes cloned
                    assertEquals(row.get("relCount"), 21L); // original was 11, 10 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 1L, "Node", 21L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 19L, "DIFFERENT_LINK", 2L));
                });
    }

    @Test
    void testCloneSubgraphWithRelsNotBetweenProvidedNodesOrStandinsShouldBeIgnored() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'}), (rootB:Root{name:'B'}) "
                        + "CALL apoc.path.subgraphAll(rootA, {relationshipFilter:'LINK>'}) YIELD nodes "
                        + "WITH rootA, rootB, nodes, [(:Node{name:'node7'})-[r]->() | r] + [(:Node{name:'node9'})-[r:DIFFERENT_LINK]->() | r] as relationships "
                        + // just an opposite-direction :LINK and the :DIFFERENT_LINK rels
                        "CALL apoc.refactor.cloneSubgraph(nodes, relationships, {standinNodes:[[rootA, rootB]]}) YIELD input, output, error "
                        + "RETURN collect(output.name) as cloneNames",
                (row) -> assertTrue(((List<String>) row.get("cloneNames"))
                        .containsAll(List.of("node1", "node2", "node3", "node4", "node5", "node8", "node9", "node6"))));

        TestUtil.testCall(
                db,
                "MATCH (:Root{name:'B'})-[:LINK]->(node:Node) " + "RETURN collect(node.name) as bLinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("bLinkedNodeNames")).contains("node11"));
                    assertEquals(((List<String>) row.get("bLinkedNodeNames")).size(), 1);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 22L); // original was 14, 8 nodes cloned
                    assertEquals(row.get("relCount"), 11L); // original was 11, 0 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 20L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 10L, "DIFFERENT_LINK", 1L));
                });
    }

    @Test
    void testCloneSubgraphWithNoNodesButWithRelsAndStandinsShouldDoNothing() {
        TestUtil.testCallEmpty(
                db,
                "MATCH (rootA:Root{name:'A'}), (rootB:Root{name:'B'}) "
                        + "CALL apoc.path.subgraphAll(rootA, {}) YIELD relationships "
                        + "WITH rootA, rootB, relationships, [rel in relationships | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraph([], relationships, {standinNodes:[[rootA, rootB]]}) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                Collections.emptyMap());
    }

    @Test
    void testCloneSubgraphWithA1ElementStandinPairShouldThrowException() {
        var e = assertThrows(
                QueryExecutionException.class,
                () -> TestUtil.testCall(
                        db,
                        "MATCH (root:Root{name:'A'})-[*]-(node) " + "WITH root, collect(DISTINCT node) as nodes "
                                + "CALL apoc.refactor.cloneSubgraph(nodes, [], {standinNodes:[[root]]}) YIELD input, output, error "
                                + "WITH collect(output) as clones, collect(output.name) as cloneNames "
                                + "RETURN cloneNames, size(cloneNames) as cloneCount, none(clone in clones WHERE (clone)--()) as noRelationshipsOnClones, "
                                + " single(clone in clones WHERE clone.name = 'node5' AND clone:Oddball) as oddballNode5Exists, "
                                + " size([clone in clones WHERE clone:Node]) as nodesWithNodeLabel",
                        (row) -> {}));
        assertTrue(e.getMessage().contains(STANDIN_SYNTAX_EXCEPTION_MSG));
    }

    @Test
    void testCloneSubgraphWithA3ElementStandinPairShouldThrowException() {
        var e = assertThrows(
                QueryExecutionException.class,
                () -> TestUtil.testCall(
                        db,
                        "MATCH (root:Root{name:'A'})-[*]-(node) " + "WITH root, collect(DISTINCT node) as nodes "
                                + "CALL apoc.refactor.cloneSubgraph(nodes, [], {standinNodes:[[root, root, root]]}) YIELD input, output, error "
                                + "WITH collect(output) as clones, collect(output.name) as cloneNames "
                                + "RETURN cloneNames, size(cloneNames) as cloneCount, none(clone in clones WHERE (clone)--()) as noRelationshipsOnClones, "
                                + " single(clone in clones WHERE clone.name = 'node5' AND clone:Oddball) as oddballNode5Exists, "
                                + " size([clone in clones WHERE clone:Node]) as nodesWithNodeLabel",
                        (row) -> {}));
        assertTrue(e.getMessage().contains(STANDIN_SYNTAX_EXCEPTION_MSG));
    }

    @Test
    void testCloneSubgraphWithANullElementInStandinPairShouldThrowException() {
        var e = assertThrows(
                QueryExecutionException.class,
                () -> TestUtil.testCall(
                        db,
                        "MATCH (root:Root{name:'A'})-[*]-(node) " + "WITH root, collect(DISTINCT node) as nodes "
                                + "CALL apoc.refactor.cloneSubgraph(nodes, [], {standinNodes:[[root, null]]}) YIELD input, output, error "
                                + "WITH collect(output) as clones, collect(output.name) as cloneNames "
                                + "RETURN cloneNames, size(cloneNames) as cloneCount, none(clone in clones WHERE (clone)--()) as noRelationshipsOnClones, "
                                + " single(clone in clones WHERE clone.name = 'node5' AND clone:Oddball) as oddballNode5Exists, "
                                + " size([clone in clones WHERE clone:Node]) as nodesWithNodeLabel",
                        (row) -> {}));
        assertTrue(e.getMessage().contains(STANDIN_SYNTAX_EXCEPTION_MSG));
    }

    @Test
    void testCloneSubgraphFromPathsWithStandinsForRootAShouldHaveRootB() {
        TestUtil.testCall(
                db,
                "MATCH (rootA:Root{name:'A'}), (rootB:Root{name:'B'}) "
                        + "CALL apoc.path.spanningTree(rootA, {relationshipFilter:'LINK>'}) YIELD path "
                        + "WITH rootA, rootB, collect(path) as paths "
                        + "WITH rootA, rootB, paths, apoc.coll.toSet(apoc.coll.flatten([path in paths | relationships(path)])) as rels "
                        + "WITH rootA, rootB, paths, [rel in rels | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as relNames "
                        + "CALL apoc.refactor.cloneSubgraphFromPaths(paths, {standinNodes:[[rootA, rootB]]}) YIELD input, output, error "
                        + "WITH relNames, collect(output) as clones, collect(output.name) as cloneNames "
                        + "CALL apoc.algo.cover(clones) YIELD rel "
                        + "WITH relNames, cloneNames, [rel in collect(rel) | startNode(rel).name + ' ' + type(rel) + ' ' + endNode(rel).name] as cloneRelNames "
                        + "WITH cloneNames, cloneRelNames, apoc.coll.containsAll(relNames, cloneRelNames) as clonedRelsVerified "
                        + "RETURN cloneNames, cloneRelNames, clonedRelsVerified",
                (row) -> {
                    assertTrue(((List<String>) row.get("cloneNames"))
                            .containsAll(
                                    List.of("node1", "node2", "node3", "node4", "node5", "node8", "node9", "node6")));
                    assertTrue(((List<String>) row.get("cloneRelNames"))
                            .containsAll(List.of(
                                    "node1 LINK node5",
                                    "node1 LINK node2",
                                    "node2 LINK node3",
                                    "node3 LINK node4",
                                    "node5 LINK node6",
                                    "node5 LINK node8",
                                    "node5 LINK node9")));
                    assertTrue((Boolean) row.get("clonedRelsVerified"));
                });

        TestUtil.testCall(
                db,
                "MATCH (:Root{name:'B'})-[:LINK]->(node:Node) " + "RETURN collect(node.name) as bLinkedNodeNames",
                (row) -> {
                    assertTrue(((List<String>) row.get("bLinkedNodeNames")).containsAll(List.of("node1", "node11")));
                    assertEquals(((List<String>) row.get("bLinkedNodeNames")).size(), 2);
                });

        TestUtil.testCall(
                db,
                "CALL apoc.meta.stats() YIELD nodeCount, relCount, labels, relTypes as relTypesMap "
                        + "CALL db.relationshipTypes() YIELD relationshipType "
                        + "WITH nodeCount, relCount, labels, collect([relationshipType, relTypesMap['()-[:' + relationshipType + ']->()']]) as relationshipTypesColl "
                        + "RETURN nodeCount, relCount, labels, apoc.map.fromPairs(relationshipTypesColl) as relTypesCount ",
                (row) -> {
                    assertEquals(row.get("nodeCount"), 22L); // original was 14, 10 nodes cloned
                    assertEquals(row.get("relCount"), 19L); // original was 11, 8 relationships cloned
                    assertEquals(row.get("labels"), map("Root", 2L, "Oddball", 2L, "Node", 20L));
                    assertEquals(row.get("relTypesCount"), map("LINK", 18L, "DIFFERENT_LINK", 1L));
                });
    }
}
