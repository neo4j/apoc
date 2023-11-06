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
package apoc.nodes;

import static apoc.nodes.NodesConfig.MAX_DEPTH_KEY;
import static apoc.nodes.NodesConfig.REL_TYPES_KEY;
import static apoc.util.Util.map;
import static apoc.util.collection.Iterators.asSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

import apoc.create.Create;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 18.08.16
 */
public class NodesTest {

    private static final List<String> FIRST_ALPHA_CYCLE_PROPS = List.of("alpha", "one", "two", "alpha");
    private static final List<String> SECOND_ALPHA_CYCLE_PROPS = List.of("alpha", "seven", "eight", "alpha");
    private static final List<String> BETA_CYCLE_PROPS = List.of("beta", "three", "four", "beta");
    private static final List<String> SELF_REL_PROPS = List.of("delta", "delta");
    private static final List<String> ONE_STEP_PROPS = List.of("epsilon", "seven", "epsilon");
    private static final String DEPEND_ON_REL_TYPE = "DEPENDS_ON";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("apoc.*"));

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Nodes.class, Create.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void isDense() {
        db.executeTransactionally(
                "CREATE (f:Foo) CREATE (b:Bar) WITH f UNWIND range(1,100) as id CREATE (f)-[:SELF]->(f)");

        TestUtil.testCall(
                db,
                "MATCH (n) WITH n, apoc.nodes.isDense(n) as dense "
                        + "WHERE n:Foo AND dense OR n:Bar AND NOT dense RETURN count(*) as c",
                (row) -> assertEquals(2L, row.get("c")));
    }

    @Test
    public void nodesGetTest() {
        db.executeTransactionally("CREATE (f:Foo), (b:Bar), (c:Car)");
        TestUtil.testResult(
                db,
                """
                MATCH (f:Foo), (b:Bar), (c:Car)
                CALL apoc.nodes.get([elementId(f), id(b), c])
                YIELD node
                RETURN node
                """,
                (result) -> {
                    List<Label> returnedNodeLabels = new ArrayList<>();
                    List<Label> expectedLabels = List.of(label("Foo"), label("Bar"), label("Car"));
                    Map<String, Object> row = result.next();
                    ((Node) row.get("node")).getLabels().forEach(returnedNodeLabels::add);
                    row = result.next();
                    ((Node) row.get("node")).getLabels().forEach(returnedNodeLabels::add);
                    row = result.next();
                    ((Node) row.get("node")).getLabels().forEach(returnedNodeLabels::add);
                    assertFalse(result.hasNext());
                    assertTrue(expectedLabels.containsAll(returnedNodeLabels));
                });
    }

    @Test
    public void relsGetTest() {
        db.executeTransactionally("CREATE ()-[:FOO]->(), ()-[:BAR]->(), ()-[:CAR]->()");
        TestUtil.testResult(
                db,
                """
                MATCH ()-[f:FOO]->(), ()-[b:BAR]->(), ()-[c:CAR]->()
                CALL apoc.nodes.rels([elementId(f), id(b), c])
                YIELD rel
                RETURN rel
                """,
                (result) -> {
                    List<RelationshipType> returnedRelTypes = new ArrayList<>();
                    List<RelationshipType> expectedTypes = List.of(withName("FOO"), withName("BAR"), withName("CAR"));
                    Map<String, Object> row = result.next();
                    returnedRelTypes.add(((Relationship) row.get("rel")).getType());
                    row = result.next();
                    returnedRelTypes.add(((Relationship) row.get("rel")).getType());
                    row = result.next();
                    returnedRelTypes.add(((Relationship) row.get("rel")).getType());
                    assertFalse(result.hasNext());
                    assertTrue(expectedTypes.containsAll(returnedRelTypes));
                });
    }

    @Test
    public void cycles() {
        createDatasetForNodesCycles();

        // with all relationships
        TestUtil.testResult(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes) YIELD path RETURN path",
                res -> {
                    List<Path> paths = Iterators.stream(res.<Path>columnAs("path"))
                            .sorted(Comparator.comparingLong(
                                    item -> (long) item.lastRelationship().getProperty("id")))
                            .toList();
                    assertEquals(5, paths.size());
                    assertionsCycle(paths.get(0), FIRST_ALPHA_CYCLE_PROPS);
                    assertionsCycle(paths.get(1), SECOND_ALPHA_CYCLE_PROPS);
                    assertionsCycle(paths.get(2), BETA_CYCLE_PROPS);
                    assertionsCycle(paths.get(3), SELF_REL_PROPS);
                    assertionsCycle(paths.get(4), ONE_STEP_PROPS);
                });
    }

    @Test
    public void cyclesWithRelTypes() {
        createDatasetForNodesCycles();

        // with single specific relationship
        TestUtil.testResult(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes, $config) YIELD path RETURN path",
                map("config", map(REL_TYPES_KEY, List.of(DEPEND_ON_REL_TYPE))),
                res -> {
                    List<Path> paths = Iterators.stream(res.<Path>columnAs("path"))
                            .sorted(Comparator.comparingLong(
                                    item -> (long) item.lastRelationship().getProperty("id")))
                            .toList();
                    assertEquals(3, paths.size());
                    assertionsCycle(paths.get(0), FIRST_ALPHA_CYCLE_PROPS, DEPEND_ON_REL_TYPE);
                    assertionsCycle(paths.get(1), SECOND_ALPHA_CYCLE_PROPS, DEPEND_ON_REL_TYPE);
                    assertionsCycle(paths.get(2), SELF_REL_PROPS, DEPEND_ON_REL_TYPE);
                });

        // with multiple specific relationship (without MY_REL_ANOTHER, shouldn't find `(:Start {bar: 'beta'})` cycle)
        TestUtil.testResult(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes, $config) YIELD path RETURN path",
                map("config", map(REL_TYPES_KEY, List.of(DEPEND_ON_REL_TYPE, "MY_REL", "NOT_EXISTENT"))),
                res -> {
                    List<Path> paths = Iterators.stream(res.<Path>columnAs("path"))
                            .sorted(Comparator.comparingLong(
                                    item -> (long) item.lastRelationship().getProperty("id")))
                            .toList();
                    assertEquals(3, paths.size());
                    assertionsCycle(paths.get(0), FIRST_ALPHA_CYCLE_PROPS);
                    assertionsCycle(paths.get(1), SECOND_ALPHA_CYCLE_PROPS);
                    assertionsCycle(paths.get(2), SELF_REL_PROPS);
                });

        // with multiple specific relationship (with MY_REL_ANOTHER, should find `(:Start {bar: 'beta'})` cycle)
        TestUtil.testResult(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes, $config) YIELD path RETURN path",
                map(
                        "config",
                        map(REL_TYPES_KEY, List.of(DEPEND_ON_REL_TYPE, "MY_REL", "MY_REL_ANOTHER", "NOT_EXISTENT"))),
                res -> {
                    List<Path> paths = Iterators.stream(res.<Path>columnAs("path"))
                            .sorted(Comparator.comparingLong(
                                    item -> (long) item.lastRelationship().getProperty("id")))
                            .toList();
                    assertEquals(4, paths.size());
                    assertionsCycle(paths.get(0), FIRST_ALPHA_CYCLE_PROPS);
                    assertionsCycle(paths.get(1), SECOND_ALPHA_CYCLE_PROPS);
                    assertionsCycle(paths.get(2), BETA_CYCLE_PROPS);
                    assertionsCycle(paths.get(3), SELF_REL_PROPS);
                });

        // with not existent relationship
        TestUtil.testCallEmpty(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes, $config) YIELD path RETURN path",
                map("config", map(REL_TYPES_KEY, List.of("NOT_EXISTENT"))));
    }

    @Test
    public void cyclesWithMaxDepth() {
        createDatasetForNodesCycles();

        // with {maxDepth: 1} config
        TestUtil.testResult(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes, $config) YIELD path RETURN path",
                map("config", map(MAX_DEPTH_KEY, 1)),
                res -> {
                    List<Path> paths = Iterators.stream(res.<Path>columnAs("path"))
                            .sorted(Comparator.comparingLong(
                                    item -> (long) item.lastRelationship().getProperty("id")))
                            .toList();
                    assertEquals(2, paths.size());
                    assertionsCycle(paths.get(0), SELF_REL_PROPS);
                    assertionsCycle(paths.get(1), ONE_STEP_PROPS);
                });

        // with {maxDepth: 0} config (only self-rel considered)
        TestUtil.testCall(
                db,
                "MATCH (m1:Start) WITH collect(m1) as nodes CALL apoc.nodes.cycles(nodes, $config) YIELD path RETURN path",
                map("config", map(REL_TYPES_KEY, List.of(DEPEND_ON_REL_TYPE), MAX_DEPTH_KEY, 0)),
                r -> assertionsCycle((Path) r.get("path"), SELF_REL_PROPS));
    }

    private void createDatasetForNodesCycles() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // node with 2 'DEPENDS_ON' cycles and 1 mixed-rel-type cycle
        db.executeTransactionally(
                """
                CREATE (m1:Start {bar: 'alpha'}) with m1
                CREATE (m1)-[:DEPENDS_ON {id: 0}]->(m2:Module {bar: 'one'})-[:DEPENDS_ON {id: 1}]->(m3:Module {bar: 'two'})-[:DEPENDS_ON {id: 2}]->(m1)
                WITH m1, m2, m3
                CREATE (m1)-[:DEPENDS_ON {id: 3}]->(m2), (m2)-[:ANOTHER {id: 4}]->(m3), (m2)-[:DEPENDS_ON {id: 5}]->(m3)
                CREATE (m1)-[:DEPENDS_ON {id: 6}]->(:Module {bar: 'seven'})-[:DEPENDS_ON {id: 7}]->(:Module {bar: 'eight'})-[:DEPENDS_ON {id: 8}]->(m1)""");

        // node with 1 mixed-rel-type cycle
        db.executeTransactionally(
                "CREATE (m1:Start {bar: 'beta'}) with m1\n"
                        + "CREATE (m1)-[:MY_REL {id: 9}]->(m2:Module {bar: 'three'})-[:MY_REL_ANOTHER  {id: 10}]->(m3:Module {bar: 'four'})-[:MY_REL {id: 11}]->(m1)");

        // node without cycle
        db.executeTransactionally(
                "CREATE (m1:Start {bar: 'gamma'}) with m1\n"
                        + "CREATE (m1)-[:DEPENDS_ON {id: 12}]->(m2:Module {bar: 'five'})-[:DEPENDS_ON {id: 13}]->(m3:Module {bar: 'six'})");

        // node with self-rel
        db.executeTransactionally(
                "CREATE (m1:Start {bar: 'delta'}) with m1\n" + "CREATE (m1)-[:DEPENDS_ON {id: 20}]->(m1)");

        // node with depth 1
        db.executeTransactionally("CREATE (m1:Start {bar: 'epsilon'}) with m1\n"
                + "CREATE (m1)-[:DEPTH_ONE {id: 30}]->(:Module {bar: 'seven'})-[:DEPTH_ONE {id: 31}]->(m1)");
    }

    private void assertionsCycle(Path path, List<String> expectedProps, String customRelType) {
        List<String> propNodes = Iterables.stream(path.nodes())
                .map(node -> (String) node.getProperty("bar"))
                .collect(Collectors.toList());
        assertEquals(expectedProps, propNodes);
        if (customRelType != null) {
            assertTrue(Iterables.stream(path.relationships())
                    .allMatch(rel -> rel.getType().name().equals(customRelType)));
        }
    }

    private void assertionsCycle(Path path, List<String> expectedProps) {
        assertionsCycle(path, expectedProps, null);
    }

    @Test
    public void link() {
        db.executeTransactionally(
                "UNWIND range(1,10) as id CREATE (n:Foo {id:id}) WITH collect(n) as nodes call apoc.nodes.link(nodes,'BAR') RETURN size(nodes) as len");

        long len = TestUtil.singleResultFirstColumn(db, "MATCH (n:Foo {id:1})-[r:BAR*9]->() RETURN size(r) as len");
        assertEquals(9L, len);
    }

    @Test
    public void linkWithAvoidDuplicateTrue() {
        db.executeTransactionally(
                "CREATE (n:Foo {id:1}), (m:Foo {id:2}) WITH [n,m] as nodes CALL apoc.nodes.link(nodes,'BAR') RETURN 1");
        TestUtil.testCall(
                db, "MATCH (n:Foo)-[r]->() RETURN count(r) as count", row -> assertEquals(1L, row.get("count")));

        // with avoidDuplicates
        db.executeTransactionally(
                "MATCH (n:Foo) WITH collect(n) as nodes CALL apoc.nodes.link(nodes,'BAR', {avoidDuplicates: true}) RETURN 1");
        TestUtil.testCall(
                db, "MATCH (n:Foo)-[r]->() RETURN count(r) as count", row -> assertEquals(1L, row.get("count")));

        // without avoidDuplicates
        db.executeTransactionally("MATCH (n:Foo) WITH collect(n) as nodes CALL apoc.nodes.link(nodes,'BAR') RETURN 1");
        TestUtil.testCall(
                db, "MATCH (n:Foo)-[r]->() RETURN count(r) as count", row -> assertEquals(2L, row.get("count")));
    }

    @Test
    public void delete() {
        db.executeTransactionally("UNWIND range(1,100) as id CREATE (n:Foo {id:id})-[:X]->(n)");

        long count = TestUtil.singleResultFirstColumn(
                db,
                "MATCH (n:Foo) WITH collect(n) as nodes call apoc.nodes.delete(nodes,1) YIELD value as count RETURN count");
        assertEquals(100L, count);

        count = TestUtil.singleResultFirstColumn(db, "MATCH (n:Foo) RETURN count(*) as count");
        assertEquals(0L, count);
    }

    @Test
    public void nodesDeleteTest() {
        db.executeTransactionally("CREATE (:FOO), (:BAR), (:BAZ)");

        long count = TestUtil.singleResultFirstColumn(
                db,
                """
                MATCH (foo:FOO), (bar:BAR), (baz:BAZ)
                CALL apoc.nodes.delete([foo, id(bar), elementId(baz)],1)
                YIELD value AS count
                RETURN count""");
        assertEquals(3L, count);

        count = TestUtil.singleResultFirstColumn(db, "MATCH (n) RETURN count(*) as count");
        assertEquals(0L, count);
    }

    @Test
    public void types() {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(f) CREATE (f)-[:X]->(f)");
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.types(n) AS value",
                (r) -> assertEquals(asSet("X", "Y"), asSet(((List) r.get("value")).toArray())));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X') AS value",
                (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X|Z') AS value",
                (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationship.types(n) AS value",
                (r) -> assertEquals(Collections.emptyList(), r.get("value")));
        TestUtil.testCall(db, "RETURN apoc.node.relationship.types(null) AS value", (r) -> assertNull(r.get("value")));
    }

    @Test
    public void nodesTypes() {
        // given
        db.executeTransactionally("CREATE (f:Foo), (f)-[:Y]->(f), (f)-[:X]->(f)");
        db.executeTransactionally("CREATE (f:Bar), (f)-[:YY]->(f), (f)-[:XX]->(f)");

        // when
        TestUtil.testCall(
                db,
                "MATCH (f:Foo), (b:Bar) RETURN apoc.nodes.relationship.types([f, elementId(b)]) AS value",
                (result) -> {
                    // then
                    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get("value");
                    assertFalse("value should not be empty", list.isEmpty());
                    list.forEach(map -> {
                        Node node = (Node) map.get("node");
                        List<String> data = (List<String>) map.get("types");
                        if (node.hasLabel(Label.label("Foo"))) {
                            assertEquals(asSet("X", "Y"), asSet(data.iterator()));
                        } else {
                            assertEquals(asSet("XX", "YY"), asSet(data.iterator()));
                        }
                    });
                });
    }

    @Test
    public void nodesHasRelationship() {
        // given
        db.executeTransactionally("CREATE (f:Foo), (f)-[:X]->(f)");
        db.executeTransactionally("CREATE (b:Bar), (b)-[:Y]->(b)");

        // when
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.nodes.relationships.exist(collect(elementId(n)), 'X|Y') AS value",
                (result) -> {
                    // then
                    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get("value");
                    assertFalse("value should not be empty", list.isEmpty());
                    list.forEach(map -> {
                        Map<String, Boolean> data = (Map<String, Boolean>) map.get("exists");
                        assertEquals(map("X", true, "Y", false), data);
                    });
                });

        TestUtil.testCall(
                db, "MATCH (n:Bar) RETURN apoc.nodes.relationships.exist(collect(n), 'X|Y') AS value", (result) -> {
                    // then
                    List<Map<String, Object>> list = (List<Map<String, Object>>) result.get("value");
                    assertFalse("value should not be empty", list.isEmpty());
                    list.forEach(map -> {
                        Map<String, Boolean> data = (Map<String, Boolean>) map.get("exists");
                        assertEquals(map("X", false, "Y", true), data);
                    });
                });
    }

    @Test
    public void hasRelationship() {
        db.executeTransactionally(
                "CREATE (:Foo)-[:Y]->(:Bar),(n:FooBar) WITH n UNWIND range(1,100) as _ CREATE (n)-[:X]->(n)");
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'Y') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'Y>') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'<Y') AS value",
                (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'X') AS value",
                (r) -> assertEquals(false, r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'Y') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'Y>') AS value",
                (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'<Y') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'X') AS value",
                (r) -> assertEquals(false, r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'X') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'X>') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'<X') AS value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'Y') AS value",
                (r) -> assertEquals(false, r.get("value")));
    }

    @Test
    public void hasRelationships() {
        db.executeTransactionally(
                "CREATE (:Foo)-[:Y]->(:Bar),(n:FooBar) WITH n UNWIND range(1,100) as _ CREATE (n)-[:X]->(n)");
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'Y') AS value",
                (r) -> assertEquals(map("Y", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'Y>') AS value",
                (r) -> assertEquals(map("Y>", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'<Y') AS value",
                (r) -> assertEquals(map("<Y", false), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'X') AS value",
                (r) -> assertEquals(map("X", false), r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'Y') AS value",
                (r) -> assertEquals(map("Y", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'Y>') AS value",
                (r) -> assertEquals(map("Y>", false), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'<Y') AS value",
                (r) -> assertEquals(map("<Y", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'X') AS value",
                (r) -> assertEquals(map("X", false), r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'X') AS value",
                (r) -> assertEquals(map("X", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'X>') AS value",
                (r) -> assertEquals(map("X>", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'<X') AS value",
                (r) -> assertEquals(map("<X", true), r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'Y') AS value",
                (r) -> assertEquals(map("Y", false), r.get("value")));
    }

    @Test
    public void testConnected() {
        db.executeTransactionally("CREATE (st:StartThin),(et:EndThin),(ed:EndDense)");
        int relCount = 20;
        for (int rel = 0; rel < relCount; rel++) {
            db.executeTransactionally(
                    "MATCH (st:StartThin),(et:EndThin),(ed:EndDense) " + " CREATE (st)-[:REL"
                            + rel + "]->(et) " + " WITH * UNWIND RANGE(1,$count) AS id CREATE (st)-[:REL"
                            + rel + "]->(ed)",
                    map("count", relCount - rel));
        }

        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e) as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL3') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL10>') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL20') as value",
                (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL15>|REL20') as value",
                (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e) as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL3') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL10>') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL20') as value",
                (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL15>|REL20') as value",
                (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s) as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL3') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL10<') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL20') as value",
                (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL15<|REL20') as value",
                (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s) as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL3') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL10<') as value",
                (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL20') as value",
                (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(
                db,
                "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL15<|REL20') as value",
                (r) -> assertEquals(true, r.get("value")));

        // todo inverse e,s then also incoming
    }

    @Test
    public void testDegreeTypeAndDirection() {
        db.executeTransactionally(
                "CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(
                db,
                "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<X') as in, apoc.node.degree(f, 'Y>') as out",
                (r) -> {
                    assertEquals(1L, r.get("in"));
                    assertEquals(2L, r.get("out"));
                });
    }

    @Test
    public void testDegreeMultiple() {
        db.executeTransactionally(
                "CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<X|Y') as all", (r) -> {
            assertEquals(3L, r.get("all"));
        });
    }

    @Test
    public void testDegreeTypeOnly() {
        db.executeTransactionally(
                "CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(
                db,
                "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, 'X') as in, apoc.node.degree(f, 'Y') as out",
                (r) -> {
                    assertEquals(2L, r.get("in"));
                    assertEquals(2L, r.get("out"));
                });
    }

    @Test
    public void testDegreeDirectionOnly() {
        db.executeTransactionally(
                "CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(
                db,
                "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<') as in, apoc.node.degree(f, '>') as out",
                (r) -> {
                    assertEquals(1L, r.get("in"));
                    assertEquals(2L, r.get("out"));
                });
    }

    @Test
    public void testDegreeInOutDirectionOnly() {
        db.executeTransactionally(
                "CREATE (a:Person{name:'test'}) CREATE (b:Person) CREATE (c:Person) CREATE (d:Person) CREATE (a)-[:Rel1]->(b) CREATE (a)-[:Rel1]->(c) CREATE (a)-[:Rel2]->(d) CREATE (a)-[:Rel1]->(b) CREATE (a)<-[:Rel2]-(b) CREATE (a)<-[:Rel2]-(c) CREATE (a)<-[:Rel2]-(d) CREATE (a)<-[:Rel1]-(d)");

        TestUtil.testCall(
                db,
                "MATCH (a:Person{name:'test'})  RETURN apoc.node.degree.in(a) as in, apoc.node.degree.out(a) as out",
                (r) -> {
                    assertEquals(4L, r.get("in"));
                    assertEquals(4L, r.get("out"));
                });
    }

    @Test
    public void testDegreeInOutType() {
        db.executeTransactionally(
                "CREATE (a:Person{name:'test'}) CREATE (b:Person) CREATE (c:Person) CREATE (d:Person) CREATE (a)-[:Rel1]->(b) CREATE (a)-[:Rel1]->(c) CREATE (a)-[:Rel2]->(d) CREATE (a)-[:Rel1]->(b) CREATE (a)<-[:Rel2]-(b) CREATE (a)<-[:Rel2]-(c) CREATE (a)<-[:Rel2]-(d) CREATE (a)<-[:Rel1]-(d)");

        TestUtil.testCall(
                db,
                "MATCH (a:Person{name:'test'})  RETURN apoc.node.degree.in(a, 'Rel1') as in1, apoc.node.degree.out(a, 'Rel1') as out1, apoc.node.degree.in(a, 'Rel2') as in2, apoc.node.degree.out(a, 'Rel2') as out2",
                (r) -> {
                    assertEquals(1L, r.get("in1"));
                    assertEquals(3L, r.get("out1"));
                    assertEquals(3L, r.get("in2"));
                    assertEquals(1L, r.get("out2"));
                });
    }

    @Test
    public void testId() {
        assertTrue(TestUtil.<Long>singleResultFirstColumn(db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.id(f) AS id")
                >= 0);
        assertTrue(TestUtil.<Long>singleResultFirstColumn(
                        db, "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.id(node) AS id")
                < 0);
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.node.id(null) AS id"));
    }

    @Test
    public void testRelId() {
        assertTrue(TestUtil.<Long>singleResultFirstColumn(
                        db, "CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.id(rel) AS id")
                >= 0);
        assertTrue(TestUtil.<Long>singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.id(rel) AS id")
                < 0);
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.rel.id(null) AS id"));
    }

    @Test
    public void testLabels() {
        assertEquals(
                singletonList("Foo"),
                TestUtil.<List<String>>singleResultFirstColumn(
                        db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.labels(f) AS labels"));
        assertEquals(
                singletonList("Foo"),
                TestUtil.<List<String>>singleResultFirstColumn(
                        db,
                        "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.labels(node) AS labels"));
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.node.labels(null) AS labels"));
    }

    @Test
    public void testProperties() {
        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f) AS props"));
        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node) AS props"));

        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel) AS props"));
        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel) AS props"));

        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.any.properties(null) AS props"));
    }

    @Test
    public void testSubProperties() {
        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['foo']) AS props"));
        assertEquals(
                emptyMap(),
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['bar']) AS props"));
        assertNull(TestUtil.singleResultFirstColumn(
                db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(null,['foo']) AS props"));
        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['foo']) AS props"));
        assertEquals(
                emptyMap(),
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['bar']) AS props"));

        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['foo']) AS props"));
        assertEquals(
                emptyMap(),
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['bar']) AS props"));
        assertEquals(
                singletonMap("foo", "bar"),
                TestUtil.singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['foo']) AS props"));
        assertEquals(
                emptyMap(),
                TestUtil.singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['bar']) AS props"));

        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.any.properties(null,['foo']) AS props"));
    }

    @Test
    public void testProperty() {
        assertEquals(
                "bar", TestUtil.singleResultFirstColumn(db, "RETURN apoc.any.property({foo:'bar'},'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.any.property({foo:'bar'},'bar') AS props"));

        assertEquals(
                "bar",
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(
                db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'bar') AS props"));

        assertEquals(
                "bar",
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(
                db, "CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'bar') AS props"));

        assertEquals(
                "bar",
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'foo') AS props"));
        assertNull(
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'bar') AS props"));

        assertEquals(
                "bar",
                TestUtil.singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'foo') AS props"));
        assertNull(
                TestUtil.singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'bar') AS props"));

        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.any.property(null,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.any.property(null,null) AS props"));
    }

    @Test
    public void testRelType() {
        assertEquals(
                "REL",
                TestUtil.singleResultFirstColumn(
                        db, "CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.type(rel) AS type"));

        assertEquals(
                "REL",
                TestUtil.singleResultFirstColumn(
                        db,
                        "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.type(rel) AS type"));

        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.rel.type(null) AS type"));
    }

    @Test
    public void testMergeSelfRelationship() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(
                db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c "
                        + "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, selfRel: true, countMerge: true}) yield from, rel, to "
                        + "return from, rel, to",
                (r) -> {
                    Map<String, Object> map = r.next();

                    assertMerge(
                            map,
                            Util.map("name", "b1", "count", 2),
                            label, // FROM
                            Util.map("count", 1),
                            "KNOWS", // REL
                            Util.map("name", "b1", "count", 2),
                            label); // TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeSelfRelationshipInverted() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("BLabel"), label("ALabel"));

        TestUtil.testResult(
                db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c "
                        + "CALL apoc.nodes.collapse([c,p],{mergeVirtualRels:true, selfRel: true, countMerge: true}) yield from, rel, to "
                        + "return from, rel, to",
                (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "a1", "count", 2),
                            label, // FROM
                            Util.map("count", 1),
                            "KNOWS", // REL
                            Util.map("name", "a1", "count", 2),
                            label); // TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeNotSelfRelationship() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(
                db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c "
                        + "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, countMerge: true}) yield from, rel, to "
                        + "return from, rel, to",
                (r) -> {
                    Map<String, Object> map = r.next();

                    assertEquals(
                            Util.map("name", "b1", "count", 2), ((VirtualNode) map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode) map.get("from")));
                    assertNull(((VirtualRelationship) map.get("rel")));
                    assertNull(((Node) map.get("to")));
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeWithRelationshipDirection() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE " + "(a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'}),"
                + "(a1)<-[:KNOWS]-(b2:CLabel {name:'c1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(
                db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c "
                        + "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, selfRel: true}) yield from, rel, to "
                        + "return from, rel, to",
                (r) -> {
                    Map<String, Object> map = r.next();

                    assertEquals(Util.map("name", "c1"), ((Node) map.get("from")).getAllProperties());
                    assertEquals(asList(label("CLabel")), ((Node) map.get("from")).getLabels());
                    assertEquals(Collections.emptyMap(), ((VirtualRelationship) map.get("rel")).getAllProperties());
                    assertEquals(
                            "KNOWS",
                            ((VirtualRelationship) map.get("rel")).getType().name());
                    assertEquals(Util.map("name", "b1", "count", 2), ((Node) map.get("to")).getAllProperties());
                    assertEquals(label, labelSet((Node) map.get("to")));
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertEquals(
                            Util.map("name", "b1", "count", 2), ((VirtualNode) map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode) map.get("from")));
                    assertEquals(Util.map("count", 1), ((VirtualRelationship) map.get("rel")).getAllProperties());
                    assertEquals(
                            "KNOWS",
                            ((VirtualRelationship) map.get("rel")).getType().name());
                    assertEquals(Util.map("name", "b1", "count", 2), ((VirtualNode) map.get("to")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode) map.get("to")));
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeRelationship() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE " + "(a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'}),"
                + "(a2:ALabel {name:'a2'})-[:HAS_REL]->(b1),"
                + "(a4:ALabel {name:'a4'})-[:HAS_REL]->(b4:BLabel {name:'b4'})");

        Set<Label> label = asSet(label("ALabel"));

        TestUtil.testResult(
                db,
                "MATCH (p:ALabel{name:'a4'}), (p1:ALabel{name:'a2'}), (p2:ALabel{name:'a1'}) WITH p, p1, p2 "
                        + "CALL apoc.nodes.collapse([p, p1, p2],{mergeVirtualRels:true}) yield from, rel, to "
                        + "return from, rel, to",
                (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "a1", "count", 3),
                            label, // FROM
                            Collections.emptyMap(),
                            "HAS_REL", // REL
                            Util.map("name", "b4"),
                            asSet(label("BLabel"))); // TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "a1", "count", 3),
                            label, // FROM
                            Util.map("count", 1),
                            "HAS_REL", // REL
                            Util.map("name", "b1"),
                            asSet(label("BLabel"))); // TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergePersonEmployee() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE " + "(:Person {name:'mike'})-[:LIVES_IN]->(:City{name:'rome'}), "
                + "(:Employee{name:'mike'})-[:WORKS_FOR]->(:Company{name:'Larus'}), "
                + "(:Person {name:'kate'})-[:LIVES_IN]->(:City{name:'london'}), "
                + "(:Employee{name:'kate'})-[:WORKS_FOR]->(:Company{name:'Neo'})");

        Set<Label> label = asSet(label("Collapsed"), label("Person"), label("Employee"));

        TestUtil.testResult(
                db,
                "MATCH (p:Person)-[r:LIVES_IN]->(c:City), (e:Employee)-[w:WORKS_FOR]->(m:Company) WITH p,r,c,e,w,m WHERE p.name = e.name "
                        + "CALL apoc.nodes.collapse([p,e],{properties:'combine', mergeVirtualRels:true, countMerge: true, collapsedLabel: true}) yield from, rel, to "
                        + "return from, rel, to",
                (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "mike", "count", 2),
                            label, // FROM
                            Collections.emptyMap(),
                            "LIVES_IN", // REL
                            Util.map("name", "rome"),
                            asSet(label("City"))); // TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "mike", "count", 2),
                            label, // FROM
                            Collections.emptyMap(),
                            "WORKS_FOR", // REL
                            Util.map("name", "Larus"),
                            asSet(label("Company"))); // TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "kate", "count", 2),
                            label, // FROM
                            Collections.emptyMap(),
                            "LIVES_IN", // REL
                            Util.map("name", "london"),
                            asSet(label("City"))); // TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(
                            map,
                            Util.map("name", "kate", "count", 2),
                            label, // FROM
                            Collections.emptyMap(),
                            "WORKS_FOR", // REL
                            Util.map("name", "Neo"),
                            asSet(label("Company"))); // TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeVirtualNode() {
        db.executeTransactionally(
                """
                CREATE
                (p:Person {name: 'John'})-[:LIVES_IN]->(c:City{name:'London'}),
                (p1:Person {name: 'Mike'})-[:LIVES_IN]->(c),
                (p2:Person {name: 'Kate'})-[:LIVES_IN]->(c),
                (p3:Person {name: 'Budd'})-[:LIVES_IN]->(c),
                (p4:Person {name: 'Alex'})-[:LIVES_IN]->(c),
                (p1)-[:KNOWS]->(p),
                (p2)-[:KNOWS]->(p1),
                (p2)-[:KNOWS]->(p3),
                (p4)-[:KNOWS]->(p3)
                """);

        Set<Label> label = asSet(label("City"), label("Person"));

        TestUtil.testResult(
                db,
                """
                MATCH (p:Person)-[:LIVES_IN]->(c:City)
                WITH c, c + collect(p) as subgraph
                CALL apoc.nodes.collapse(subgraph,{properties:'discard', mergeVirtualRels:true, countMerge: true}) yield from, rel, to return from, rel, to""",
                null,
                result -> {
                    Map<String, Object> map = result.next();

                    assertEquals(
                            Util.map("name", "London", "count", 6), ((VirtualNode) map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode) map.get("from")));
                    assertNull(map.get("rel"));
                    assertNull(map.get("to"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testMergeVirtualNodeBOTH() {
        db.executeTransactionally("CREATE \n" + "(p:Person {name: 'John'})-[:LIVES_IN]->(c:City{name:'London'}),"
                + "(c)-[:LIVES_IN]->(p)");

        Set<Label> label = asSet(label("City"), label("Person"));

        TestUtil.testResult(
                db,
                "MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:LIVES_IN]->(b:Person)\n"
                        + "CALL apoc.nodes.collapse([c,p,b],{mergeVirtualRels:true, countMerge: true, selfRel: true}) yield from, rel, to return from, rel, to",
                null,
                result -> {
                    Map<String, Object> map = result.next();

                    assertMerge(
                            map,
                            Util.map("name", "John", "count", 2),
                            label, // FROM
                            Util.map("count", 3),
                            "LIVES_IN", // REL
                            Util.map("name", "John", "count", 2),
                            label); // TO
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testIsDeleted() {
        db.executeTransactionally("CREATE \n" + "(:NodeA)-[:HAS_REL_A]->(:NodeB)");

        TestUtil.testResult(
                db,
                "MATCH (a:NodeA)-[relA:HAS_REL_A]->() WITH a, relA, apoc.any.isDeleted(a) AS deletedNode1, apoc.any.isDeleted(relA) AS deletedRel1\n"
                        + "DETACH DELETE a RETURN deletedNode1, deletedRel1, apoc.any.isDeleted(a) AS deletedNode2, apoc.any.isDeleted(relA) AS deletedRel2",
                result -> {
                    Map<String, Object> map = result.next();

                    assertFalse((boolean) map.get("deletedNode1"));
                    assertFalse((boolean) map.get("deletedRel1"));
                    assertTrue((boolean) map.get("deletedNode2"));
                    assertTrue((boolean) map.get("deletedRel2"));
                });
    }

    private static void assertMerge(
            Map<String, Object> map,
            Map<String, Object> fromProperties,
            Set<Label> fromLabel,
            Map<String, Object> relProperties,
            String relType,
            Map<String, Object> toProperties,
            Set<Label> toLabel) {
        assertEquals(fromProperties, ((VirtualNode) map.get("from")).getAllProperties());
        assertEquals(fromLabel, labelSet((VirtualNode) map.get("from")));
        assertEquals(relProperties, ((VirtualRelationship) map.get("rel")).getAllProperties());
        assertEquals(relType, ((VirtualRelationship) map.get("rel")).getType().name());
        assertEquals(toProperties, ((Node) map.get("to")).getAllProperties());
        assertEquals(toLabel, labelSet((Node) map.get("to")));
    }

    private static Set<Label> labelSet(Node node) {
        return Iterables.asSet(node.getLabels());
    }
}
