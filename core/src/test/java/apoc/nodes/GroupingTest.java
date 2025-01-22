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

import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.junit.Assert.*;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DurationValue;

/**
 * @author mh
 * @since 22.06.17
 */
public class GroupingTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule().withSetting(procedure_unrestricted, List.of("apoc*"));

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Grouping.class, Nodes.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    public void createGraph() {
        db.executeTransactionally("CREATE " + "(alice:Person {name:'Alice', gender:'female', age:32, kids:1}),"
                + "(bob:Person   {name:'Bob',   gender:'male',   age:42, kids:3}),"
                + "(eve:Person   {name:'Eve',   gender:'female', age:28, kids:2}),"
                + "(graphs:Forum {name:'Graphs',    members:23}),"
                + "(dbs:Forum    {name:'Databases', members:42}),"
                + "(alice)-[:KNOWS {since:2017}]->(bob),"
                + "(eve)-[:KNOWS   {since:2018}]->(bob),"
                + "(alice)-[:MEMBER_OF]->(graphs),"
                + "(alice)-[:MEMBER_OF]->(dbs),"
                + "(bob)-[:MEMBER_OF]->(dbs),"
                + "(eve)-[:MEMBER_OF]->(graphs)");
    }

    @Test
    public void testGroupAllNodes() {
        createGraph();
        Map<String, Object> female = map("gender", "female", "count_*", 2L, "min_age", 28L);
        Map<String, Object> male = map("gender", "male", "count_*", 1L, "min_age", 42L);
        Map<String, Object> other = map("gender", null, "count_*", 2L);

        testResult(
                db,
                "CALL apoc.nodes.group(" + "['*'],['gender'],[" + "{`*`:'count', age:'min'}," + "{`*`:'count'}" + "])",
                (result) -> {
                    assertTrue(result.hasNext());

                    String[] keys = {"count_*", "gender", "min_age"};
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();

                        Node node = (Node) row.get("node");
                        Object value = node.getProperty("gender");

                        Relationship rel = (Relationship) row.get("relationship");
                        if (value == null) {
                            assertEquals(other, node.getProperties(keys));
                            assertNull(rel);
                        } else if (value.equals("female")) {
                            assertEquals(female, node.getProperties(keys));
                            //              assertEquals(2L, rels.size());
                            Object count = rel.getProperty("count_*");
                            if (count.equals(3L)) { // MEMBER_OF
                                assertEquals(other, rel.getEndNode().getProperties(keys));
                            } else if (count.equals(2L)) { // KNOWS
                                assertEquals(male, rel.getEndNode().getProperties(keys));
                            } else {
                                assertTrue("Unexpected count value: " + count, false);
                            }
                        } else if (value.equals("male")) {
                            assertEquals(male, node.getProperties(keys));
                            assertEquals(1L, rel.getProperty("count_*"));
                            assertEquals(other, rel.getEndNode().getProperties(keys));
                        } else {
                            assertTrue("Unexpected value: " + value, false);
                        }
                    }
                });
    }

    @Test
    public void testGroupNode() {
        createGraph();
        Map<String, Object> female =
                map("gender", "female", "count_*", 2L, "sum_kids", 3L, "min_age", 28L, "max_age", 32L, "avg_age", 30D);
        Map<String, Object> male =
                map("gender", "male", "count_*", 1L, "sum_kids", 3L, "min_age", 42L, "max_age", 42L, "avg_age", 42D);
        testResult(
                db,
                "CALL apoc.nodes.group(" + "['Person'],['gender'],["
                        + "{`*`:'count', kids:'sum', age:['min', 'max', 'avg'], gender:'collect'},"
                        + "{`*`:'count', since:['min', 'max']}"
                        + "])",
                (result) -> {
                    assertTrue(result.hasNext());
                    Map<String, Object> row = result.next();
                    Node node = (Node) row.get("node");
                    String[] keys = {"count_*", "gender", "sum_kids", "min_age", "max_age", "avg_age"};
                    assertEquals(node.getProperty("gender").equals("female") ? female : male, node.getProperties(keys));
                    Relationship rel = (Relationship) row.get("relationship");
                    assertEquals(2L, rel.getProperty("count_*"));
                    assertEquals(2017L, rel.getProperty("min_since"));
                    assertEquals(2018L, rel.getProperty("max_since"));
                    assertEquals("KNOWS", rel.getType().name());
                    node = rel.getOtherNode(node);
                    assertEquals(node.getProperty("gender").equals("female") ? female : male, node.getProperties(keys));

                    assertTrue(result.hasNext());
                    row = result.next();

                    node = (Node) row.get("node");
                    assertEquals(node.getProperty("gender").equals("female") ? female : male, node.getProperties(keys));
                    rel = (Relationship) row.get("relationship");
                    assertEquals(null, rel);
                });
    }

    @Test
    public void testRemoveOrphans() {
        db.executeTransactionally("CREATE (u:User {gender:'male'})");
        TestUtil.testCallCount(db, "CALL apoc.nodes.group(['User'],['gender'],null,{orphans:false})", 0);
        TestUtil.testCallCount(db, "CALL apoc.nodes.group(['User'],['gender'],null,{orphans:true})", 1);
    }

    @Test
    public void testGroupWithDatetimes() {
        db.executeTransactionally(
                """
                UNWIND range(1, 1000) AS minutes
                CREATE (f:Foo {
                    created_at: datetime({ year: 2019, month: 3, day: 23 }) - duration({minutes: minutes})
                })
                SET f.created_at_hour = datetime.truncate('hour', f.created_at)
                """);
        TestUtil.testCallCount(
                db,
                "CALL apoc.nodes.group(['Foo'], ['created_at_hour'], [{ created_at: 'min' }]) YIELD node\n"
                        + "RETURN node",
                17);

        // Delete nodes
        db.executeTransactionally("MATCH (n:Foo) DELETE n");
    }

    public static class TestObject {
        final String testValues; // Values to be inserted as nodes
        final String expectedMin; // Expected minimum value
        final String expectedMax; // Expected maximum value

        public TestObject(String testValues, String expectedMin, String expectedMax) {
            this.testValues = testValues;
            this.expectedMin = expectedMin;
            this.expectedMax = expectedMax;
        }
    }

    @Test
    public void testGroupWithVariousProperties() {
        List<TestObject> testObjects = List.of(
                new TestObject("42, 99, 12, 34", "12", "99"),
                new TestObject("\"alpha\", \"beta\", \"zeta\"", "\"alpha\"", "\"zeta\""),
                new TestObject("true, false, true", "false", "true"),
                new TestObject(
                        "datetime({ year: 2022, month: 1, day: 1 }), datetime({ year: 2021, month: 1, day: 1 }), datetime({ year: 2023, month: 1, day: 1 })",
                        "datetime({ year: 2021, month: 1, day: 1 })",
                        "datetime({ year: 2023, month: 1, day: 1 })"),
                new TestObject(
                        "localdatetime({ year: 2022, month: 1, day: 1 }), localdatetime({ year: 2021, month: 1, day: 1 }), localdatetime({ year: 2023, month: 1, day: 1 })",
                        "localdatetime({ year: 2021, month: 1, day: 1 })",
                        "localdatetime({ year: 2023, month: 1, day: 1 })"),
                new TestObject(
                        "localtime({hour: 10, minute: 30, second: 1}), localtime({hour: 4, minute: 23, second: 3}), localtime({hour: 6, minute: 33, second: 15})",
                        "localtime({hour: 4, minute: 23, second: 3})",
                        "localtime({hour: 10, minute: 30, second: 1})"),
                new TestObject(
                        "time({hour: 10, minute: 30, second: 1}), time({hour: 4, minute: 23, second: 3}), time({hour: 6, minute: 33, second: 15})",
                        "time({hour: 4, minute: 23, second: 3})",
                        "time({hour: 10, minute: 30, second: 1})"),
                new TestObject(
                        "date({ year: 2022, month: 1, day: 1 }), date({ year: 2021, month: 1, day: 1 }), date({ year: 2023, month: 1, day: 1 })",
                        "date({ year: 2021, month: 1, day: 1 })",
                        "date({ year: 2023, month: 1, day: 1 })"),
                new TestObject("[1, 0, 3], [1, 2, 3]", "[1, 0, 3]", "[1, 2, 3]"),
                // Mixed values
                new TestObject("1, [1, 2, 3], false", "[1, 2, 3]", "1"),
                new TestObject("1, [1, 2, 3], null", "[1, 2, 3]", "1"),
                new TestObject("duration('P11DT16H12M'), \"alpha\", false", "duration('P11DT16H12M')", "false"),
                new TestObject(
                        "date({ year: 2022, month: 1, day: 1 }), localtime({hour: 10, minute: 30, second: 1}), datetime({ year: 2022, month: 1, day: 1 })",
                        "datetime({ year: 2022, month: 1, day: 1 })",
                        "localtime({hour: 10, minute: 30, second: 1})"));

        for (TestObject testObject : testObjects) {
            runTestForProperty(testObject);
        }
    }

    private void runTestForProperty(TestObject testObject) {
        String testValues = testObject.testValues;
        String expectedMin = testObject.expectedMin;
        String expectedMax = testObject.expectedMax;

        // Create nodes in the database
        db.executeTransactionally(String.format(
                """
            UNWIND [%s] AS value
            CREATE (n:Test { testValue: value, groupKey: 1 })
            """,
                testValues));

        // Test for minimum value
        TestUtil.testCall(
                db,
                String.format(
                        """
            CALL apoc.nodes.group(['Test'], ['groupKey'], [{ testValue: 'min' }]) YIELD node
            WITH apoc.any.property(node, 'min_testValue') AS result
            RETURN result = %s AS value, result
            """,
                        expectedMin),
                (row) -> assertTrue(
                        "Testing: " + testValues + "; expected: " + expectedMin + "; but got: " + row.get("result"),
                        (Boolean) row.get("value")));

        // Test for maximum value
        TestUtil.testCall(
                db,
                String.format(
                        """
            CALL apoc.nodes.group(['Test'], ['groupKey'], [{ testValue: 'max' }]) YIELD node
            WITH apoc.any.property(node, 'max_testValue') AS result
            RETURN result = %s AS value, result
            """,
                        expectedMax),
                (row) -> assertTrue(
                        "Testing: " + testValues + "; expected: " + expectedMax + "; but got: " + row.get("result"),
                        (Boolean) row.get("value")));

        // Delete nodes
        db.executeTransactionally("MATCH (n:Test) DELETE n");
    }

    @Test
    public void testSumAndAvg() {
        // Create nodes in the database
        db.executeTransactionally(
                """
            UNWIND [
                [duration('P11DT16H12M'), 1, 3],
                [duration('P1DT16H12M'), 2, duration('P1DT16H12M')],
                [duration('P3DT20H12M'), 3, 4]] AS value
            CREATE (n:Test { durationValue: value[0], intValue: value[1], mixedValue: value[2], groupKey: 1 })
            """);

        // Test for sum value
        TestUtil.testCall(
                db,
                """
            CALL apoc.nodes.group(['Test'], ['groupKey'], [{ durationValue: 'sum', intValue: 'sum' }]) YIELD node
            WITH apoc.any.property(node, 'sum_durationValue') AS sum_durationValue, apoc.any.property(node, 'sum_intValue') AS sum_intValue
            RETURN sum_durationValue, sum_intValue
            """,
                (row) -> {
                    assertEquals(DurationValue.duration(0, 15, 189360, 0), row.get("sum_durationValue"));
                    assertEquals(6L, row.get("sum_intValue"));
                });

        // Test for avg value
        TestUtil.testCall(
                db,
                """
            CALL apoc.nodes.group(['Test'], ['groupKey'], [{ durationValue: 'avg', intValue: 'avg' }]) YIELD node
            WITH apoc.any.property(node, 'avg_durationValue') AS avg_durationValue, apoc.any.property(node, 'avg_intValue') AS avg_intValue
            RETURN avg_durationValue, avg_intValue
            """,
                (row) -> {
                    assertEquals(DurationValue.duration(0, 5, 126240, 0), row.get("avg_durationValue"));
                    assertEquals(2.0, row.get("avg_intValue"));
                });

        // Delete nodes
        db.executeTransactionally("MATCH (n:Test) DELETE n");
    }

    @Test
    public void testSelfRels() {
        db.executeTransactionally("CREATE (u:User {gender:'male'})-[:REL]->(u)");

        Relationship rel = TestUtil.singleResultFirstColumn(
                db,
                "CALL apoc.nodes.group(['User'],['gender'],null,{selfRels:true}) yield relationship return relationship");
        assertNotNull(rel);

        rel = TestUtil.singleResultFirstColumn(
                db,
                "CALL apoc.nodes.group(['User'],['gender'],null,{selfRels:false}) yield relationship return relationship");
        assertNull(rel);
    }

    @Test
    public void testFilterMin() {
        db.executeTransactionally(
                "CREATE (:User {name:'Joe',gender:'male'}), (:User {gender:'female',name:'Jane'}), (:User {gender:'female',name:'Jenny'})");
        TestUtil.testResult(
                db, "CALL apoc.nodes.group(['User'],['gender'],null,{filter:{`User.count_*.min`:2}})", result -> {
                    Node node = Iterators.single(result.columnAs("node"));
                    assertEquals("female", node.getProperty("gender"));
                });
        TestUtil.testCallCount(
                db, "CALL apoc.nodes.group(['User'],['gender'],null,{filter:{`User.count_*.min`:3}})", 0);
    }

    @Test
    public void testFilterMax() {
        db.executeTransactionally(
                "CREATE (:User {name:'Joe',gender:'male'}), (:User {gender:'female',name:'Jane'}), (:User {gender:'female',name:'Jenny'})");
        TestUtil.testResult(
                db, "CALL apoc.nodes.group(['User'],['gender'],null,{filter:{`User.count_*.max`:1}})", result -> {
                    Node node = Iterators.single(result.columnAs("node"));
                    assertEquals("male", node.getProperty("gender"));
                });
        TestUtil.testCallCount(
                db, "CALL apoc.nodes.group(['User'],['gender'],null,{filter:{`User.count_*.max`:0}})", 0);
    }

    @Test
    public void testFilterRelationshipsInclude() {
        db.executeTransactionally("CREATE (u:User {name:'Joe',gender:'male'})-[:KNOWS]->(u), (u)-[:LOVES]->(u)");
        assertEquals(
                "KNOWS",
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.nodes.group(['User'],['gender'],null,{includeRels:'KNOWS'}) yield relationship return type(relationship)"));
    }

    @Test
    public void testFilterRelationshipsExclude() {
        db.executeTransactionally("CREATE (u:User {name:'Joe',gender:'male'})-[:KNOWS]->(u), (u)-[:LOVES]->(u)");
        assertEquals(
                "KNOWS",
                TestUtil.singleResultFirstColumn(
                        db,
                        "CALL apoc.nodes.group(['User'],['gender'],null,{excludeRels:'LOVES'}) yield relationship return type(relationship)"));
    }

    @Test
    public void testFilterRelationshipsBothExcludeAndInclude() {
        db.executeTransactionally("CREATE (u:User {name:'Joe',gender:'male'})-[:KNOWS]->(u), (u)-[:LOVES]->(u)");
        final Map<String, String> conf = map("includeRels", "LOVES", "excludeRels", "LOVES");

        final Object result = TestUtil.singleResultFirstColumn(
                db,
                "CALL apoc.nodes.group(['User'], ['gender'], null, $conf) yield relationship return relationship",
                map("conf", conf));
        assertNull(result);
    }

    @Test
    public void testFilterRelationshipsExcludeAsList() {
        db.executeTransactionally("CREATE (u:User {name:'Joe',gender:'male'})-[:KNOWS]->(u), (u)-[:LOVES]->(u)");
        final Map<String, Object> conf = map("excludeRels", List.of("KNOWS", "LOVES"));

        final Object result = TestUtil.singleResultFirstColumn(
                db,
                "CALL apoc.nodes.group(['User'], ['gender'], null, $conf) yield relationship return relationship",
                map("conf", conf));
        assertNull(result);
    }

    @Test
    public void testFilterRelationshipsIncludeAsList() {
        db.executeTransactionally("CREATE (u:User {name:'Joe',gender:'male'})-[:KNOWS]->(u), (u)-[:LOVES]->(u)");
        final List<String> rels = List.of("KNOWS", "LOVES");
        final Map<String, Object> conf = map("includeRels", rels);

        testResult(
                db,
                "CALL apoc.nodes.group(['User'], ['gender'], null, $conf) yield relationship with type(relationship) as rel return rel order by rel ",
                map("conf", conf),
                r -> assertEquals(rels, Iterators.asList(r.columnAs("rel"))));
    }

    @Test
    public void testGroupAllLabels() {
        db.executeTransactionally("CREATE (u:User {name:'Joe',gender:'male'})");
        TestUtil.testResult(db, "CALL apoc.nodes.group(['*'],['gender'])", result -> {
            Node node = Iterators.single(result.columnAs("node"));
            assertEquals("User", Iterables.single(node.getLabels()).name());
        });
    }

    @Test
    public void testLimitNodes() {
        db.executeTransactionally("CREATE (:User {name:'Joe',gender:'male'}), (:User {name:'Jane',gender:'female'})");
        TestUtil.testResult(db, "CALL apoc.nodes.group(['User'],['gender'],null, {limitNodes:1})", result -> {
            Node node = Iterators.single(result.columnAs("node"));
            assertEquals("User", Iterables.single(node.getLabels()).name());
        });
    }

    @Test
    public void testLimitRelsNodes() {
        db.executeTransactionally(
                "CREATE (u:User {name:'Joe',gender:'male'})-[:KNOWS]->(u), (u)-[:LOVES]->(u), (u)-[:HATES]->(u)");
        TestUtil.testResult(db, "CALL apoc.nodes.group(['User'],['gender'],null, {relsPerNode:1})", result -> {
            Node node = Iterators.single(result.columnAs("node"));
            assertEquals("User", Iterables.single(node.getLabels()).name());
        });
    }
}
