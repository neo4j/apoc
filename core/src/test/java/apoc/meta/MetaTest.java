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
package apoc.meta;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.driver.Values.isoDuration;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;

import apoc.HelperProcedures;
import apoc.graph.Graphs;
import apoc.nodes.Nodes;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import com.google.common.collect.ImmutableMap;
import java.io.InputStreamReader;
import java.time.Clock;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

public class MetaTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseSettings.procedure_unrestricted,
                    List.of(
                            "apoc.meta.nodes.count",
                            "apoc.meta.stats",
                            "apoc.meta.data",
                            "apoc.meta.schema",
                            "apoc.meta.nodeTypeProperties",
                            "apoc.meta.relTypeProperties",
                            "apoc.meta.graph",
                            "apoc.meta.graph.of",
                            "apoc.meta.graphSample",
                            "apoc.meta.subGraph"))
            .withSetting(GraphDatabaseInternalSettings.cypher_enable_vector_type, true)
            .withSetting(
                    newBuilder("internal.dbms.debug.track_cursor_close", BOOL, false)
                            .build(),
                    false)
            .withSetting(
                    newBuilder("internal.dbms.debug.trace_cursors", BOOL, false).build(), false);

    @Before
    public void setUp() {
        TestUtil.registerProcedure(
                db, Meta.class, MetaRestricted.class, Graphs.class, Nodes.class, HelperProcedures.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    public static boolean hasRecordMatching(List<Map<String, Object>> records, Map<String, Object> record) {
        return hasRecordMatching(records, row -> {
            boolean okSoFar = true;

            for (String k : record.keySet()) {
                okSoFar = okSoFar
                        && row.containsKey(k)
                        && (row.get(k) == null
                                ? (record.get(k) == null)
                                : row.get(k).equals(record.get(k)));
            }

            return okSoFar;
        });
    }

    public static boolean hasRecordMatching(
            List<Map<String, Object>> records, Predicate<Map<String, Object>> predicate) {
        return records.stream().anyMatch(predicate);
    }

    public static List<Map<String, Object>> gatherRecords(Result r) {
        List<Map<String, Object>> rows = new ArrayList<>();
        while (r.hasNext()) {
            Map<String, Object> row = r.next();
            rows.add(row);
        }
        return rows;
    }
    // Can be valuable for debugging purposes
    @SuppressWarnings("unused")
    private static String toCSV(List<Map<String, Object>> list) {
        List<String> headers =
                list.stream().flatMap(map -> map.keySet().stream()).distinct().toList();
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < headers.size(); i++) {
            sb.append(headers.get(i));
            sb.append(i == headers.size() - 1 ? "\n" : ",");
        }
        for (Map<String, Object> map : list) {
            for (int i = 0; i < headers.size(); i++) {
                sb.append(map.get(headers.get(i)));
                sb.append(i == headers.size() - 1 ? "\n" : ",");
            }
        }
        return sb.toString();
    }

    public static boolean testDBCallEquivalence(GraphDatabaseService db, String testCall, String equivalentToCall) {
        AtomicReference<List<Map<String, Object>>> compareTo = new AtomicReference<>();
        AtomicReference<List<Map<String, Object>>> testSet = new AtomicReference<>();

        TestUtil.testResult(db, equivalentToCall, r -> compareTo.set(gatherRecords(r)));

        TestUtil.testResult(db, testCall, r -> testSet.set(gatherRecords(r)));

        // Uncomment this for debugging purposes
        /*
        System.out.println("COMPARE TO:");
        System.out.println(toCSV(compareTo.get()));
        System.out.println("TEST SET:");
        System.out.println(toCSV(testSet.get()));
        */

        return resultSetsEquivalent(compareTo.get(), testSet.get());
    }

    public static boolean resultSetsEquivalent(List<Map<String, Object>> baseSet, List<Map<String, Object>> testSet) {
        if (baseSet.size() != testSet.size()) {
            System.err.println("Result sets have different cardinality");
            return false;
        }

        boolean allMatch = true;

        for (Map<String, Object> baseRecord : baseSet) {
            allMatch = allMatch && hasRecordMatching(testSet, baseRecord);
        }

        return allMatch;
    }

    @Test
    public void testMetaGraphExtraRels() {
        db.executeTransactionally(
                """
                CREATE (a:S1 {SomeName1:'aaa'})
                CREATE (b:S2 {SomeName2:'bbb'})
                CREATE (c:S3 {SomeName3:'ccc'})
                CREATE (a)-[:HAS]->(b)
                CREATE (b)-[:HAS]->(c)""");

        testCall(db, "call apoc.meta.graph()", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(3, nodes.size());
            assertEquals(2, relationships.size());
        });
    }

    @Test
    public void testMetaGraphMaxRels() {
        db.executeTransactionally("CREATE (:S2 {id:'another'}), (:S2 {id:'another2'}), (:S2 {id:'another3'}), \n" +
                // create nodes to be linked
                "(a:S1 {id:'aaa'}), (b:S2 {id:'bbb'}), (c:S3 {id:'ccc'}), (d:S4 {id:'ddd'}), "
                + "(e:S5 {id:'eee'}), (f:S6 {id:'fff'}), (g:S7 {id:'ggg'}),"
                +
                // create rels
                "(a)-[:HAS]->(b), (a)-[:HAS]->(c), (a)-[:HAS]->(d), (a)-[:HAS]->(e), (a)-[:HAS]->(f), (a)-[:HAS]->(g),"
                + "(b)-[:HAS]->(c), (b)-[:HAS]->(d), (b)-[:HAS]->(e), (b)-[:HAS]->(f), (b)-[:HAS]->(g)");

        testCall(db, "call apoc.meta.graph()", (row) -> {
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(11, relationships.size());
        });

        testCall(db, "call apoc.meta.graph({maxRels: 1})", (row) -> {
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(8, relationships.size());
        });

        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testRelationshipExistsSampling() {
        List<Long> skipSizes = List.of(2L, 100L, 1000L, -1L, 0L, 1L);
        List<Long> fromNodeCounts = List.of(4L, 1000L, 100L, 500L, 10000L, 4L);
        List<Integer> expectedChecks = List.of(2, 10, 1, 500, 10000, 4);

        for (int i = 0; i < skipSizes.size(); i++) {
            testSampling(skipSizes.get(i), fromNodeCounts.get(i), expectedChecks.get(i));
        }
    }

    public void testSampling(Long skipSize, Long fromNodeCount, int expectedChecks) {
        SampleMetaConfig config = Mockito.mock(SampleMetaConfig.class);
        Mockito.when(config.getSample()).thenReturn(skipSize);

        Label labelFromLabel = Label.label("A");
        Label labelToLabel = Label.label("B");
        RelationshipType relationshipType = RelationshipType.withName("R");
        Direction direction = Direction.OUTGOING;

        Transaction tx = Mockito.mock(Transaction.class);
        ResourceIterator<Node> nodes = Mockito.mock(ResourceIterator.class);
        ResourceIterable<Relationship> relationships = Mockito.mock(ResourceIterable.class);
        ResourceIterator<Relationship> relationshipIterator = Mockito.mock(ResourceIterator.class);
        Node node = Mockito.mock(Node.class);

        Mockito.when(tx.findNodes(labelFromLabel)).thenReturn(nodes);
        List<Boolean> nodesNext = new ArrayList<>(Arrays.asList(new Boolean[fromNodeCount.intValue()]));
        Collections.fill(nodesNext, Boolean.TRUE);
        nodesNext.set(fromNodeCount.intValue() - 1, false);
        Mockito.when(nodes.hasNext()).thenReturn(true, nodesNext.toArray(Boolean[]::new));
        Mockito.when(nodes.next()).thenReturn(node);

        Mockito.when(node.getRelationships(direction, relationshipType)).thenReturn(relationships);
        Mockito.when(relationships.iterator()).thenReturn(relationshipIterator);

        assertFalse(Meta.relationshipExists(tx, labelFromLabel, labelToLabel, relationshipType, direction, config));
        Mockito.verify(nodes, Mockito.times(fromNodeCount.intValue())).next();

        Mockito.verify(node, Mockito.times(expectedChecks)).getRelationships(Mockito.any(), Mockito.any());
    }

    @Test
    public void testMetaType() {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("FOO"));
            testTypeName(node, "NODE");
            testTypeName(rel, "RELATIONSHIP");
            Path path = tx.traversalDescription()
                    .evaluator(toDepth(1))
                    .traverse(node)
                    .iterator()
                    .next();
            // TODO PATH FAILS              testTypeName(path, "PATH");
            tx.rollback();
        }
        testTypeName(singletonMap("a", 10), "MAP");
        testTypeName(asList(1, 2), "LIST OF INTEGER");
        testTypeName(1L, "INTEGER");
        testTypeName(1, "INTEGER");
        testTypeName(1.0D, "FLOAT");
        testTypeName(1.0, "FLOAT");
        testTypeName("a", "STRING");
        testTypeName(false, "BOOLEAN");
        testTypeName(true, "BOOLEAN");
        testTypeName(null, "NULL");
    }

    @Test
    public void testMetaTypeArray() {
        testTypeName(asList(1, 2), "LIST OF INTEGER");
        testTypeName(asList(LocalDate.of(2018, 1, 1), 2), "LIST OF ANY");
        testTypeName(new Integer[] {1, 2}, "LIST OF INTEGER");
        testTypeName(new Float[] {1f, 2f}, "LIST OF FLOAT");
        testTypeName(new Double[] {1d, 2d}, "LIST OF FLOAT");
        testTypeName(new String[] {"a", "b"}, "LIST OF STRING");
        testTypeName(new Long[] {1L, 2L}, "LIST OF INTEGER");
        testTypeName(new LocalDate[] {LocalDate.of(2018, 1, 1), LocalDate.of(2018, 1, 1)}, "LIST OF DATE");
        testTypeName(new Object[] {1d, ""}, "LIST OF ANY");
    }

    @Test
    public void testMetaIsType() {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo(node, RelationshipType.withName("FOO"));
            testIsTypeName(node, "NODE");
            testIsTypeName(rel, "RELATIONSHIP");
            Path path = tx.traversalDescription()
                    .evaluator(toDepth(1))
                    .traverse(node)
                    .iterator()
                    .next();
            // TODO PATH FAILS            testIsTypeName(path, "PATH");
            tx.rollback();
        }
        testIsTypeName(singletonMap("a", 10), "MAP");
        testIsTypeName(asList(1, 2), "LIST OF INTEGER");
        testIsTypeName(1L, "INTEGER");
        testIsTypeName(1, "INTEGER");
        testIsTypeName(1.0D, "FLOAT");
        testIsTypeName(1.0, "FLOAT");
        testIsTypeName("a", "STRING");
        testIsTypeName(false, "BOOLEAN");
        testIsTypeName(true, "BOOLEAN");
        testIsTypeName(null, "NULL");
    }

    @Test
    public void testMetaTypes() {

        Map<String, Object> param = map(
                "MAP",
                singletonMap("a", 10),
                "LIST OF INTEGER",
                asList(1, 2),
                "INTEGER",
                1L,
                "FLOAT",
                1.0D,
                "STRING",
                "a",
                "BOOLEAN",
                true,
                "NULL",
                null);
        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types($param) AS value", singletonMap("param", param), row -> {
            Map<String, String> res = (Map) row.get("value");
            res.forEach(Assert::assertEquals);
        });
    }

    private void testTypeName(Object value, String type) {
        TestUtil.testCall(
                db,
                "RETURN apoc.meta.cypher.type($value) AS value",
                singletonMap("value", value),
                row -> assertEquals(type, row.get("value")));
    }

    @Test
    public void testVectorTypes() {
        var vectorTypes = List.of("INT64", "INT32", "INT16", "INT8", "FLOAT64", "FLOAT32");
        for (String type : vectorTypes) {
            TestUtil.testCall(
                    db,
                    """
                            CYPHER 25
                            WITH VECTOR([1, 2, 3], 3, %s) AS v
                            RETURN apoc.meta.cypher.type(v) AS value"""
                            .formatted(type),
                    row -> assertEquals("VECTOR", row.get("value")));

            TestUtil.testCall(
                    db,
                    """
                            CYPHER 25
                            WITH VECTOR([1, 2, 3], 3, %s) AS v
                            RETURN apoc.meta.cypher.isType(v, "VECTOR") AS value"""
                            .formatted(type),
                    row -> assertEquals(true, row.get("value")));
        }
    }

    private void testIsTypeName(Object value, String type) {
        TestUtil.testCall(
                db,
                "RETURN apoc.meta.cypher.isType($value,$type) AS value",
                map("value", value, "type", type),
                result -> assertEquals("type was not " + type, true, result.get("value")));
        TestUtil.testCall(
                db,
                "RETURN apoc.meta.cypher.isType($value,$type) AS value",
                map("value", value, "type", type + "foo"),
                result -> assertEquals(false, result.get("value")));
    }

    private void assertStats(String setupQuery, Map<String, Object> expected) {
        // Stats works on committed data
        db.executeTransactionally(setupQuery);
        try (final var tx = db.beginTx()) {
            assertThat(tx.execute("CALL apoc.meta.stats()").stream().toList())
                    .satisfiesExactly(row -> assertThat(row).containsExactlyInAnyOrderEntriesOf(expected));
            tx.commit();
        }

        // Stats works on uncommited data
        db.executeTransactionally("CREATE (:UnrelatedLabel)-[:UNRELATED_REL]->()");
        try (final var tx = db.beginTx()) {
            assertThat(tx.execute("MATCH (n) DETACH DELETE n").stream().toList())
                    .size()
                    .isLessThanOrEqualTo(0);
            assertThat(tx.execute(setupQuery).stream().toList()).size().isGreaterThanOrEqualTo(0);
            assertThat(tx.execute("CALL apoc.meta.stats()").stream().toList())
                    .satisfiesExactly(row -> assertThat(row).containsExactlyInAnyOrderEntriesOf(expected));
            tx.commit();
        }
    }

    private Map<String, Object> statsMap(Map<String, Object> values) {
        final var stats = values.entrySet().stream()
                .filter(e -> !"relTypesCount".equals(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return ImmutableMap.<String, Object>builder()
                .putAll(values)
                .put("stats", stats)
                .build();
    }

    @Test
    public void testMetaStats() {
        final var setup =
                "CREATE (:Actor)-[:ACTED_IN]->(:Movie), ()-[:ACTED_IN]->(:Movie), (:Actor)-[:ACTED_IN]->(), ()-[:ACTED_IN]->()";
        final var expected = statsMap(Map.of(
                "relTypeCount", 1L,
                "propertyKeyCount", 0L,
                "labelCount", 2L,
                "nodeCount", 8L,
                "relCount", 4L,
                "labels", Map.of("Movie", 2L, "Actor", 2L),
                "relTypes",
                        Map.of(
                                "()-[:ACTED_IN]->(:Movie)",
                                2L,
                                "()-[:ACTED_IN]->()",
                                4L,
                                "(:Actor)-[:ACTED_IN]->()",
                                2L),
                "relTypesCount", Map.of("ACTED_IN", 4L)));
        assertStats(setup, expected);
    }

    @Test
    public void testMetaStats2() {
        final var nodeLabels = List.of("", ":A", ":B", ":A:B");
        final var setup = new StringBuilder();
        for (final var labelsA : nodeLabels) {
            setup.append("CREATE (%s)%n".formatted(labelsA));
            for (final var labelsB : nodeLabels) {
                setup.append("CREATE (%s)-[:R1]->(%s)%n".formatted(labelsA, labelsB));
                setup.append("CREATE (%s)<-[:R2]-(%s)%n".formatted(labelsA, labelsB));
            }
        }
        final var expected = statsMap(Map.of(
                "relTypeCount", 2L,
                "propertyKeyCount", 0L,
                "labelCount", 2L,
                "nodeCount", 68L,
                "relCount", 32L,
                "labels", Map.of("A", 34L, "B", 34L),
                "relTypes",
                        Map.of(
                                "()-[:R1]->()", 16L,
                                "()-[:R2]->()", 16L,
                                "()-[:R1]->(:A)", 8L,
                                "()-[:R1]->(:B)", 8L,
                                "()-[:R2]->(:A)", 8L,
                                "()-[:R2]->(:B)", 8L,
                                "(:A)-[:R1]->()", 8L,
                                "(:A)-[:R2]->()", 8L,
                                "(:B)-[:R1]->()", 8L,
                                "(:B)-[:R2]->()", 8L),
                "relTypesCount", Map.of("R1", 16L, "R2", 16L)));
        assertStats(setup.toString(), expected);
    }

    @Test
    public void testMetaGraph() {
        db.executeTransactionally("CREATE (a:Actor)-[:ACTED_IN]->(m1:Movie),(a)-[:ACTED_IN]->(m2:Movie)");
        TestUtil.testCall(db, "CALL apoc.meta.graph()", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            Node n1 = nodes.get(0);
            assertTrue(n1.hasLabel(Label.label("Actor")));
            assertEquals(1L, n1.getProperty("count"));
            assertEquals("Actor", n1.getProperty("name"));
            Node n2 = nodes.get(1);
            assertTrue(n2.hasLabel(Label.label("Movie")));
            assertEquals("Movie", n2.getProperty("name"));
            assertEquals(2L, n2.getProperty("count"));
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            Relationship rel = rels.iterator().next();
            assertEquals("ACTED_IN", rel.getType().name());
            assertEquals(2L, rel.getProperty("count"));
        });
    }

    @Test
    public void testMetaGraph2() {
        db.executeTransactionally("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ");
        TestUtil.testCall(db, "CALL apoc.meta.graphSample()", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            Node n1 = nodes.get(0);
            assertTrue(n1.hasLabel(Label.label("Actor")));
            assertEquals(1L, n1.getProperty("count"));
            assertEquals("Actor", n1.getProperty("name"));
            Node n2 = nodes.get(1);
            assertTrue(n2.hasLabel(Label.label("Movie")));
            assertEquals("Movie", n2.getProperty("name"));
            assertEquals(1L, n1.getProperty("count"));
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            Relationship rel = rels.iterator().next();
            assertEquals("ACTED_IN", rel.getType().name());
            assertEquals(1L, rel.getProperty("count"));
        });
    }

    @Test
    public void testMetaData() {
        db.executeTransactionally("create index for (n:Movie) on (n.title)");
        db.executeTransactionally("create constraint for (a:Actor) require a.name is unique");
        db.executeTransactionally(
                """
                CREATE (actor1:Actor {name:'Tom Hanks'})-[:ACTED_IN {roles:'Forrest'}]->(movie1:Movie {title:'Forrest Gump'}),
                (actor2:Actor {name: 'Bruce Lee'})-[:ACTED_IN {roles:'FooBaz'}]->(movie1),
                (actor1)-[:ACTED_IN {roles:'Movie2Role'}]->(movie2:Movie {title:'Movie2'}), (actor1)-[:ACTED_IN {roles:'Movie3Role'}]->(movie3:Movie {title:'Movie3'}),
                (actor1)-[:DIRECTED {foo: 'first'}]->(movie2), (actor1)-[:DIRECTED {foo: 'second'}]->(:Movie {title:'Movie4'}),
                (:Studio {name: 'Pixar'})-[:ANIMATED {bar: 'alpha'}]->(movie2)""");
        TestUtil.testResult(
                db,
                """
                CALL apoc.meta.data()
                YIELD label, property, count, unique, index, existence, type, array, left, right, other, otherLabels, elementType
                RETURN * ORDER BY elementType, property""",
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals("node", row.get("elementType"));
                    assertEquals("ACTED_IN", row.get("property"));
                    assertEquals("Actor", row.get("label"));
                    assertRelationshipActedInMetaData(row);
                    row = r.next();
                    assertEquals("node", row.get("elementType"));
                    assertEquals("ANIMATED", row.get("property"));
                    assertEquals("Studio", row.get("label"));
                    assertRelationshipsAnimatedMetaData(row);
                    row = r.next();
                    assertEquals("node", row.get("elementType"));
                    assertEquals("DIRECTED", row.get("property"));
                    assertEquals("Actor", row.get("label"));
                    assertRelationshipsDirectedMetaData(row);
                    row = r.next();
                    assertEquals("node", row.get("elementType"));
                    assertPropertiesMetaData(row);
                    row = r.next();
                    assertEquals("node", row.get("elementType"));
                    assertPropertiesMetaData(row);
                    row = r.next();
                    assertEquals("node", row.get("elementType"));
                    assertPropertiesMetaData(row);
                    row = r.next();
                    assertEquals("relationship", row.get("elementType"));
                    assertEquals("ACTED_IN", row.get("label"));
                    assertEquals("Actor", row.get("property"));
                    assertRelationshipActedInMetaData(row);
                    row = r.next();
                    assertEquals("relationship", row.get("elementType"));
                    assertEquals("DIRECTED", row.get("label"));
                    assertEquals("Actor", row.get("property"));
                    assertRelationshipsDirectedMetaData(row);
                    row = r.next();
                    assertEquals("relationship", row.get("elementType"));
                    assertEquals("ANIMATED", row.get("label"));
                    assertEquals("Studio", row.get("property"));
                    assertRelationshipsAnimatedMetaData(row);
                    row = r.next();
                    assertEquals("relationship", row.get("elementType"));
                    assertPropertiesMetaData(row);
                    row = r.next();
                    assertEquals("relationship", row.get("elementType"));
                    assertPropertiesMetaData(row);
                    row = r.next();
                    assertEquals("relationship", row.get("elementType"));
                    assertPropertiesMetaData(row);
                    assertFalse(r.hasNext());
                });
    }

    private void assertRelationshipsDirectedMetaData(Map<String, Object> row) {
        assertRowMetaData(row, 1L, 2L, 0L, Types.RELATIONSHIP);
    }

    private void assertRelationshipsAnimatedMetaData(Map<String, Object> row) {
        assertRowMetaData(row, 1L, 1L, 0L, Types.RELATIONSHIP);
    }

    private void assertRelationshipActedInMetaData(Map<String, Object> row) {
        assertRowMetaData(row, 2L, 2L, 0L, Types.RELATIONSHIP);
    }

    private void assertPropertiesMetaData(Map<String, Object> row) {
        assertRowMetaData(row, 0L, 0L, 0L, Types.STRING);
    }

    private void assertRowMetaData(Map<String, Object> row, long count, long left, long right, Types type) {
        assertEquals(count, row.get("count"));
        assertEquals(left, row.get("left"));
        assertEquals(right, row.get("right"));
        assertEquals(type.name(), row.get("type"));
    }

    @Test
    public void testMetaSchema() {
        db.executeTransactionally("create index for (n:Movie) on (n.title)");
        db.executeTransactionally("create constraint for (p:Person) require p.name is unique");
        db.executeTransactionally(
                "CREATE (:Person:Actor:Director {name:'Tom', born:'05-06-1956', dead:false})-[:ACTED_IN {roles:'Forrest'}]->(:Movie {title:'Forrest Gump'})");
        testCall(db, "CALL apoc.meta.schema()", (row) -> {
            List<String> emprtyList = new ArrayList<>();
            List<String> fullList = Arrays.asList("Actor", "Director");

            Map<String, Object> o = (Map<String, Object>) row.get("value");
            assertEquals(5, o.size());

            Map<String, Object> movie = (Map<String, Object>) o.get("Movie");
            Map<String, Object> movieProperties = (Map<String, Object>) movie.get("properties");
            Map<String, Object> movieTitleProperties = (Map<String, Object>) movieProperties.get("title");
            assertNotNull(movie);
            assertEquals("node", movie.get("type"));
            assertEquals(1L, movie.get("count"));
            assertEquals(emprtyList, movie.get("labels"));
            assertEquals(4, movieTitleProperties.size());
            assertEquals("STRING", movieTitleProperties.get("type"));
            assertEquals(true, movieTitleProperties.get("indexed"));
            assertEquals(false, movieTitleProperties.get("unique"));
            Map<String, Object> movieRel = (Map<String, Object>) movie.get("relationships");
            Map<String, Object> movieActedIn = (Map<String, Object>) movieRel.get("ACTED_IN");
            assertEquals(1L, movieRel.size());
            assertEquals("in", movieActedIn.get("direction"));
            assertEquals(1L, movieActedIn.get("count"));
            assertEquals(Arrays.asList("Person", "Actor", "Director"), movieActedIn.get("labels"));

            Map<String, Object> person = (Map<String, Object>) o.get("Person");
            Map<String, Object> personProperties = (Map<String, Object>) person.get("properties");
            Map<String, Object> personNameProperty = (Map<String, Object>) personProperties.get("name");
            assertNotNull(person);
            assertEquals("node", person.get("type"));
            assertEquals(1L, person.get("count"));
            assertEquals(fullList, person.get("labels"));
            assertEquals(true, personNameProperty.get("unique"));
            assertEquals(3, personProperties.size());

            Map<String, Object> actor = (Map<String, Object>) o.get("Actor");
            assertNotNull(actor);
            assertEquals("node", actor.get("type"));
            assertEquals(1L, actor.get("count"));
            assertEquals(emprtyList, actor.get("labels"));

            Map<String, Object> director = (Map<String, Object>) o.get("Director");
            Map<String, Object> directorProperties = (Map<String, Object>) director.get("properties");
            assertNotNull(director);
            assertEquals("node", director.get("type"));
            assertEquals(1L, director.get("count"));
            assertEquals(emprtyList, director.get("labels"));
            assertEquals(3, directorProperties.size());

            Map<String, Object> actedIn = (Map<String, Object>) o.get("ACTED_IN");
            Map<String, Object> actedInProperties = (Map<String, Object>) actedIn.get("properties");
            Map<String, Object> actedInRoleProperty = (Map<String, Object>) actedInProperties.get("roles");
            assertNotNull(actedIn);
            assertEquals("relationship", actedIn.get("type"));
            assertEquals("STRING", actedInRoleProperty.get("type"));
            assertEquals(false, actedInRoleProperty.get("array"));
            assertEquals(false, actedInRoleProperty.get("existence"));
        });
    }

    @Test
    public void testMetaSchemaWithNodesAndRelsWithoutProps() {
        db.executeTransactionally(
                "CREATE (:Other), (:Other)-[:REL_1]->(:Movie)<-[:REL_2 {baz: 'baa'}]-(:Director), (:Director {alpha: 'beta'}), (:Actor {foo:'bar'}), (:Person)");
        testCall(db, "CALL apoc.meta.schema()", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertEquals(7, value.size());

            Map<String, Object> other = (Map<String, Object>) value.get("Other");
            Map<String, Object> otherProperties = (Map<String, Object>) other.get("properties");
            assertEquals(0, otherProperties.size());
            assertEquals("node", other.get("type"));
            assertEquals(2L, other.get("count"));
            Map<String, Object> Movie = (Map<String, Object>) value.get("Movie");
            Map<String, Object> movieProperties = (Map<String, Object>) Movie.get("properties");
            assertEquals(0, movieProperties.size());
            assertEquals("node", Movie.get("type"));
            assertEquals(1L, Movie.get("count"));
            Map<String, Object> director = (Map<String, Object>) value.get("Director");
            Map<String, Object> directorProperties = (Map<String, Object>) director.get("properties");
            assertEquals(1, directorProperties.size());
            assertEquals("node", director.get("type"));
            assertEquals(2L, director.get("count"));
            Map<String, Object> person = (Map<String, Object>) value.get("Person");
            Map<String, Object> personProperties = (Map<String, Object>) person.get("properties");
            assertEquals(0, personProperties.size());
            assertEquals("node", person.get("type"));
            assertEquals(1L, person.get("count"));
            Map<String, Object> actor = (Map<String, Object>) value.get("Actor");
            Map<String, Object> actorProperties = (Map<String, Object>) actor.get("properties");
            assertEquals(1, actorProperties.size());
            assertEquals("node", actor.get("type"));
            assertEquals(1L, actor.get("count"));

            Map<String, Object> rel1 = (Map<String, Object>) value.get("REL_1");
            Map<String, Object> rel1Properties = (Map<String, Object>) rel1.get("properties");
            assertEquals(0, rel1Properties.size());
            assertEquals("relationship", rel1.get("type"));
            assertEquals(1L, rel1.get("count"));
            Map<String, Object> rel2 = (Map<String, Object>) value.get("REL_2");
            Map<String, Object> rel2Properties = (Map<String, Object>) rel2.get("properties");
            assertEquals(1, rel2Properties.size());
            assertEquals("relationship", rel2.get("type"));
            assertEquals(1L, rel2.get("count"));
        });
    }

    @Test
    public void testMetaSchemaWithSmallSampleAndRelationships() {
        final List<String> labels = List.of("Other", "Foo");
        db.executeTransactionally(
                "CREATE (:Foo), (:Other)-[:REL_0]->(:Other), (:Other)-[:REL_1]->(:Other)<-[:REL_2 {baz: 'baa'}]-(:Other), (:Other {alpha: 'beta'}), (:Other {foo:'bar'})-[:REL_3]->(:Other)");
        testCall(
                db, "CALL apoc.meta.schema({sample: 2})", (row) -> ((Map<String, Map<String, Object>>) row.get("value"))
                        .forEach((key, value) -> {
                            if (labels.contains(key)) {
                                assertEquals("node", value.get("type"));
                            } else {
                                assertEquals("relationship", value.get("type"));
                            }
                        }));
    }

    @Test
    public void testIssue1861LabelAndTypeWithSameName() {
        db.executeTransactionally(
                """
                CREATE (s0 :person{id:1} ) SET s0.name = 'rose'
                CREATE (t0 :person{id:2}) SET t0.name = 'jack'
                MERGE (s0) -[r0:person {alfa: 'beta'}] -> (t0)""");
        testCall(db, "CALL apoc.meta.schema()", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            assertEquals(2, value.size());

            Map<String, Object> personRelationship = (Map<String, Object>) value.get("person (RELATIONSHIP)");
            assertEquals(1L, personRelationship.get("count"));
            assertEquals("relationship", personRelationship.get("type"));
            Map<String, Object> relationshipProps = (Map<String, Object>) personRelationship.get("properties");
            assertEquals(Set.of("alfa"), relationshipProps.keySet());

            Map<String, Object> personNode = (Map<String, Object>) value.get("person");
            assertEquals(2L, personNode.get("count"));
            assertEquals("node", personNode.get("type"));
            Map<String, Object> nodeProps = (Map<String, Object>) personNode.get("properties");
            assertEquals(Set.of("name", "id"), nodeProps.keySet());
        });
    }

    @Test
    public void testSubGraphNoLimits() {
        db.executeTransactionally("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)");
        testCall(db, "CALL apoc.meta.subGraph({})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(3, nodes.size());
            assertTrue(nodes.stream()
                    .map(n -> Iterables.first(n.getLabels()).name())
                    .allMatch(n -> n.equals("A") || n.equals("B") || n.equals("C")));
            assertEquals(2, rels.size());
            assertTrue(rels.stream().map(r -> r.getType().name()).allMatch(n -> n.equals("X") || n.equals("Y")));
        });
    }

    @Test
    public void testSubGraphNonExistingLabels() {
        db.executeTransactionally(
                """
                MATCH (n) DETACH DELETE n
                WITH COUNT(1) as foo
                CREATE (:Foo), (:Bar), (:Test) // label Test created here
                WITH COUNT(1) as foo
                MATCH (t:Test) DELETE t // node with label Test delete again
                WITH COUNT(1) as foo
                MATCH (n)
                RETURN n
                """);
        // Check for a completely new label
        testCall(db, "CALL apoc.meta.subGraph({includeLabels:['X'],rels:[],excludes:[]})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(0, nodes.size());
            assertEquals(0, rels.size());
        });
        // Check for a previous existing label
        testCall(db, "CALL apoc.meta.subGraph({includeLabels:['Test'],rels:[],excludes:[]})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(0, nodes.size());
            assertEquals(0, rels.size());
        });
    }

    @Test
    public void testSubGraphNonExistingTypes() {
        db.executeTransactionally(
                """
                MATCH (n) DETACH DELETE n
                WITH COUNT(1) as foo
                CREATE (:Foo)-[:R]->(:Bar)-[:Rtest]->(:Test)
                WITH COUNT(1) as foo
                MATCH (t:Test) DETACH DELETE t
                WITH COUNT(1) as foo
                MATCH (n)
                RETURN n
                """);
        // Check for a completely new type
        testCall(db, "CALL apoc.meta.subGraph({includeLabels:[],includeRels:['X'],excludes:[]})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertEquals(0, rels.size());
        });
        // Check for a previous existing type
        testCall(db, "CALL apoc.meta.subGraph({includeLabels:[],includeRels:['Rtest'],excludes:[]})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertEquals(0, rels.size());
        });
    }

    @Test
    public void testSubGraphLimitLabels() {
        final String labels = "labels";
        testSubgraphLabelsCommon(labels);
    }

    private void testSubgraphLabelsCommon(String labels) {
        db.executeTransactionally("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)");
        testCall(db, "CALL apoc.meta.subGraph($conf)", map("conf", map(labels, List.of("A", "B"))), (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertTrue(nodes.stream()
                    .map(n -> Iterables.first(n.getLabels()).name())
                    .allMatch(n -> n.equals("A") || n.equals("B")));
            assertEquals(1, rels.size());
            assertTrue(rels.stream().map(r -> r.getType().name()).allMatch(n -> n.equals("X")));
        });
    }

    @Test
    public void testSubGraphWithIncludeLabels() {
        final String labels = "includeLabels";
        testSubgraphLabelsCommon(labels);
    }

    @Test
    public void testSubGraphLimitWithRels() {
        final String relsConf = "rels";
        assertMetaSubgraphCommon(relsConf);
    }

    @Test
    public void testSubGraphLimitWithIncludeRels() {
        final String relsConf = "includeRels";
        assertMetaSubgraphCommon(relsConf);
    }

    private void assertMetaSubgraphCommon(String relsConf) {
        final Consumer<Map<String, Object>> consumer = (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(3, nodes.size());
            assertTrue(nodes.stream()
                    .map(n -> Iterables.first(n.getLabels()).name())
                    .allMatch(n -> n.equals("A") || n.equals("B") || n.equals("C")));
            assertEquals(1, rels.size());
            assertTrue(rels.stream().map(r -> r.getType().name()).allMatch(n -> n.equals("X")));
        };
        final Map<String, Object> conf = map(relsConf, List.of("X"));
        testGraphCommon(conf, consumer);
    }

    @Test
    public void testSubGraphExcludes() {
        final String relsConf = "excludes";
        testExcludeLabelsCommon(relsConf);
    }

    @Test
    public void testSubGraphExcludesLabels() {
        final String relsConf = "excludeLabels";
        testExcludeLabelsCommon(relsConf);
    }

    private void testGraphCommon(Map<String, Object> conf, Consumer<Map<String, Object>> consumer) {
        db.executeTransactionally("CREATE (:A)-[:X]->(b:B),(b)-[:Y]->(:C)");
        testCall(db, "CALL apoc.meta.subGraph($conf)", map("conf", conf), consumer);
    }

    private void testExcludeLabelsCommon(String relsConf) {
        final Consumer<Map<String, Object>> consumer = (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> rels = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertTrue(nodes.stream()
                    .map(n -> Iterables.first(n.getLabels()).name())
                    .allMatch(n -> n.equals("A") || n.equals("C")));
            assertEquals(0, rels.size());
        };
        final Map<String, Object> conf = map(relsConf, List.of("B"));
        testGraphCommon(conf, consumer);
    }

    @Test
    public void testMetaSubgraphBothIncludeAndExclude() {
        final Consumer<Map<String, Object>> consumer = (row) -> {
            assertEquals(Collections.emptyList(), row.get("nodes"));
            assertEquals(Collections.emptyList(), row.get("relationships"));
        };
        final Map<String, Object> conf = map("excludeLabels", List.of("B"), "includeLabels", List.of("B"));
        testGraphCommon(conf, consumer);
    }

    @Test
    public void testMetaDate() {

        Map<String, Object> param = map(
                "DATE", DateValue.now(Clock.systemDefaultZone()),
                "LOCAL_DATE", LocalDateTimeValue.now(Clock.systemDefaultZone()),
                "TIME", TimeValue.now(Clock.systemDefaultZone()),
                "LOCAL_TIME", LocalTimeValue.now(Clock.systemDefaultZone()),
                "DATE_TIME", DateTimeValue.now(Clock.systemDefaultZone()),
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types($param) AS value", singletonMap("param", param), row -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");

            assertEquals("DATE", r.get("DATE"));
            assertEquals("LOCAL_DATE_TIME", r.get("LOCAL_DATE"));
            assertEquals("TIME", r.get("TIME"));
            assertEquals("LOCAL_TIME", r.get("LOCAL_TIME"));
            assertEquals("DATE_TIME", r.get("DATE_TIME"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMetaArray() {

        Map<String, Object> param = map(
                "ARRAY", new String[] {"a", "b", "c"},
                "ARRAY_FLOAT", new Float[] {1.2f, 2.2f},
                "ARRAY_DOUBLE", new Double[] {1.2, 2.2},
                "ARRAY_INT", new Integer[] {1, 2},
                "ARRAY_OBJECT", new Object[] {1, "a"},
                "ARRAY_POINT",
                        new Object[] {
                            Values.pointValue(CoordinateReferenceSystem.WGS_84, 56.d, 12.78),
                            Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 56.d, 12.78, 100)
                        },
                "ARRAY_DURATION",
                        new Object[] {
                            isoDuration(5, 1, 43200, 0).asIsoDuration(),
                            isoDuration(2, 1, 125454, 0).asIsoDuration()
                        },
                "ARRAY_ARRAY",
                        new Object[] {
                            1,
                            "a",
                            new Object[] {"a", 1},
                            isoDuration(5, 1, 43200, 0).asIsoDuration()
                        },
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types($param) AS value", singletonMap("param", param), row -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");

            assertEquals("LIST OF STRING", r.get("ARRAY"));
            assertEquals("LIST OF FLOAT", r.get("ARRAY_FLOAT"));
            assertEquals("LIST OF FLOAT", r.get("ARRAY_DOUBLE"));
            assertEquals("LIST OF INTEGER", r.get("ARRAY_INT"));
            assertEquals("LIST OF ANY", r.get("ARRAY_OBJECT"));
            assertEquals("LIST OF POINT", r.get("ARRAY_POINT"));
            assertEquals("LIST OF DURATION", r.get("ARRAY_DURATION"));
            assertEquals("LIST OF ANY", r.get("ARRAY_ARRAY"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMetaNumber() {

        Map<String, Object> param = map("INTEGER", 1L, "FLOAT", 1.0f, "DOUBLE", 1.0D, "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types($param) AS value", singletonMap("param", param), row -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");

            assertEquals("INTEGER", r.get("INTEGER"));
            assertEquals("FLOAT", r.get("FLOAT"));
            assertEquals("FLOAT", r.get("DOUBLE"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMeta() {

        Map<String, Object> param = map(
                "LIST", asList(1.2, 2.1),
                "STRING", "a",
                "BOOLEAN", true,
                "CHAR", 'a',
                "DURATION", 'a',
                "POINT_2D", Values.pointValue(CoordinateReferenceSystem.WGS_84, 56.d, 12.78),
                "POINT_3D", Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 56.7, 12.78, 100.0),
                "POINT_XYZ_2D", Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.3, 4.5),
                "POINT_XYZ_3D", Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 2.3, 4.5, 1.2),
                "DURATION", isoDuration(5, 1, 43200, 0).asIsoDuration(),
                "MAP", Util.map("a", "b"),
                "NULL", null);

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types($param) AS value", singletonMap("param", param), row -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");

            assertEquals("LIST OF FLOAT", r.get("LIST"));
            assertEquals("STRING", r.get("STRING"));
            assertEquals("BOOLEAN", r.get("BOOLEAN"));
            assertEquals("Character", r.get("CHAR"));
            assertEquals("POINT", r.get("POINT_2D"));
            assertEquals("POINT", r.get("POINT_3D"));
            assertEquals("POINT", r.get("POINT_XYZ_2D"));
            assertEquals("POINT", r.get("POINT_XYZ_3D"));
            assertEquals("DURATION", r.get("DURATION"));
            assertEquals("MAP", r.get("MAP"));
            assertEquals("NULL", r.get("NULL"));
        });
    }

    @Test
    public void testMetaList() {

        Map<String, Object> param = map(
                "LIST FLOAT", asList(1.2F, 2.1F),
                "LIST STRING", asList("a", "b"),
                "LIST CHAR", asList('a', 'a'),
                "LIST DATE", asList(LocalDate.of(2018, 1, 1), LocalDate.of(2018, 2, 2)),
                "LIST ANY", asList("test", 1, "asd", isoDuration(5, 1, 43200, 0).asIsoDuration()),
                "LIST NULL", asList("test", null),
                "LIST POINT",
                        asList(
                                Values.pointValue(CoordinateReferenceSystem.WGS_84, 56.d, 12.78),
                                Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 2.3, 4.5, 1.2)),
                "LIST DURATION",
                        asList(
                                isoDuration(5, 1, 43200, 0).asIsoDuration(),
                                isoDuration(2, 1, 125454, 0).asIsoDuration()),
                "LIST OBJECT", new Object[] {LocalDate.of(2018, 1, 1), "test"},
                "LIST OF LIST", asList(asList("a", "b", "c"), asList("aa", "bb", "cc"), asList("aaa", "bbb", "ccc")),
                "LIST DOUBLE", asList(1.2D, 2.1D));

        TestUtil.testCall(db, "RETURN apoc.meta.cypher.types($param) AS value", singletonMap("param", param), row -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");

            assertEquals("LIST OF FLOAT", r.get("LIST FLOAT"));
            assertEquals("LIST OF STRING", r.get("LIST STRING"));
            assertEquals("LIST OF ANY", r.get("LIST CHAR"));
            assertEquals("LIST OF DATE", r.get("LIST DATE"));
            assertEquals("LIST OF FLOAT", r.get("LIST DOUBLE"));
            assertEquals("LIST OF POINT", r.get("LIST POINT"));
            assertEquals("LIST OF DURATION", r.get("LIST DURATION"));
            assertEquals("LIST OF ANY", r.get("LIST ANY"));
            assertEquals("LIST OF ANY", r.get("LIST OBJECT"));
            assertEquals("LIST OF LIST", r.get("LIST OF LIST"));
            assertEquals("LIST OF ANY", r.get("LIST NULL"));
        });
    }

    @Test
    public void testMetaPoint() {
        db.executeTransactionally("CREATE (:TEST {born:point({ longitude: 56.7, latitude: 12.78, height: 100 })})");

        TestUtil.testCall(
                db,
                "MATCH (t:TEST) WITH t.born as born RETURN apoc.meta.cypher.type(born) AS value",
                row -> assertEquals("POINT", row.get("value")));
    }

    @Test
    public void testMetaDuration() {
        db.executeTransactionally("CREATE (:TEST {duration:duration('P5M1DT12H')})");

        TestUtil.testCall(
                db,
                "MATCH (t:TEST) WITH t.duration as duration RETURN apoc.meta.cypher.type(duration) AS value",
                row -> assertEquals("DURATION", row.get("value")));
    }

    @Test
    public void testMetaDataWithSample() {
        db.executeTransactionally("create index for (n:Person) on (n.name)");
        db.executeTransactionally("CREATE (:Person {name:'Tom'})");
        db.executeTransactionally("CREATE (:Person {name:'John', surname:'Brown'})");
        db.executeTransactionally("CREATE (:Person {name:'Nick'})");
        db.executeTransactionally("CREATE (:Person {name:'Daisy', surname:'Bob'})");
        db.executeTransactionally("CREATE (:Person {name:'Elizabeth'})");
        db.executeTransactionally("CREATE (:Person {name:'Jack', surname:'White'})");
        db.executeTransactionally("CREATE (:Person {name:'Joy'})");
        db.executeTransactionally("CREATE (:Person {name:'Sarah', surname:'Taylor'})");
        db.executeTransactionally("CREATE (:Person {name:'Jane'})");
        db.executeTransactionally("CREATE (:Person {name:'Jeff', surname:'Logan'})");
        TestUtil.testResult(db, "CALL apoc.meta.data({sample:2})", (r) -> assertThat(
                        r.stream().map(m -> m.get("property")))
                .containsExactlyInAnyOrder("name", "surname"));
    }

    @Test
    public void testMetaDataWithSampleNormalized() {
        db.executeTransactionally("create index for (n:Person) on (n.name)");
        db.executeTransactionally("CREATE (:Person {name:'Tom'})");
        db.executeTransactionally("CREATE (:Person {name:'John'})");
        db.executeTransactionally("CREATE (:Person {name:'Nick'})");
        db.executeTransactionally("CREATE (:Person {name:'Daisy', surname:'Bob'})");
        db.executeTransactionally("CREATE (:Person {name:'Elizabeth'})");
        db.executeTransactionally("CREATE (:Person {name:'Jack'})");
        db.executeTransactionally("CREATE (:Person {name:'Joy'})");
        db.executeTransactionally("CREATE (:Person {name:'Sarah'})");
        db.executeTransactionally("CREATE (:Person {name:'Jane'})");
        db.executeTransactionally("CREATE (:Person {name:'Jeff', surname:'Logan'})");
        db.executeTransactionally("CREATE (:City {name:'Milano'})");
        db.executeTransactionally("CREATE (:City {name:'Roma'})");
        db.executeTransactionally("CREATE (:City {name:'Firenze'})");
        db.executeTransactionally("CREATE (:City {name:'Taormina', region:'Sicilia'})");
        TestUtil.testResult(db, "CALL apoc.meta.data({sample:5})", (r) -> {
            Map<String, Object> personNameProperty = r.next();
            Map<String, Object> personSurnameProperty = r.next();
            assertEquals("Person", personNameProperty.get("label"));
            assertEquals("name", personNameProperty.get("property"));
            assertEquals("Person", personSurnameProperty.get("label"));
            assertEquals("surname", personSurnameProperty.get("property"));

            Map<String, Object> cityNameProperty = r.next();
            Map<String, Object> cityRegionProperty = r.next();
            assertEquals("City", cityNameProperty.get("label"));
            assertEquals("name", cityNameProperty.get("property"));
            assertEquals("City", cityRegionProperty.get("label"));
            assertEquals("region", cityRegionProperty.get("property"));
        });
    }

    @Test
    public void testRelationshipAndNodeNames() {
        db.executeTransactionally("CREATE (a:NODE)-[r:RELATIONSHIP]->(m:Movie)");
        TestUtil.testResult(db, "CALL apoc.meta.data()", (r) -> {
            assertThat(r.stream().map(m -> m.get("label"))).contains("RELATIONSHIP", "NODE");
            r.close();
        });
    }

    @Test
    public void testMetaDataWithSample5() {
        db.executeTransactionally("create index for (n:Person) on (n.name)");
        db.executeTransactionally("CREATE (:Person {name:'John', surname:'Brown'})");
        db.executeTransactionally("CREATE (:Person {name:'Daisy', surname:'Bob'})");
        db.executeTransactionally("CREATE (:Person {name:'Nick'})");
        db.executeTransactionally("CREATE (:Person {name:'Jack', surname:'White'})");
        db.executeTransactionally("CREATE (:Person {name:'Elizabeth'})");
        db.executeTransactionally("CREATE (:Person {name:'Joy'})");
        db.executeTransactionally("CREATE (:Person {name:'Sarah', surname:'Taylor'})");
        db.executeTransactionally("CREATE (:Person {name:'Jane'})");
        db.executeTransactionally("CREATE (:Person {name:'Jeff', surname:'Logan'})");
        db.executeTransactionally("CREATE (:Person {name:'Tom'})");
        TestUtil.testResult(db, "CALL apoc.meta.data({sample:5})", (r) -> {
            assertThat(r.stream().map(m -> m.get("property"))).contains("name");
            r.close();
        });
    }

    @Test
    public void testSchemaWithSample() {
        db.executeTransactionally("create constraint for (p:Person) require p.name is unique");
        db.executeTransactionally("CREATE (:Person {name:'Tom'})");
        db.executeTransactionally("CREATE (:Person {name:'John', surname:'Brown'})");
        db.executeTransactionally("CREATE (:Person {name:'Nick'})");
        db.executeTransactionally("CREATE (:Person {name:'Daisy', surname:'Bob'})");
        db.executeTransactionally("CREATE (:Person {name:'Elizabeth'})");
        db.executeTransactionally("CREATE (:Person {name:'Jack', surname:'White'})");
        db.executeTransactionally("CREATE (:Person {name:'Joy'})");
        db.executeTransactionally("CREATE (:Person {name:'Sarah', surname:'Taylor'})");
        db.executeTransactionally("CREATE (:Person {name:'Jane'})");
        db.executeTransactionally("CREATE (:Person {name:'Jeff', surname:'Logan'})");
        testCall(db, "CALL apoc.meta.schema({sample:2})", (row) -> {
            Map<String, Object> o = (Map<String, Object>) row.get("value");
            assertEquals(1, o.size());

            Map<String, Object> person = (Map<String, Object>) o.get("Person");
            Map<String, Object> personProperties = (Map<String, Object>) person.get("properties");
            Map<String, Object> personNameProperty = (Map<String, Object>) personProperties.get("name");
            Map<String, Object> personSurnameProperty = (Map<String, Object>) personProperties.get("surname");
            assertNotNull(person);
            assertEquals("node", person.get("type"));
            assertEquals(10L, person.get("count"));
            assertEquals("STRING", personNameProperty.get("type"));
            assertEquals(false, personSurnameProperty.get("unique"));
            assertEquals("STRING", personSurnameProperty.get("type"));
            assertEquals(2, personProperties.size());
        });
    }

    @Test
    public void testSchemaWithSample5() {
        db.executeTransactionally("create constraint for (p:Person) require p.name is unique");
        db.executeTransactionally("CREATE (:Person {name:'Tom'})");
        db.executeTransactionally("CREATE (:Person {name:'John', surname:'Brown'})");
        db.executeTransactionally("CREATE (:Person {name:'Nick'})");
        db.executeTransactionally("CREATE (:Person {name:'Daisy', surname:'Bob'})");
        db.executeTransactionally("CREATE (:Person {name:'Elizabeth'})");
        db.executeTransactionally("CREATE (:Person {name:'Jack', surname:'White'})");
        db.executeTransactionally("CREATE (:Person {name:'Joy'})");
        db.executeTransactionally("CREATE (:Person {name:'Sarah', surname:'Taylor'})");
        db.executeTransactionally("CREATE (:Person {name:'Jane'})");
        db.executeTransactionally("CREATE (:Person {name:'Jeff', surname:'Logan'})");
        testCall(db, "CALL apoc.meta.schema({sample:5})", (row) -> {
            Map<String, Object> o = (Map<String, Object>) row.get("value");
            assertEquals(1, o.size());
            Map<String, Object> person = (Map<String, Object>) o.get("Person");
            Map<String, Object> personProperties = (Map<String, Object>) person.get("properties");
            Map<String, Object> personNameProperty = (Map<String, Object>) personProperties.get("name");
            assertNotNull(person);
            assertEquals("node", person.get("type"));
            assertEquals(10L, person.get("count"));
            assertEquals("STRING", personNameProperty.get("type"));
            assertEquals(true, personNameProperty.get("unique"));
            assertTrue(personProperties.size() >= 1);
        });
    }

    @Test
    public void testMetaGraphExtraRelsWithSample() {
        db.executeTransactionally("CREATE (:S1 {name:'Tom'})");
        db.executeTransactionally("CREATE (:S2 {name:'John', surname:'Brown'})-[:KNOWS{since:2012}]->(:S7)");
        db.executeTransactionally("CREATE (:S1 {name:'Nick'})");
        db.executeTransactionally("CREATE (:S3 {name:'Daisy', surname:'Bob'})-[:KNOWS{since:2012}]->(:S7)");
        db.executeTransactionally("CREATE (:S1 {name:'Elizabeth'})");
        db.executeTransactionally("CREATE (:S4 {name:'Jack', surname:'White'})-[:KNOWS{since:2012}]->(:S7)");
        db.executeTransactionally("CREATE (:S1 {name:'Joy'})");
        db.executeTransactionally("CREATE (:S5 {name:'Sarah', surname:'Taylor'})-[:KNOWS{since:2012}]->(:S7)");
        db.executeTransactionally("CREATE (:S1 {name:'Jane'})");
        db.executeTransactionally("CREATE (:S6 {name:'Jeff', surname:'Logan'})-[:KNOWS{since:2012}]->(:S7)");

        testCall(db, "call apoc.meta.graph({sample:2})", (row) -> {
            List<Node> nodes = (List<Node>) row.get("nodes");
            assertEquals(7, nodes.size());
        });
    }

    // Tests for T4L

    @Test
    public void testRelTypePropertiesBasic() {
        db.executeTransactionally("CREATE (:Base)-[:RELTYPE { a: 1, d: null }]->(:Target)");
        db.executeTransactionally("CREATE (:Base)-[:RELTYPE { a: 2, b: 2, c: 2, d: 4 }]->(:Target);");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties()", r -> {
            List<Map<String, Object>> records = gatherRecords(r);

            assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("propertyName").equals("a")
                            && ((List) m.get("propertyTypes")).get(0).equals("Long")
                            && m.get("mandatory").equals(false)));

            assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("propertyName").equals("b")
                            && ((List) m.get("propertyTypes")).get(0).equals("Long")
                            && m.get("mandatory").equals(false)));

            assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("propertyName").equals("c")
                            && ((List) m.get("propertyTypes")).get(0).equals("Long")
                            && m.get("mandatory").equals(false)));

            assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("propertyName").equals("d")
                            && ((List) m.get("propertyTypes")).get(0).equals("Long")
                            && m.get("mandatory").equals(false)));
        });
    }

    @Test
    public void testRelTypePropertiesIncludes() {
        db.executeTransactionally("CREATE (:A)-[:CATCHME { c: 1 }]->(:B)");
        db.executeTransactionally("CREATE (:A)-[:IGNOREME { d: 1 }]->(:B)");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeRels: ['CATCHME'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertTrue(records.get(0).get("propertyName").equals("c"));
        });
    }

    @Test
    public void testRelTypePropertiesIncludesWithDoubleRel() {
        db.executeTransactionally("CREATE (a:A)-[:FIRST_A {a: 1}]->(b:B), (a)-[:SECOND_B {b: '2'}]->(c)");
        db.executeTransactionally(
                "CREATE (:Alpha)-[:FIRST_A {c: true}]->(:Beta), (:Gamma)-[:SECOND_B {d: datetime()}]->(:Delta)");

        final Consumer<Result> assertFirstRel = res -> {
            Map<String, Object> r = res.next();
            assertEquals("a", r.get("propertyName"));
            assertEquals(":`FIRST_A`", r.get("relType"));
            assertEquals(List.of("Long"), r.get("propertyTypes"));
            r = res.next();
            assertEquals("c", r.get("propertyName"));
            assertEquals(":`FIRST_A`", r.get("relType"));
            assertEquals(List.of("Boolean"), r.get("propertyTypes"));
            assertFalse(res.hasNext());
        };

        final Consumer<Result> assertSecondRel = res -> {
            Map<String, Object> r = res.next();
            assertEquals("b", r.get("propertyName"));
            assertEquals(":`SECOND_B`", r.get("relType"));
            assertEquals(List.of("String"), r.get("propertyTypes"));
            r = res.next();
            assertEquals("d", r.get("propertyName"));
            assertEquals(":`SECOND_B`", r.get("relType"));
            assertEquals(List.of("DateTime"), r.get("propertyTypes"));
            assertFalse(res.hasNext());
        };

        final String query =
                "CALL apoc.meta.relTypeProperties($conf) YIELD propertyName, relType, propertyTypes RETURN * ORDER BY relType";

        testResult(db, query, map("conf", map("includeRels", List.of("FIRST_A"))), assertFirstRel);
        testResult(db, query, map("conf", map("excludeRels", List.of("SECOND_B"))), assertFirstRel);

        testResult(db, query, map("conf", map("excludeRels", List.of("FIRST_A"))), assertSecondRel);
        testResult(db, query, map("conf", map("includeRels", List.of("SECOND_B"))), assertSecondRel);

        TestUtil.testCallCount(db, "CALL apoc.meta.relTypeProperties()", emptyMap(), 4);
    }

    @Test
    public void testNodeTypePropertiesNodeExcludes() {
        db.executeTransactionally("CREATE (:ExcludeMe)");
        db.executeTransactionally("CREATE (:IncludeMe)");

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ excludeLabels: ['ExcludeMe'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(":`IncludeMe`", records.get(0).get("nodeType"));
        });
    }

    @Test
    public void testNodeTypePropertiesNodeIncludes() {
        db.executeTransactionally("CREATE (:ExcludeMe)");
        db.executeTransactionally("CREATE (:IncludeMe)");

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ includeLabels: ['IncludeMe'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(":`IncludeMe`", records.get(0).get("nodeType"));
        });
    }

    @Test
    public void testNodeTypePropertiesRelExcludes() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ excludeRels: ['RELA'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(2, records.size());
            for (Map<String, Object> rec : records) {
                if (rec.get("nodeType").equals(":`A`")) {
                    assertEquals(":`B`", rec.get("nodeType"));
                }
                if (rec.get("nodeType").equals(":`C`")) {
                    assertEquals(":`D`", rec.get("nodeType"));
                }
            }
        });
    }

    @Test
    public void testNodeTypePropertiesRelIncludes() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ includeRels: ['RELA'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(2, records.size());
            for (Map<String, Object> rec : records) {
                if (rec.get("nodeType").equals(":`A`")) {
                    assertEquals(":`A`", rec.get("nodeType"));
                }
                if (rec.get("nodeType").equals(":`C`")) {
                    assertEquals(":`C`", rec.get("nodeType"));
                }
            }
        });
    }

    @Test
    public void testNodeTypePropertiesWithWeirdConfig() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(
                db, "CALL apoc.meta.nodeTypeProperties({ includeRels: ['RELA'], stupidInput: ['RELB'] })", r -> {
                    // should ignore all unknown input{
                    List<Map<String, Object>> records = gatherRecords(r);
                    assertEquals(2, records.size());
                    for (Map<String, Object> rec : records) {
                        if (rec.get("nodeType").equals(":`A`")) {
                            assertEquals(":`A`", rec.get("nodeType"));
                        }
                        if (rec.get("nodeType").equals(":`C`")) {
                            assertEquals(":`C`", rec.get("nodeType"));
                        }
                    }
                });
    }

    @Test
    public void testRelTypePropertiesWithWeirdConfig() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeLabels: ['A'], stupidInput: ['B'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                if (rec.get("relType").equals(":`RELA`")) {
                    assertEquals(":`RELA`", rec.get("relType"));
                }
            }
        });
    }

    @Test
    public void testRelTypePropertiesRelExcludes() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ excludeRels: ['RELA'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(":`RELB`", records.get(0).get("relType"));
        });
    }

    @Test
    public void testRelTypePropertiesRelIncludes() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeRels: ['RELA'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            assertEquals(":`RELA`", records.get(0).get("relType"));
        });
    }

    @Test
    public void testRelTypePropertiesNodeExcludes() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ excludeLabels: ['A'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                if (rec.get("relType").equals(":`RELB`")) {
                    assertEquals(":`RELB`", rec.get("relType"));
                }
            }
        });
    }

    @Test
    public void testRelTypePropertiesNodeIncludes() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties({ includeLabels: ['A'] })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                if (rec.get("relType").equals(":`RELA`")) {
                    assertEquals(":`RELA`", rec.get("relType"));
                }
            }
        });
    }

    @Test
    public void testNodeTypePropertiesNodeIncludesRelIncludes1() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        // both together contract the data model and it should result in 0 results
        TestUtil.testResult(
                db,
                "CALL apoc.meta.nodeTypeProperties({ includeLabels: ['A'], includeRels: ['RELB'] })",
                r -> assertEquals(0, gatherRecords(r).size()));
    }

    @Test
    public void testNodeTypePropertiesNodeIncludesRelIncludes2() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(
                db, "CALL apoc.meta.nodeTypeProperties({ includeLabels: ['A'], includeRels: ['RELA'] })", r -> {
                    List<Map<String, Object>> records = gatherRecords(r);
                    assertEquals(1, records.size()); // why not A and C? The label has to be on the start of the rel
                    for (Map<String, Object> rec : records) {
                        if (rec.get("nodeType").equals(":`A`")) {
                            assertEquals(":`A`", rec.get("nodeType"));
                        }
                    }
                });
    }

    @Test
    public void testRelTypePropertiesNodeIncludesAndRelsInclude1() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        // both together contract the data model and it should result in 0 results
        TestUtil.testResult(
                db, "CALL apoc.meta.relTypeProperties({ includeLabels: ['A'], includeRels: ['RELB'] })", r -> {
                    assertEquals(0, gatherRecords(r).size());
                });
    }

    @Test
    public void testRelTypePropertiesNodeIncludesAndRelsInclude2() {
        db.executeTransactionally("CREATE (:A)-[:RELA { x: 1 }]->(:C)");
        db.executeTransactionally("CREATE (:B)-[:RELB { x: 1 }]->(:D)");

        TestUtil.testResult(
                db, "CALL apoc.meta.relTypeProperties({ includeLabels: ['A'], includeRels: ['RELA'] })", r -> {
                    List<Map<String, Object>> records = gatherRecords(r);
                    assertEquals(1, records.size());
                    for (Map<String, Object> rec : records) {
                        if (rec.get("relType").equals(":`RELA`")) {
                            assertEquals(":`RELA`", rec.get("relType"));
                        }
                    }
                });
    }

    @Test
    public void testNodeTypePropertiesCompleteResult() {
        db.executeTransactionally("CREATE (:Foo { z: 'hej' });");
        db.executeTransactionally("CREATE (:Foo { z: 1 });");

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties()", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                assertEquals(":`Foo`", rec.get("nodeType"));
                assertEquals(List.of("Foo"), rec.get("nodeLabels"));
                assertEquals("z", rec.get("propertyName"));
                assertEquals(List.of("Long", "String"), rec.get("propertyTypes"));
                assertEquals(2L, rec.get("propertyObservations"));
                assertEquals(2L, rec.get("totalObservations"));
                assertEquals(false, rec.get("mandatory"));
            }
        });
    }

    @Test
    public void testVectorTypesOnProperties() {
        var vectorTypes = List.of("INT64", "INT32", "INT16", "INT8", "FLOAT64", "FLOAT32");
        for (String type : vectorTypes) {
            db.executeTransactionally("CYPHER 25 CREATE (:Foo { z: VECTOR([1, 2, 3], 3, %s) });".formatted(type));
        }

        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties()", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                assertEquals(":`Foo`", rec.get("nodeType"));
                assertEquals(List.of("Foo"), rec.get("nodeLabels"));
                assertEquals("z", rec.get("propertyName"));
                assertEquals(
                        List.of(
                                "Float32Vector",
                                "Float64Vector",
                                "Int16Vector",
                                "Int32Vector",
                                "Int64Vector",
                                "Int8Vector"),
                        rec.get("propertyTypes"));
                assertEquals(6L, rec.get("propertyObservations"));
                assertEquals(6L, rec.get("totalObservations"));
                assertEquals(false, rec.get("mandatory"));
            }
        });

        for (String type : vectorTypes) {
            db.executeTransactionally(
                    "CYPHER 25 CREATE (:A)-[:Foo { z: VECTOR([1, 2, 3], 3, %s) }]->(:B);".formatted(type));
        }

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties()", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                assertEquals(":`Foo`", rec.get("relType"));
                assertEquals(List.of("A"), rec.get("sourceNodeLabels"));
                assertEquals(List.of("B"), rec.get("targetNodeLabels"));
                assertEquals("z", rec.get("propertyName"));
                assertEquals(
                        List.of(
                                "Float32Vector",
                                "Float64Vector",
                                "Int16Vector",
                                "Int32Vector",
                                "Int64Vector",
                                "Int8Vector"),
                        rec.get("propertyTypes"));
                assertEquals(6L, rec.get("propertyObservations"));
                assertEquals(6L, rec.get("totalObservations"));
                assertEquals(false, rec.get("mandatory"));
            }
        });
    }

    @Test
    public void testRelTypePropertiesCompleteResult() {
        db.executeTransactionally("CREATE (:A)-[:Foo { z: 'hej' }]->(:B);");
        db.executeTransactionally("CREATE (:A)-[:Foo { z: 1 }]->(:B);");

        TestUtil.testResult(db, "CALL apoc.meta.relTypeProperties()", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                assertEquals(":`Foo`", rec.get("relType"));
                assertEquals(List.of("A"), rec.get("sourceNodeLabels"));
                assertEquals(List.of("B"), rec.get("targetNodeLabels"));
                assertEquals("z", rec.get("propertyName"));
                assertEquals(List.of("Long", "String"), rec.get("propertyTypes"));
                assertEquals(2L, rec.get("propertyObservations"));
                assertEquals(2L, rec.get("totalObservations"));
                assertEquals(false, rec.get("mandatory"));
            }
        });
    }

    @Test
    public void testNodeTypePropertiesWithSpecialSampleSize() {
        db.executeTransactionally("CREATE (:Foo { z: 'hej' });");
        db.executeTransactionally("CREATE (:Foo { z: 1 });");
        db.executeTransactionally("CREATE (:Foo { z: true });");
        db.executeTransactionally("CREATE (:Foo { z: 1.5 });");

        // sample = -1 scans all entities
        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ pollingPeriod: -1})", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                assertEquals(":`Foo`", rec.get("nodeType"));
                assertEquals("z", rec.get("propertyName"));
                assertEquals(4L, rec.get("propertyObservations"));
                assertEquals(4L, rec.get("totalObservations"));
            }
        });

        // not scan all of them, might not reach maxSampleSize
        TestUtil.testResult(db, "CALL apoc.meta.nodeTypeProperties({ pollingPeriod: 1 })", r -> {
            List<Map<String, Object>> records = gatherRecords(r);
            assertEquals(1, records.size());
            for (Map<String, Object> rec : records) {
                assertEquals(":`Foo`", rec.get("nodeType"));
                assertEquals("z", rec.get("propertyName"));
                assertTrue((long) rec.get("propertyObservations") <= 4L);
                assertTrue((long) rec.get("totalObservations") <= 4L);
            }
        });
    }

    @Test
    public void testNodeTypePropertiesEquivalenceAdvanced() {
        db.executeTransactionally("CREATE (:Foo { l: 1, s: 'foo', d: datetime(), ll: ['a', 'b'], dl: [2.0, 3.0] });");
        // Missing all properties to make everything non-mandatory.
        db.executeTransactionally("CREATE (:Foo { z: 1 });");
        assertTrue(testDBCallEquivalence(
                db, "CALL apoc.meta.nodeTypeProperties()", "CYPHER 5 CALL db.schema.nodeTypeProperties()"));
    }

    @Test
    public void testRelTypePropertiesEquivalenceAdvanced() {
        db.executeTransactionally(
                "CREATE (:Foo)-[:REL { l: 1, s: 'foo', d: datetime(), ll: ['a', 'b'], dl: [2.0, 3.0] }]->();");
        // Missing all properties to make everything non-mandatory.
        db.executeTransactionally("CREATE (:Foo)-[:REL { z: 1 }]->();");
        assertTrue(testDBCallEquivalence(
                db, "CALL apoc.meta.relTypeProperties()", "CYPHER 5 CALL db.schema.relTypeProperties()"));
    }

    @Test
    public void testNodeTypePropertiesEquivalenceTypeMapping() {
        String q = "CREATE (:Test {" + "    longProp: 1,"
                + "    doubleProp: 3.14,"
                + "    stringProp: 'Hello',"
                + "    longArrProp: [1,2,3],"
                + "    doubleArrProp: [3.14, 3.14],"
                + "    stringArrProp: ['Hello', 'World'],"
                + "    dateTimeProp: datetime(),"
                + "    dateProp: date(),"
                + "    pointProp: point({ x:0, y:4, z:1 }),"
                + "    pointArrProp: [point({ x:0, y:4, z:1 }), point({ x:0, y:4, z:1 })],"
                + "    boolProp: true,"
                + "    boolArrProp: [true, false]\n"
                + "})"
                + "CREATE (:Test { randomProp: 'this property is here to make everything mandatory = false'});";

        db.executeTransactionally(q);
        assertTrue(testDBCallEquivalence(
                db, "CALL apoc.meta.nodeTypeProperties()", "CYPHER 5 CALL db.schema.nodeTypeProperties()"));
    }

    @Test
    public void testRelTypePropertiesEquivalenceTypeMapping() {
        String q = "CREATE (t:Test)-[:REL{" + "    longProp: 1,"
                + "    doubleProp: 3.14,"
                + "    stringProp: 'Hello',"
                + "    longArrProp: [1,2,3],"
                + "    doubleArrProp: [3.14, 3.14],"
                + "    stringArrProp: ['Hello', 'World'],"
                + "    dateTimeProp: datetime(),"
                + "    dateProp: date(),"
                + "    pointProp: point({ x:0, y:4, z:1 }),"
                + "    pointArrProp: [point({ x:0, y:4, z:1 }), point({ x:0, y:4, z:1 })],"
                + "    boolProp: true,"
                + "    boolArrProp: [true, false]\n"
                + "}]->(t)"
                + "CREATE (b:Test)-[:REL{ randomProp: 'this property is here to make everything mandatory = false'}]->(b);";

        db.executeTransactionally(q);
        assertTrue(testDBCallEquivalence(
                db, "CALL apoc.meta.relTypeProperties()", "CYPHER 5 CALL db.schema.relTypeProperties()"));
    }

    @Test
    public void testMetaDataOf() {
        db.executeTransactionally("create index for (n:Movie) on (n.title)");
        db.executeTransactionally("create constraint for (a:Actor) require a.name is unique");
        db.executeTransactionally(
                "CREATE (p:Person {name:'Tom Hanks'}), (m:Movie {title:'Forrest Gump'}), (pr:Product{name: 'Awesome Product'}), "
                        + "(p)-[:VIEWED]->(m), (p)-[:BOUGHT{quantity: 10}]->(pr)");
        Set<Map<String, Object>> expectedResult = new HashSet<>();
        expectedResult.add(MapUtil.map(
                "other",
                List.of(),
                "count",
                0L,
                "existence",
                false,
                "index",
                false,
                "label",
                "BOUGHT",
                "right",
                0L,
                "type",
                "INTEGER",
                "sample",
                null,
                "array",
                false,
                "left",
                0L,
                "unique",
                false,
                "property",
                "quantity",
                "elementType",
                "relationship",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of(),
                "count",
                0L,
                "existence",
                false,
                "index",
                false,
                "label",
                "Product",
                "right",
                0L,
                "type",
                "STRING",
                "sample",
                null,
                "array",
                false,
                "left",
                0L,
                "unique",
                false,
                "property",
                "name",
                "elementType",
                "node",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of("Product"),
                "count",
                1L,
                "existence",
                false,
                "index",
                false,
                "label",
                "BOUGHT",
                "right",
                0L,
                "type",
                "RELATIONSHIP",
                "sample",
                null,
                "array",
                false,
                "left",
                1L,
                "unique",
                false,
                "property",
                "Person",
                "elementType",
                "relationship",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of("Product"),
                "count",
                1L,
                "existence",
                false,
                "index",
                false,
                "label",
                "Person",
                "right",
                0L,
                "type",
                "RELATIONSHIP",
                "sample",
                null,
                "array",
                false,
                "left",
                1L,
                "unique",
                false,
                "property",
                "BOUGHT",
                "elementType",
                "node",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of(),
                "count",
                0L,
                "existence",
                false,
                "index",
                false,
                "label",
                "Person",
                "right",
                0L,
                "type",
                "STRING",
                "sample",
                null,
                "array",
                false,
                "left",
                0L,
                "unique",
                false,
                "property",
                "name",
                "elementType",
                "node",
                "otherLabels",
                List.of()));

        String keys = expectedResult.stream()
                .findAny()
                .map(Map::keySet)
                .map(s -> String.join(", ", s))
                .get();

        Consumer<Result> assertResult = (r) -> {
            Set<Map<String, Object>> result = r.stream().collect(Collectors.toSet());
            assertEquals(expectedResult, result);
        };

        TestUtil.testResult(db, "CALL apoc.meta.data.of('MATCH p = ()-[:BOUGHT]->() RETURN p')", assertResult);

        TestUtil.testResult(
                db,
                "MATCH p = ()-[:BOUGHT]->() " + "WITH {nodes: nodes(p), relationships: relationships(p)} AS graphMap "
                        + String.format("CALL apoc.meta.data.of(graphMap) YIELD %s ", keys)
                        + "RETURN "
                        + keys,
                assertResult);

        TestUtil.testResult(
                db,
                "CALL apoc.graph.fromCypher('MATCH p = ()-[:BOUGHT]->() RETURN p', {}, '', {}) YIELD graph "
                        + String.format("CALL apoc.meta.data.of(graph) YIELD %s ", keys)
                        + "RETURN "
                        + keys,
                assertResult);
    }

    @Test
    public void testMetaDataOfWithRelConstraints() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKES]-() REQUIRE like.score IS UNIQUE");
        db.executeTransactionally(
                "CREATE (gem:Person {name: \"Gem\"})-[:LIKES {score: 10}]->(cake:Cake {type: \"Chocolate\"})");
        Set<Map<String, Object>> expectedResult = new HashSet<>();
        expectedResult.add(MapUtil.map(
                "other",
                List.of("Cake"),
                "count",
                1L,
                "existence",
                false,
                "index",
                false,
                "label",
                "LIKES",
                "right",
                0L,
                "type",
                "RELATIONSHIP",
                "sample",
                null,
                "array",
                false,
                "left",
                1L,
                "unique",
                false,
                "property",
                "Person",
                "elementType",
                "relationship",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of(),
                "count",
                0L,
                "existence",
                false,
                "index",
                true,
                "label",
                "LIKES",
                "right",
                0L,
                "type",
                "INTEGER",
                "sample",
                null,
                "array",
                false,
                "left",
                0L,
                "unique",
                true,
                "property",
                "score",
                "elementType",
                "relationship",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of(),
                "count",
                0L,
                "existence",
                false,
                "index",
                false,
                "label",
                "Cake",
                "right",
                0L,
                "type",
                "STRING",
                "sample",
                null,
                "array",
                false,
                "left",
                0L,
                "unique",
                false,
                "property",
                "type",
                "elementType",
                "node",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of("Cake"),
                "count",
                1L,
                "existence",
                false,
                "index",
                false,
                "label",
                "Person",
                "right",
                0L,
                "type",
                "RELATIONSHIP",
                "sample",
                null,
                "array",
                false,
                "left",
                1L,
                "unique",
                false,
                "property",
                "LIKES",
                "elementType",
                "node",
                "otherLabels",
                List.of()));
        expectedResult.add(MapUtil.map(
                "other",
                List.of(),
                "count",
                0L,
                "existence",
                false,
                "index",
                false,
                "label",
                "Person",
                "right",
                0L,
                "type",
                "STRING",
                "sample",
                null,
                "array",
                false,
                "left",
                0L,
                "unique",
                false,
                "property",
                "name",
                "elementType",
                "node",
                "otherLabels",
                List.of()));

        Set<Map<String, Object>> actualResult = new HashSet<>();

        TestUtil.testResult(db, "CALL apoc.meta.data.of('MATCH p = ()-[:LIKES]->() RETURN p')", result -> {
            while (result.hasNext()) {
                actualResult.add(result.next());
            }
            assertEquals(actualResult, expectedResult);
        });
    }

    @Test
    public void testMetaGraphOf() {
        db.executeTransactionally(
                "CREATE (p:Person {name:'Tom Hanks'}), (m:Movie {title:'Forrest Gump'}), (pr:Product{name: 'Awesome Product'}), "
                        + "(p)-[:VIEWED]->(m), (p)-[:BOUGHT{quantity: 10}]->(pr)");

        Consumer<Result> assertResult = (r) -> {
            Map<String, Object> row = r.next();
            List<Node> nodes = (List<Node>) row.get("nodes");
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(2, nodes.size());
            assertEquals(1, relationships.size());
            Set<Set<String>> labels = nodes.stream()
                    .map(n -> StreamSupport.stream(n.getLabels().spliterator(), false)
                            .map(Label::name)
                            .collect(Collectors.toSet()))
                    .collect(Collectors.toSet());
            assertEquals(2, labels.size());
            assertEquals(Set.of(Set.of("Person"), Set.of("Product")), labels);
            assertEquals(
                    RelationshipType.withName("BOUGHT"), relationships.get(0).getType());
        };

        TestUtil.testResult(db, "CALL apoc.meta.graph.of('MATCH p = ()-[:BOUGHT]->() RETURN p')", assertResult);

        TestUtil.testResult(
                db,
                "MATCH p = ()-[:BOUGHT]->() " + "WITH {nodes: nodes(p), relationships: relationships(p)} AS graphMap "
                        + "CALL apoc.meta.graph.of(graphMap) YIELD nodes, relationships "
                        + "RETURN *",
                assertResult);

        TestUtil.testResult(
                db,
                "CALL apoc.graph.fromCypher('MATCH p = ()-[:BOUGHT]->() RETURN p', {}, '', {}) YIELD graph "
                        + "CALL apoc.meta.graph.of(graph) YIELD nodes, relationships "
                        + "RETURN *",
                assertResult);
    }

    @Test
    public void testMetaRelTypePropertiesWithManyRels() {
        db.executeTransactionally("UNWIND range (0, 200) as idx CREATE (a:A)-[:FIRST_A]-> (b:B)");
        db.executeTransactionally("CREATE (a:A)-[:FIRST_A {a: 1}]->(b:B)");

        // with default maxRels
        testCall(db, "CALL apoc.meta.relTypeProperties({includeRels: ['FIRST_A']})", r -> {
            assertNull(r.get("propertyTypes"));
            assertNull(r.get("propertyName"));
        });

        // with maxRels incremented
        testCall(db, "CALL apoc.meta.relTypeProperties({includeRels: ['FIRST_A'], maxRels: 1000})", r -> {
            assertEquals(List.of("Long"), r.get("propertyTypes"));
            assertEquals("a", r.get("propertyName"));
        });
    }

    @Test
    public void testMetaStatsWithTwoDots() {
        db.executeTransactionally(
                "CREATE (n:`My:Label` {id:1})-[r:`http://www.w3.org/2000/01/rdf-schema#isDefinedBy` {alpha: 'beta'}]->(s:Another)");
        TestUtil.testCall(db, "CALL apoc.meta.stats()", row -> {
            assertEquals(map("My:Label", 1L, "Another", 1L), row.get("labels"));
            assertEquals(2L, row.get("labelCount"));
            assertEquals(map("http://www.w3.org/2000/01/rdf-schema#isDefinedBy", 1L), row.get("relTypesCount"));
            assertEquals(2L, row.get("propertyKeyCount"));
            assertEquals(
                    map(
                            "()-[:http://www.w3.org/2000/01/rdf-schema#isDefinedBy]->(:Another)",
                            1L,
                            "()-[:http://www.w3.org/2000/01/rdf-schema#isDefinedBy]->()",
                            1L,
                            "(:My:Label)-[:http://www.w3.org/2000/01/rdf-schema#isDefinedBy]->()",
                            1L),
                    row.get("relTypes"));
        });
    }

    @Test
    public void testMetaDataWithRelIndexes() {
        datasetWithNodeRelIdxs();

        testResult(
                db,
                "CALL apoc.meta.data() YIELD label, property, index, type "
                        + "\nWHERE type='STRING' RETURN label, property, index ORDER BY property",
                (res) -> {
                    Map<String, Object> aProp = res.next();
                    assertEquals("Person", aProp.get("label"));
                    assertEquals("a", aProp.get("property"));
                    assertFalse((boolean) aProp.get("index"));

                    Map<String, Object> bProp = res.next();
                    assertEquals("Movie", bProp.get("label"));
                    assertEquals("b", bProp.get("property"));
                    assertTrue((boolean) bProp.get("index"));

                    Map<String, Object> fooProp = res.next();
                    assertEquals("ACTED_IN", fooProp.get("label"));
                    assertEquals("foo", fooProp.get("property"));
                    assertFalse((boolean) fooProp.get("index"));

                    Map<String, Object> idProp = res.next();
                    assertEquals("ACTED_IN", idProp.get("label"));
                    assertEquals("id", idProp.get("property"));
                    assertTrue((boolean) idProp.get("index"));

                    Map<String, Object> rolesProp = res.next();
                    assertEquals("ACTED_IN", rolesProp.get("label"));
                    assertEquals("roles", rolesProp.get("property"));
                    assertTrue((boolean) rolesProp.get("index"));
                    assertFalse(res.hasNext());
                });
    }

    @Test
    public void testMetaSchemaWithRelIndexes() {
        datasetWithNodeRelIdxs();

        TestUtil.testCall(db, "CALL apoc.meta.schema()", (row) -> {
            Map<String, Object> value = (Map<String, Object>) row.get("value");
            Map<String, Object> relData = (Map<String, Object>) value.get("ACTED_IN");
            Map<String, Object> relProperties = (Map<String, Object>) relData.get("properties");
            Map<String, Object> rolesProp = (Map<String, Object>) relProperties.get("roles");
            assertTrue((boolean) rolesProp.get("indexed"));
            Map<String, Object> fooProp = (Map<String, Object>) relProperties.get("foo");
            assertFalse((boolean) fooProp.get("indexed"));
            Map<String, Object> idProp = (Map<String, Object>) relProperties.get("id");
            assertTrue((boolean) idProp.get("indexed"));

            Map<String, Object> movieData = (Map<String, Object>) value.get("Movie");
            Map<String, Object> movieProperties = (Map<String, Object>) movieData.get("properties");
            Map<String, Object> bProp = (Map<String, Object>) movieProperties.get("b");
            assertTrue((boolean) bProp.get("indexed"));

            Map<String, Object> personData = (Map<String, Object>) value.get("Person");
            Map<String, Object> personProperties = (Map<String, Object>) personData.get("properties");
            Map<String, Object> aProp = (Map<String, Object>) personProperties.get("a");
            assertFalse((boolean) aProp.get("indexed"));
        });
    }

    private void datasetWithNodeRelIdxs() {
        db.executeTransactionally("CREATE INDEX node_index_name FOR (n:Movie) ON (n.b)");
        db.executeTransactionally("CREATE INDEX rel_index_name FOR ()-[r:ACTED_IN]-() ON (r.roles, r.id)");
        db.executeTransactionally(
                "CREATE (:Person {a: '11'})-[:ACTED_IN {roles:'Forrest', id:'123', foo: 'bar'}]->(:Movie {b: '1'})");
    }

    @Test
    public void testMetaStatsWithLabelAndRelTypeCountInUse() {
        db.executeTransactionally("CREATE (:Node:Test)-[:REL {a: 'b'}]->(:Node {c: 'd'})<-[:REL]-(:Node:Test)");
        db.executeTransactionally("CREATE (:A {e: 'f'})-[:ANOTHER {g: 'h'}]->(:C)");

        TestUtil.testCall(db, "CALL apoc.meta.stats()", row -> {
            assertEquals(map("A", 1L, "C", 1L, "Test", 2L, "Node", 3L), row.get("labels"));
            assertEquals(5L, row.get("nodeCount"));
            assertEquals(4L, row.get("labelCount"));

            assertEquals(map("REL", 2L, "ANOTHER", 1L), row.get("relTypesCount"));
            assertEquals(2L, row.get("relTypeCount"));
            assertEquals(3L, row.get("relCount"));
            Map<String, Object> expectedRelTypes = map(
                    "(:A)-[:ANOTHER]->()",
                    1L,
                    "()-[:REL]->(:Node)",
                    2L,
                    "(:Test)-[:REL]->()",
                    2L,
                    "(:Node)-[:REL]->()",
                    2L,
                    "()-[:ANOTHER]->(:C)",
                    1L,
                    "()-[:ANOTHER]->()",
                    1L,
                    "()-[:REL]->()",
                    2L);
            assertEquals(expectedRelTypes, row.get("relTypes"));
        });

        db.executeTransactionally("match p=(:A)-[:ANOTHER]->(:C) delete p");
        TestUtil.testCall(db, "CALL apoc.meta.stats()", row -> {
            assertEquals(map("Test", 2L, "Node", 3L), row.get("labels"));
            assertEquals(3L, row.get("nodeCount"));
            assertEquals(2L, row.get("labelCount"));

            assertEquals(map("REL", 2L), row.get("relTypesCount"));
            assertEquals(1L, row.get("relTypeCount"));
            assertEquals(2L, row.get("relCount"));
            Map<String, Object> expectedRelTypes = map(
                    "()-[:REL]->(:Node)", 2L, "(:Test)-[:REL]->()", 2L, "(:Node)-[:REL]->()", 2L, "()-[:REL]->()", 2L);
            assertEquals(expectedRelTypes, row.get("relTypes"));
        });
    }

    @Test
    public void testMetaNodesCount() {
        db.executeTransactionally("CREATE (:MyCountLabel {id: 1}), (:MyCountLabel {id: 2}), (:ThirdLabel {id: 3})");

        // 2 outcome rels and 1 incoming
        db.executeTransactionally(
                "MATCH (n:MyCountLabel {id: 1}), (m:ThirdLabel {id: 3}) "
                        + "CREATE (n)-[:MY_COUNT_REL]->(m), (n)-[:ANOTHER_MY_COUNT_REL]->(m), (n)<-[:ANOTHER_MY_COUNT_REL]-(m)");

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel'], {rels: ['MY_COUNT_REL']}) AS count",
                row -> assertEquals(1L, row.get("count")));

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel', 'NotExistent'], {rels: ['MY_COUNT_REL']}) AS count",
                row -> assertEquals(1L, row.get("count")));

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel'], {rels: ['MY_COUNT_REL>']}) AS count",
                row -> assertEquals(1L, row.get("count")));

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel'], {rels: ['MY_COUNT_REL<']}) AS count",
                row -> assertEquals(0L, row.get("count")));

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel'], {rels: ['MY_COUNT_REL', 'ANOTHER_MY_COUNT_REL']}) AS count",
                row -> assertEquals(1L, row.get("count")));

        // another 2 nodes with 2 new labels
        db.executeTransactionally("CREATE (:AnotherCountLabel)<-[:MY_COUNT_REL]-(:NotInCountLabel)");

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel', 'AnotherCountLabel'], {rels: ['MY_COUNT_REL', 'ANOTHER_MY_COUNT_REL']}) AS count",
                row -> assertEquals(2L, row.get("count")));

        // create another 2 rels in `MyCountLabel` nodes
        db.executeTransactionally("MATCH (n:MyCountLabel) WITH n CREATE (n)<-[:MY_COUNT_REL]-(:NotInCountLabel)");

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel', 'AnotherCountLabel'], {rels: ['MY_COUNT_REL', 'ANOTHER_MY_COUNT_REL']}) AS count",
                row -> assertEquals(3L, row.get("count")));

        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel', 'AnotherCountLabel'], {rels: ['MY_COUNT_REL', 'ANOTHER_MY_COUNT_REL']}) AS count",
                row -> assertEquals(3L, row.get("count")));

        // just to check that with both direction takes all
        TestUtil.testCall(
                db,
                "RETURN apoc.meta.nodes.count(['MyCountLabel', 'AnotherCountLabel'], {rels: ['MY_COUNT_REL>', 'MY_COUNT_REL<', 'ANOTHER_MY_COUNT_REL']}) AS count",
                row -> assertEquals(3L, row.get("count")));
    }

    @Test
    public void testRelTypePropertiesMovies() throws Exception {
        final String query = IOUtils.toString(new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("movies.cypher")));

        db.executeTransactionally(query);

        TestUtil.testResult(
                db,
                "CALL apoc.meta.relTypeProperties($config)",
                Map.of("config", Map.of("includeRels", List.of("REVIEWED"))),
                r -> {
                    final Set<Map<String, Object>> actual = r.stream().collect(Collectors.toSet());
                    final Set<Map<String, Object>> expected = Set.of(
                            Map.of(
                                    "relType",
                                    ":`REVIEWED`",
                                    "sourceNodeLabels",
                                    List.of("Person"),
                                    "targetNodeLabels",
                                    List.of("Movie"),
                                    "propertyTypes",
                                    List.of("Long"),
                                    "mandatory",
                                    false,
                                    "propertyObservations",
                                    8L,
                                    "totalObservations",
                                    8L,
                                    "propertyName",
                                    "rating"),
                            Map.of(
                                    "relType",
                                    ":`REVIEWED`",
                                    "sourceNodeLabels",
                                    List.of("Person"),
                                    "targetNodeLabels",
                                    List.of("Movie"),
                                    "propertyTypes",
                                    List.of("String"),
                                    "mandatory",
                                    false,
                                    "propertyObservations",
                                    8L,
                                    "totalObservations",
                                    8L,
                                    "propertyName",
                                    "summary"));
                    Assert.assertEquals(expected, actual);
                });
    }

    @Test
    public void testMetaGraphSampling() {
        db.executeTransactionally("CREATE (:A)-[:R1]->(:B)-[:R1]->(:C)");

        // Not specifying sampling will check through all relationships and make sure they
        // exist, clearing out non-existing ones
        testCall(db, "CALL apoc.meta.graph()", (row) -> {
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(2, relationships.size());
        });

        // A SampleSize larger than the number of Nodes will check one node still
        testCall(db, "CALL apoc.meta.graph({ sample: 1000 })", (row) -> {
            List<Relationship> relationships = (List<Relationship>) row.get("relationships");
            assertEquals(2, relationships.size());
        });

        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testMetaGraphSparseSampling() {
        // The 3 procedures using this sampling, set to look at the whole graph
        List<String> samplingBasedProcs = List.of(
                "apoc.meta.graph(", "apoc.meta.graph.of(\"MATCH p = ()-[]->() RETURN p\", ", "apoc.meta.subGraph(");
        // The "Schema" Should be: A->B->C and A->C
        for (int i = 0; i < 100; i++) {
            if (i == 50) {
                // Create one existing A-->C relationship
                db.executeTransactionally("CREATE (:A)-[:R1]->(:C)");
            } else {
                // Create 99 A->B->C relationships
                db.executeTransactionally("CREATE (:A)-[:R1]->(:B)-[:R1]->(:C)");
            }
        }

        for (String samplingProc : samplingBasedProcs) {
            // Not specifying sampling will check through all relationships and make sure they
            // exist, clearing out non-existing ones
            testCall(db, "CALL " + samplingProc + " {})", (row) -> {
                List<Relationship> relationships = (List<Relationship>) row.get("relationships");
                assertEquals(3, relationships.size());
            });

            // A SampleSize larger than the number of Nodes will check one node still, returning
            // missing relationships. In this case; A->C which does exist will not be returned.
            testCall(db, "CALL " + samplingProc + " { sample: 1000 })", (row) -> {
                List<Relationship> relationships = (List<Relationship>) row.get("relationships");
                assertEquals(2, relationships.size());
            });

            // A SampleSize which isn't fine-grained enough to find the one A->C relationship, returning
            // missing relationships. In this case; A->C which does exist will not be returned.
            testCall(db, "CALL " + samplingProc + " { sample: 99 })", (row) -> {
                List<Relationship> relationships = (List<Relationship>) row.get("relationships");
                assertEquals(2, relationships.size());
            });
        }

        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testDifferentCypherVersionsApocMetaDataOf() {
        db.executeTransactionally("CREATE (:CYPHER_5 {prop: 1}), (:CYPHER_25 {prop: 1})");

        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.meta.data.of('%s MATCH (n:$(apoc.cypherVersion())) RETURN n') YIELD label RETURN label",
                    cypherVersion.outerVersion, cypherVersion.innerVersion);
            testCall(db, query, r -> assertEquals(cypherVersion.result, r.get("label")));
        }
    }

    @Test
    public void testDifferentCypherVersionsApocMetaGraphOf() {
        db.executeTransactionally("CREATE (:CYPHER_5), (:CYPHER_25)");

        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.meta.graph.of('%s MATCH (n:$(apoc.cypherVersion())) RETURN n') YIELD nodes RETURN labels(nodes[0])[0] AS version",
                    cypherVersion.outerVersion, cypherVersion.innerVersion);

            testCall(db, query, r -> assertEquals(cypherVersion.result, r.get("version")));
        }
    }
}
