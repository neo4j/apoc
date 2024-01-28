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
package apoc.cypher;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testFail;
import static apoc.util.TestUtil.testResult;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;
import static apoc.util.TransactionTestUtil.checkTransactionTimeReasonable;
import static apoc.util.TransactionTestUtil.lastTransactionChecks;
import static apoc.util.TransactionTestUtil.terminateTransactionAsync;
import static apoc.util.Util.map;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 08.05.16
 */
public class CypherTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    new File("src/test/resources").toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(
                db, Cypher.class, Utils.class, CypherFunctions.class, Timeboxed.class, Strings.class);
    }

    @After
    public void clearDB() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testRunWrite() {
        runWriteAndDoItCommons("runWrite");
    }

    @Test
    public void testDoIt() {
        runWriteAndDoItCommons("doIt");
    }

    @Test
    public void testRunSchema() {
        testCallEmpty(
                db,
                "CALL apoc.cypher.runSchema('CREATE INDEX test FOR (w:TestOne) ON (w.name)',{})",
                Collections.emptyMap());
        testCallEmpty(
                db,
                "CALL apoc.cypher.runSchema('CREATE CONSTRAINT testConstraint FOR (w:TestTwo) REQUIRE w.baz IS UNIQUE',{})",
                Collections.emptyMap());

        try (Transaction tx = db.beginTx()) {
            assertNotNull(tx.schema().getConstraintByName("testConstraint"));
            assertNotNull(tx.schema().getIndexByName("test"));
        }
    }

    @Test
    public void testRun() {
        testCall(
                db,
                "CALL apoc.cypher.run('RETURN $a + 7 AS b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunNullParams() {
        testCall(
                db,
                "CALL apoc.cypher.run('RETURN 42 AS b',null)",
                r -> assertEquals(42L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunNoParams() {
        testCall(
                db,
                "CALL apoc.cypher.run('RETURN 42 AS b',{})",
                r -> assertEquals(42L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunVariable() {
        testCall(
                db,
                "CALL apoc.cypher.run('RETURN a + 7 AS b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testRunFirstColumnSingle() {
        testCall(
                db,
                "RETURN apoc.cypher.runFirstColumnSingle('RETURN a + 7 AS b', {a: 3}) AS s",
                r -> assertEquals(10L, (r.get("s"))));
    }

    @Test
    public void testRunFirstColumnMany() {
        testCall(
                db,
                "RETURN apoc.cypher.runFirstColumnMany('UNWIND range(1,a) AS id RETURN id', {a: 3}) AS s",
                r -> assertEquals(Arrays.asList(1L, 2L, 3L), (r.get("s"))));
    }

    @Test
    public void testRunFirstColumnBugCompiled() {
        TestUtil.singleResultFirstColumn(
                db,
                "CREATE (m:Movie  {title:'MovieA'})<-[:ACTED_IN]-(p:Person {name:'PersonA'})-[:ACTED_IN]->(m2:Movie {title:'MovieB'}) RETURN m");
        String query =
                "MATCH (m:Movie  {title:'MovieA'}) MATCH (m)<-[:ACTED_IN]-(:Person)-[:ACTED_IN]->(rec:Movie) RETURN rec LIMIT 10";
        String plan =
                db.executeTransactionally("EXPLAIN " + query, emptyMap(), result -> result.getExecutionPlanDescription()
                        .toString());
        System.out.println(plan);
        List<Node> recs = TestUtil.firstColumn(db, query);
        assertEquals(1, recs.size());
    }

    @Test
    public void testSingular() {
        int size = 10_000;
        testResult(
                db,
                "CALL apoc.cypher.run('UNWIND a AS row UNWIND range(0,9) AS b RETURN b',{ a:range(1, $size) })",
                map("size", size),
                r -> assertEquals(size * 10, Iterators.count(r)));
    }

    private long toLong(Object value) {
        return Util.toLong(value);
    }

    @Test(timeout = 9000)
    public void testWithTimeout() {
        assertFalse(db.executeTransactionally(
                "CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10000)', null, $timeout)",
                singletonMap("timeout", 100),
                Result::hasNext));
    }

    @Test
    public void testRunTimeboxedWithTermination() {
        final String query =
                "CALL apoc.cypher.runTimeboxed('UNWIND range(0, 10) AS id CALL apoc.util.sleep(2000) RETURN 0', null, 20000)";
        checkTerminationGuard(db, query);
    }

    @Test
    public void testRunTimeboxedWithTerminationInnerTransaction1() {
        // this query throws an error because of ` AS 'a'`
        final String innerQuery = "CALL apoc.util.sleep(1000) RETURN 1 AS 'a'";
        final String query = "CALL apoc.cypher.runTimeboxed($innerQuery, null, $timeout)";

        long timeBefore = System.currentTimeMillis();

        // check that the query returns nothing and terminate before `timeout`
        long timeout = 5L;
        db.executeTransactionally(query, Map.of("innerQuery", innerQuery, "timeout", timeout), Result::resultAsString);
        checkTransactionTimeReasonable(timeout, timeBefore);
    }

    @Test
    public void testRunTimeboxedWithTerminationInnerTransaction() {
        final String innerLongQuery = "CALL apoc.util.sleep(10999) RETURN 0";
        final String query = "CALL apoc.cypher.runTimeboxed($innerQuery, null, 99999)";

        terminateTransactionAsync(db, innerLongQuery);

        long timeBefore = System.currentTimeMillis();

        try (final var tx = db.beginTx()) {
            final var result = tx.execute(query, Map.of("innerQuery", innerLongQuery));
            /*
             * There are two possible valid outcomes of this query:
             * 1. The inner query is terminated but successfully finishes and returns 0.
             *    apoc.util.sleep will try to keep going even after tx termination weirdly enough ¯\_(ツ)_/¯.
             * 2. The inner query do not finish or return anything,
             *    runtime has its own termination guards that can make this happen.
             */
            if (result.hasNext()) {
                final var row = result.next();
                assertEquals(Map.of("0", 0L), row.get("value"));
                assertFalse("Expected one or zero rows", result.hasNext());
            }
        }

        lastTransactionChecks(db, query, timeBefore);
    }

    @Test
    public void testRunMany() {
        final Map<String, Object> map = map("name", "John", "name2", "Doe");
        testResult(
                db,
                "CALL apoc.cypher.runMany('CREATE (n:Node {name:$name});\nMATCH (n {name:$name}) CREATE (n)-[:X {name:$name2}]->(n);',$params)",
                map("params", map),
                r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("relationshipsCreated")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    assertFalse(r.hasNext());
                });
        final long count = (long) db.executeTransactionally(
                        "MATCH p = (n:Node{name : $name})-[r:X{name: $name2}]->(n) RETURN count(p) AS count",
                        map,
                        Result::next)
                .get("count");
        assertEquals(1, count);
    }

    @Test
    public void testRunManyReadOnlyShouldFail() {
        final Map<String, Object> map = map("name", "John", "name2", "Doe");
        db.executeTransactionally(
                "CALL apoc.cypher.runManyReadOnly('" + "CREATE (n:Node {name:$name});\n"
                        + "MATCH (n {name:$name}) "
                        + "CREATE (n)-[:X {name:$name2}]->(n) "
                        + "RETURN *;"
                        + "', $params)",
                map("params", map),
                Result::resultAsString);
        final long count = (long) db.executeTransactionally(
                        "MATCH p = (n:Node {name : $name})-[r:X {name: $name2}]->(n) RETURN count(p) AS count",
                        map,
                        Result::next)
                .get("count");
        assertEquals(0, count);
    }

    @Test
    public void shouldTimeboxedReturnAllResultsSoFar() {
        db.executeTransactionally(Util.readResourceFile("movies.cypher"));

        long start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(
                    "CALL apoc.cypher.runTimeboxed('MATCH (n)-[*]-(m) RETURN elementId(n), elementId(m)', {}, 1000) YIELD value RETURN value");
            assertTrue(Iterators.count(result) > 0);
            tx.commit();
        }
        long duration = System.currentTimeMillis() - start;
        // Assert that the test runs in less than 1500 milliseconds
        assertTrue(duration < 1500L);
    }

    @Test(timeout = 9000)
    public void shouldTooLongTimeboxBeNotHarmful() {
        assertFalse(db.executeTransactionally(
                "CALL apoc.cypher.runTimeboxed('CALL apoc.util.sleep(10)', null, $timeout)",
                singletonMap("timeout", 10000),
                Result::hasNext));
    }

    @Test
    public void testSimpleWhenIfCondition() {
        testCall(db, "CALL apoc.when(true, 'RETURN 7 AS b')", r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleWhenElseCondition() {
        testCall(
                db,
                "CALL apoc.when(false, 'RETURN 7 AS b') YIELD value RETURN value",
                r -> assertNull(((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenIfCondition() {
        testCall(
                db,
                "CALL apoc.when(true, 'RETURN $a + 7 AS b', 'RETURN $a AS b',{a:3})",
                r -> assertEquals(10L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testWhenElseCondition() {
        testCall(
                db,
                "CALL apoc.when(false, 'RETURN $a + 7 AS b', 'RETURN $a AS b',{a:3})",
                r -> assertEquals(3L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testDoWhenIfCondition() {
        testCall(
                db,
                "CALL apoc.do.when(true, 'CREATE (a:Node{name:\"A\"}) RETURN a.name AS aName', 'CREATE (b:Node{name:\"B\"}) RETURN b.name AS bName',{})",
                r -> {
                    assertEquals("A", ((Map) r.get("value")).get("aName"));
                    assertNull(((Map) r.get("value")).get("bName"));
                });
    }

    @Test
    public void testDoWhenElseCondition() {
        testCall(
                db,
                "CALL apoc.do.when(false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name AS aName', 'CREATE (b:Node{name:\"B\"}) RETURN b.name AS bName',{})",
                r -> {
                    assertEquals("B", ((Map) r.get("value")).get("bName"));
                    assertNull(((Map) r.get("value")).get("aName"));
                });
    }

    @Test
    public void testCase() {
        testCall(
                db,
                "CALL apoc.case([false, 'RETURN $a + 7 AS b', false, 'RETURN $a AS b', true, 'RETURN $a + 4 AS b', false, 'RETURN $a + 1 AS b'], 'RETURN $a + 10 AS b', {a:3})",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testCaseElseCondition() {
        testCall(
                db,
                "CALL apoc.case([false, 'RETURN $a + 7 AS b', false, 'RETURN $a AS b', false, 'RETURN $a + 4 AS b'], 'RETURN $a + 10 AS b', {a:3})",
                r -> assertEquals(13L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleCase() {
        testCall(
                db,
                "CALL apoc.case([false, 'RETURN 3 + 7 AS b', false, 'RETURN 3 AS b', true, 'RETURN 3 + 4 AS b'])",
                r -> assertEquals(7L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testSimpleCaseElseCondition() {
        testCall(
                db,
                "CALL apoc.case([false, 'RETURN 3 + 7 AS b', false, 'RETURN 3 AS b', false, 'RETURN 3 + 4 AS b'], 'RETURN 3 + 10 AS b')",
                r -> assertEquals(13L, ((Map) r.get("value")).get("b")));
    }

    @Test
    public void testCaseDo() {
        testCall(
                db,
                "CALL apoc.do.case([false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name AS aName', true, 'CREATE (b:Node{name:\"B\"}) RETURN b.name AS bName'], 'CREATE (c:Node{name:\"C\"}) RETURN c.name AS cName',{})",
                r -> {
                    assertNull(((Map) r.get("value")).get("aName"));
                    assertEquals("B", ((Map) r.get("value")).get("bName"));
                    assertNull(((Map) r.get("value")).get("cName"));
                });
    }

    @Test
    public void testCaseDoElseCondition() {
        testCall(
                db,
                "CALL apoc.do.case([false, 'CREATE (a:Node{name:\"A\"}) RETURN a.name AS aName', false, 'CREATE (b:Node{name:\"B\"}) RETURN b.name AS bName'], 'CREATE (c:Node{name:\"C\"}) RETURN c.name AS cName',{})",
                r -> {
                    assertNull(((Map) r.get("value")).get("aName"));
                    assertNull(((Map) r.get("value")).get("bName"));
                    assertEquals("C", ((Map) r.get("value")).get("cName"));
                });
    }

    private void runWriteAndDoItCommons(String functionName) {
        testCallEmpty(
                db,
                String.format("CALL apoc.cypher.%s('CREATE (n:TestOne {a: $b})',{b: 32})", functionName),
                emptyMap());

        testCall(
                db,
                String.format("CALL apoc.cypher.%s('MATCH (n:TestOne) RETURN n',{})", functionName),
                r -> assertEquals(
                        "TestOne",
                        Iterables.single(((Node) ((Map) r.get("value")).get("n")).getLabels())
                                .name()));

        testFail(
                db,
                String.format("CALL apoc.cypher.%s('CREATE INDEX test FOR (w:TestOne) ON (w.foo)',{})", functionName),
                QueryExecutionException.class);
    }

    @Test
    public void runManyCloseTransactionsWithRandomFailures() {
        final var rnd = new Random();
        final var seed = rnd.nextLong();
        rnd.setSeed(seed);

        final var statements = new ArrayList<String>();

        // Add some statements that will never fail
        final var successStatements = 1024;
        int row = 0;
        for (int i = 0; i < successStatements; ++i) {
            final var size = rnd.nextInt(512);
            statements.add("unwind range(%s, %s) as x return x".formatted(row, size));
            row += size + 1;
        }

        // Add one failing query at random position
        final var failureIndex = rnd.nextInt(statements.size());
        statements.set(
                failureIndex,
                "unwind range(%s, %s) as y return y, 1/y".formatted(-rnd.nextInt(1024), rnd.nextInt(1024)));

        // The outer query also fails at a random row
        final var failureRow = rnd.nextInt(row);

        assertThatThrownBy(() -> {
                    try (final var tx = db.beginTx()) {
                        final var q =
                                """
                    call apoc.cypher.runMany($q, {}) yield row, result
                    return row, result, 1 / (result.x - $x) as boom
                    """;
                        final var innerQ = String.join(";\n", statements);
                        try (final var result = tx.execute(q, Map.of("q", innerQ, "x", failureRow))) {
                            result.accept(r -> true);
                        }
                    }
                })
                .hasRootCauseInstanceOf(org.neo4j.exceptions.ArithmeticException.class);

        assertNoOpenTransactionsEventually();
    }

    private void assertNoOpenTransactionsEventually() {
        await("transactions closed")
                .pollInterval(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(10))
                .pollInSameThread()
                .untilAsserted(this::assertNoOpenTransactions);
    }

    private void assertNoOpenTransactions() {
        final var txs = db.executeTransactionally(
                "show transactions", Map.of(), r -> r.stream().toList());
        assertThat(txs).satisfiesExactly(row -> assertEquals("show transactions", row.get("currentQuery")));
    }
}
