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
package apoc.periodic;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.TransactionTestUtil.lastTransactionChecks;
import static apoc.util.TransactionTestUtil.terminateTransactionAsync;
import static apoc.util.Util.CONSUME_VOID;
import static apoc.util.Util.map;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Iterables.count;
import static org.neo4j.test.assertion.Assert.assertEventually;

import apoc.HelperProcedures;
import apoc.cypher.Cypher;
import apoc.refactor.GraphRefactoring;
import apoc.schema.Schemas;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import apoc.util.collection.Iterators;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
public class PeriodicTest {
    public static class MockLogger {
        @Context
        public Log log;

        @Procedure("apoc.mockLog")
        public void mockLog(@Name("value") String value) {
            log.info(value);
        }
    }

    public static final long RUNDOWN_COUNT = 1000;
    public static final int BATCH_SIZE = 399;

    @Inject
    GraphDatabaseAPI db;

    public static AssertableLogProvider logProvider = new AssertableLogProvider();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setInternalLogProvider(logProvider)
                .setConfigRaw(Map.of("internal.dbms.cypher.enable_experimental_versions", "true"));
    }

    @BeforeAll
    public void initDb() {
        TestUtil.registerProcedure(
                db,
                Periodic.class,
                Schemas.class,
                Cypher.class,
                Utils.class,
                MockLogger.class,
                GraphRefactoring.class,
                HelperProcedures.class,
                TickTockProcedure.class);
    }

    @AfterEach
    void afterEach() {
        db.executeTransactionally(
                "call apoc.periodic.list() yield name call apoc.periodic.cancel(name) yield name as name2 return count(*)");
    }

    @Test
    public void testRepeatWithVoidProcedure() {
        String logVal = "repeatVoid";
        String query =
                "CALL apoc.periodic.repeat('repeat-1', 'CALL apoc.mockLog($logVal)', 1, {params: {logVal: $logVal}})";
        testLogIncrease(query, logVal);
    }

    @Test
    public void testRepeatWithVoidProcedureAndReturn() {
        String logVal = "repeatVoidWithReturn";
        String query =
                "CALL apoc.periodic.repeat('repeat-2', 'CALL apoc.mockLog($logVal) RETURN 1', 1, {params: {logVal: $logVal}})";
        testLogIncrease(query, logVal);
    }

    @Test
    public void testSubmitWithVoidProcedure() {
        String logVal = "submitVoid";
        String query =
                "CALL apoc.periodic.submit('submit-1', 'CALL apoc.mockLog($logVal) RETURN 1', {params: {logVal: $logVal}})";
        testLogIncrease(query, logVal);
    }

    @Test
    public void testSubmitWithVoidProcedureAndReturn() {
        String logVal = "submitVoidWithReturn";
        String query =
                "CALL apoc.periodic.submit('submit-1', 'CALL apoc.mockLog($logVal)', {params: {logVal: $logVal}})";
        testLogIncrease(query, logVal);
    }

    private void testLogIncrease(String query, String logVal) {
        // execute a periodic procedure with `CALL apoc.mockLog(...)` as an inner procedure
        db.executeTransactionally(query, Map.of("logVal", logVal));

        // check custom log in logProvider
        assertEventually(
                () -> {
                    String serialize = logProvider.serialize();
                    return serialize.contains(logVal);
                },
                (val) -> val,
                5L,
                TimeUnit.SECONDS);
    }

    @Test
    public void testSubmitStatement() throws Exception {
        String callList = "CALL apoc.periodic.list()";
        // force pre-caching the queryplan => higher probability to get a result in the last assertion
        db.executeTransactionally(callList, Map.of(), CONSUME_VOID);

        testCall(db, "CALL apoc.periodic.submit('foo','create (:Foo)')", (row) -> {
            assertEquals("foo", row.get("name"));
            assertEquals(false, row.get("done"));
            assertEquals(false, row.get("cancelled"));
            assertEquals(0L, row.get("delay"));
            assertEquals(0L, row.get("rate"));
        });

        long count = tryReadCount(50, "MATCH (:Foo) RETURN COUNT(*) AS count", 1L);

        assertEquals(1L, count);

        try (final var tx = db.beginTx();
                final var result = tx.execute(callList)) {
            final var resultList = result.stream().toList();
            // We clean up completed tasks periodically so this can be empty
            if (!resultList.isEmpty()) {
                assertThat(resultList)
                        .contains(Map.of("name", "foo", "done", true, "cancelled", false, "delay", 0L, "rate", 0L));
            }
        }
    }

    @Test
    public void testSubmitWithSchemaOperation() {
        String errMessage = "Supported query types for the operation are [READ_ONLY, WRITE, READ_WRITE]";
        testSchemaOperationCommon("CREATE INDEX periodicIdx FOR (n:Bar) ON (n.first_name, n.last_name)", errMessage);
    }

    @Test
    public void testSubmitWithSchemaProcedure() {
        String errMessage = "Supported inner procedure modes for the operation are [READ, WRITE, DEFAULT]";
        testSchemaOperationCommon("CALL apoc.schema.assert({}, {})", errMessage);

        testSchemaOperationCommon(
                "CALL apoc.cypher.runSchema('CREATE CONSTRAINT periodicIdx FOR (n:Bar) REQUIRE n.first_name IS UNIQUE', {})",
                errMessage);

        // inner schema procedure
        final String query = "CALL { WITH 1 AS one CALL apoc.schema.assert({}, {}) YIELD key RETURN key } "
                + "IN TRANSACTIONS OF 1000 rows RETURN 1";
        testSchemaOperationCommon(query, errMessage);
    }

    private void testSchemaOperationCommon(String query, String errMessage) {
        try {
            testCall(
                    db,
                    "CALL apoc.periodic.submit('subSchema', $query)",
                    Map.of("query", query),
                    (row) -> fail("Should fail because of unsupported schema operation"));
        } catch (RuntimeException e) {
            final String expected = "Failed to invoke procedure `apoc.periodic.submit`: "
                    + "Caused by: java.lang.RuntimeException: " + errMessage;
            assertEquals(expected, e.getMessage());
        }
    }

    @Test
    public void testSubmitStatementWithParams() throws Exception {
        String callList = "CALL apoc.periodic.list()";
        // force pre-caching the queryplan => higher probability to get a result in the last assertion
        db.executeTransactionally(callList, Map.of(), CONSUME_VOID);

        testCall(
                db,
                "CALL apoc.periodic.submit('foo','create (:Foo { id: $id })', {params: {id: '(╯°□°)╯︵ ┻━┻' }})",
                (row) -> {
                    assertEquals("foo", row.get("name"));
                    assertEquals(false, row.get("done"));
                    assertEquals(false, row.get("cancelled"));
                    assertEquals(0L, row.get("delay"));
                    assertEquals(0L, row.get("rate"));
                });

        long count = tryReadCount(50, "MATCH (:Foo { id: '(╯°□°)╯︵ ┻━┻' }) RETURN COUNT(*) AS count", 1L);

        assertEquals(1L, count);

        try (final var tx = db.beginTx();
                final var result = tx.execute(callList)) {
            final var resultList = result.stream().toList();
            // We clean up completed tasks periodically so this can be empty
            if (!resultList.isEmpty()) {
                assertThat(resultList)
                        .contains(Map.of("name", "foo", "done", true, "cancelled", false, "delay", 0L, "rate", 0L));
            }
        }
    }

    @Test
    public void testApplyPlanner() {
        assertEquals("CYPHER 5 RETURN 1", Util.applyPlanner("RETURN 1", Util.Planner.DEFAULT, "5"));
        assertEquals(
                "CYPHER 5 planner=cost MATCH (n:cypher) RETURN n",
                Util.applyPlanner("MATCH (n:cypher) RETURN n", Util.Planner.COST, "5"));
        assertEquals(
                "CYPHER 25 planner=cost  MATCH (n:cypher) RETURN n",
                Util.applyPlanner("CYPHER 25 MATCH (n:cypher) RETURN n", Util.Planner.COST, "5"));
        assertEquals(
                "CYPHER 5 planner=idp MATCH (n:cypher) RETURN n",
                Util.applyPlanner("MATCH (n:cypher) RETURN n", Util.Planner.IDP, "5"));
        assertEquals(
                "CYPHER 25 planner=dp  runtime=compiled MATCH (n) RETURN n",
                Util.applyPlanner("cypher runtime=compiled MATCH (n) RETURN n", Util.Planner.DP, "25"));
        assertEquals(
                "CYPHER 5 planner=dp MATCH (n) RETURN n",
                Util.applyPlanner("MATCH (n) RETURN n", Util.Planner.DP, "5"));
        assertEquals(
                "CYPHER 25 planner=idp  expressionEngine=compiled MATCH (n) RETURN n",
                Util.applyPlanner("cypher expressionEngine=compiled MATCH (n) RETURN n", Util.Planner.IDP, "25"));
        assertEquals(
                "CYPHER 5  expressionEngine=compiled  planner=cost  MATCH (n) RETURN n",
                Util.applyPlanner(
                        "cypher expressionEngine=compiled planner=idp MATCH (n) RETURN n", Util.Planner.COST, "5"));
        assertEquals(
                "CYPHER 5   planner=cost  MATCH (n) RETURN n",
                Util.applyPlanner("cypher planner=cost MATCH (n) RETURN n", Util.Planner.COST, "5"));
    }

    @Test
    public void testSlottedRuntime() {
        // Positive Tests
        assertEquals(
                "CYPHER 5 runtime=slotted MATCH (n:cypher) RETURN n",
                Util.slottedRuntime("MATCH (n:cypher) RETURN n", "5"));
        assertEquals("CYPHER 25 runtime=slotted MATCH (n) RETURN n", Util.slottedRuntime("MATCH (n) RETURN n", "25"));
        assertEquals(
                "CYPHER 5 runtime=slotted  MATCH (n) RETURN n",
                Util.slottedRuntime("CYPHER 5 MATCH (n) RETURN n", "25"));
        assertEquals("CYPHER 5 runtime=slotted  MATCH (n) RETURN n", Util.slottedRuntime(" MATCH (n) RETURN n", "5"));
        assertEquals(
                "CYPHER 5 runtime=slotted  expressionEngine=compiled MATCH (n) RETURN n",
                Util.slottedRuntime("cypher expressionEngine=compiled MATCH (n) RETURN n", "5"));
        assertEquals(
                "CYPHER 5 runtime=compiled expressionEngine=compiled MATCH (n) RETURN n",
                Util.slottedRuntime("CYPHER 5 runtime=compiled expressionEngine=compiled MATCH (n) RETURN n", "5"));

        // Negative tests
        assertFalse(Util.slottedRuntime(" cypher runtime=compiled MATCH (n) RETURN n", "5")
                .contains("runtime=slotted "));
        assertFalse(Util.slottedRuntime("cypher runtime=compiled MATCH (n) RETURN n", "25")
                .contains("runtime=slotted cypher"));
        assertFalse(Util.slottedRuntime("cypher expressionEngine=compiled MATCH (n) RETURN n", "5")
                .contains(" runtime=slotted cypher"));
        assertFalse(Util.slottedRuntime("cypher 25 expressionEngine=compiled MATCH (n) RETURN n", "5")
                .contains(" runtime=slotted cypher"));
    }

    @Test
    public void testTerminateCommit() {
        PeriodicTestUtils.testTerminateInnerPeriodicQuery(
                db,
                // This query never finish
                """
                        CALL apoc.periodic.commit(
                          'CALL apoc.util.sleep(1000) // Avoid overloading the dbms
                           UNWIND range(0,1000) AS id
                           WITH id
                           MERGE (n:Foo {id: id})
                           WITH n
                           LIMIT 1000
                           RETURN COUNT(n)',
                          {}
                        )""",
                "CALL apoc.util.sleep(1000)");
    }

    @Test
    public void testPeriodicCommitWithoutLimitShouldFail() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> db.executeTransactionally("CALL apoc.periodic.commit('return 0')"));
        assertTrue(e.getMessage().contains("the statement sent to apoc.periodic.commit must contain a `limit`"));
    }

    @Test
    public void testRunDown() {
        db.executeTransactionally(
                "UNWIND range(1,$count) AS id CREATE (n:Person {id:id})", MapUtil.map("count", RUNDOWN_COUNT));

        String query = "MATCH (p:Person) WHERE NOT p:Processed WITH p LIMIT $limit SET p:Processed RETURN count(*)";

        testCall(
                db,
                "CALL apoc.periodic.commit($query,$params)",
                MapUtil.map("query", query, "params", MapUtil.map("limit", BATCH_SIZE)),
                r -> {
                    assertEquals((long) Math.ceil((double) RUNDOWN_COUNT / BATCH_SIZE), r.get("executions"));
                    assertEquals(RUNDOWN_COUNT, r.get("updates"));
                });
        assertEquals(RUNDOWN_COUNT, (long) db.executeTransactionally(
                "MATCH (p:Processed) RETURN COUNT(*) AS c",
                Collections.emptyMap(),
                result -> Iterators.single(result.columnAs("c"))));
    }

    @Test
    public void testPeriodicIterateErrors() {
        testResult(
                db,
                "CALL apoc.periodic.iterate('UNWIND range(0,99) as id RETURN id', 'CREATE null', {batchSize:10,iterateList:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                    assertEquals(0L, row.get("committedOperations"));
                    assertEquals(100L, row.get("failedOperations"));
                    assertEquals(10L, row.get("failedBatches"));

                    String expectedPattern =
                            "(?s)Invalid input.*\\\"CYPHER(?:\\s+(\\d+))? UNWIND \\$_batch AS _batch WITH _batch.id AS id  CREATE null\\\".*";

                    String expectedBatchPattern = "org.neo4j.graphdb.QueryExecutionException: " + expectedPattern;

                    assertError(
                            ((Map<String, Long>) ((Map) row.get("batch")).get("errors")),
                            expectedBatchPattern,
                            Long.valueOf(10));
                    assertError(
                            ((Map<String, Long>) ((Map) row.get("operations")).get("errors")),
                            expectedPattern,
                            Long.valueOf(10));
                });
    }

    private void assertError(Map<String, Long> errors, String expectedPattern, Long errorCount) {
        assertEquals(1, errors.size());
        errors.values().forEach(value -> assertEquals(errorCount, value));
        errors.keySet().forEach(key -> MatcherAssert.assertThat(key, matchesPattern(expectedPattern)));
    }

    @Test
    public void testTerminateIterate() {
        // Calls to apoc.util.sleep needed to not overload db and keep execution alive long enough for termination.
        PeriodicTestUtils.testTerminateInnerPeriodicQuery(
                db,
                """
                        CALL apoc.periodic.iterate(
                          'UNWIND range(0,1000) AS id CALL apoc.util.sleep(1000) RETURN id',
                          'CALL apoc.util.sleep(1000) WITH $id AS id CREATE (:Foo {id: $id})',
                          {batchSize:1,parallel:true}
                        )""",
                "UNWIND range(0,1000) AS id");
        PeriodicTestUtils.testTerminateInnerPeriodicQuery(
                db,
                """
                        CALL apoc.periodic.iterate(
                          'UNWIND range(0,1000) AS id CALL apoc.util.sleep(1000) RETURN id',
                          'CALL apoc.util.sleep(1000) WITH $id AS id CREATE (:Foo {id: $id})',
                          {batchSize:2,iterateList:true}
                        )""",
                "UNWIND range(0,1000) AS id");
        PeriodicTestUtils.testTerminateInnerPeriodicQuery(
                db,
                """
                        CALL apoc.periodic.iterate(
                          'UNWIND range(0,1000) AS id CALL apoc.util.sleep(1000) RETURN id',
                          'CALL apoc.util.sleep(1000) WITH $id AS id CREATE (:Foo {id: $id})',
                          {batchSize:2,iterateList:false}
                        )""",
                "UNWIND range(0,1000) AS id");
    }

    @Test
    public void testWithTerminationInnerTransaction() {
        // terminating the apoc.util.sleep should instantly terminate the periodic query without any creation
        final String innerLongQuery = "CALL apoc.util.sleep(20999) RETURN 0";
        final String query =
                "CALL apoc.periodic.iterate($innerQuery, 'WITH $id as id CREATE (:Foo {id: $id})', {params: {innerQuery: $innerQuery}})";

        terminateTransactionAsync(db, innerLongQuery);

        long timeBefore = System.currentTimeMillis();
        // assert query terminated (RETURN 0 nodesCreated)
        try {
            TestUtil.testCall(db, query, Map.of("innerQuery", innerLongQuery), row -> {
                final Object actual = ((Map) row.get("updateStatistics")).get("nodesCreated");
                assertEquals(0L, actual);
            });
            fail("Should have terminated");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("terminated"));
        }

        lastTransactionChecks(db, query, timeBefore);
    }

    /**
     * test for https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/1314
     * note that this test might depend on timings on your machine
     * prior to fixing #1314 this test fails sporadic (~ in 4 out of 5 attempts) with a
     * java.nio.channels.ClosedChannelException upon db.shutdown
     */
    @Test
    public void terminateIterateShouldNotFailonShutdown() throws Exception {

        long totalNumberOfNodes = 100000;
        int batchSizeCreate = 10000;

        db.executeTransactionally(
                "call apoc.periodic.iterate( " + "'unwind range(0,$totalNumberOfNodes) as i return i', "
                        + "'create (p:Person{name:\"person_\" + i})', "
                        + "{batchSize:$batchSizeCreate, parallel:true, params: {totalNumberOfNodes: $totalNumberOfNodes}})",
                MapUtil.map(
                        "totalNumberOfNodes", totalNumberOfNodes,
                        "batchSizeCreate", batchSizeCreate));

        Thread thread = new Thread(() -> {
            try {
                db.executeTransactionally("call apoc.periodic.iterate( " + "'match (p:Person) return p', "
                        + "'set p.name = p.name + \"ABCDEF\"', "
                        + "{batchSize:100, parallel:true, concurrency:20})");

            } catch (TransientTransactionFailureException e) {
                // this exception is expected due to killPeriodicQueryAsync
            }
        });
        thread.start();

        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);

        // wait until we've started processing by checking queryIds incrementing
        while (maxQueryId(kernelTransactions) < (totalNumberOfNodes / batchSizeCreate) + 20) {
            Thread.sleep(200);
        }

        PeriodicTestUtils.killPeriodicQueryAsync(db);
        thread.join();
    }

    private Long maxQueryId(KernelTransactions kernelTransactions) {
        LongStream longStream = kernelTransactions.activeTransactions().stream()
                .map(KernelTransactionHandle::executingQuery)
                .filter(Optional::isPresent)
                .mapToLong(executingQuery -> executingQuery.get().internalQueryId());
        return longStream.max().orElse(0l);
    }

    @Test
    public void testIteratePrefixGiven() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'WITH $p as p SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count")));
    }

    @Test
    public void testIterate() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count")));
    }

    @Test
    public void testIterateWithQueryPlanner() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        String cypherIterate = "match (p:Person) return p";
        String cypherAction = "SET p.lastname =p.name REMOVE p.name";
        testResult(
                db,
                "CALL apoc.periodic.iterate($cypherIterate, $cypherAction, $config)",
                map(
                        "cypherIterate",
                        cypherIterate,
                        "cypherAction",
                        cypherAction,
                        "config",
                        map("batchSize", 10, "planner", "DP")),
                result -> assertEquals(10L, Iterators.single(result).get("batches")));

        String cypherActionUnwind =
                "cypher runtime=slotted UNWIND $_batch AS batch WITH batch.p AS p  SET p.lastname =p.name";

        testResult(
                db,
                "CALL apoc.periodic.iterate($cypherIterate, $cypherActionUnwind, $config)",
                map(
                        "cypherIterate",
                        cypherIterate,
                        "cypherActionUnwind",
                        cypherActionUnwind,
                        "config",
                        map("batchSize", 10, "batchMode", "BATCH_SINGLE", "planner", "DP")),
                result -> assertEquals(10L, Iterators.single(result).get("batches")));
    }

    @Test
    public void testIterateUpdateStats() {
        testResult(
                db,
                "CALL apoc.periodic.iterate(" + "'UNWIND range(1, 100) AS x RETURN x', "
                        + "'CREATE (n:Node {x:x})"
                        + "   SET n.y = 1 "
                        + " CREATE (n)-[:SELF]->(n)',"
                        + "{ batchSize:10, parallel:true })",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertNotNull(row.get("updateStatistics"));
                    Map<String, Long> updateStats = (Map<String, Long>) row.get("updateStatistics");
                    assertNotNull(updateStats);
                    assertEquals(100, (long) updateStats.get("nodesCreated"));
                    assertEquals(0, (long) updateStats.get("nodesDeleted"));
                    assertEquals(100, (long) updateStats.get("relationshipsCreated"));
                    assertEquals(0, (long) updateStats.get("relationshipsDeleted"));
                    assertEquals(200, (long) updateStats.get("propertiesSet"));
                    assertEquals(100, (long) updateStats.get("labelsAdded"));
                    assertEquals(0, (long) updateStats.get("labelsRemoved"));
                });

        testResult(
                db,
                "CALL apoc.periodic.iterate(" + "'MATCH (n:Node) RETURN n', "
                        + "'REMOVE n:Node', "
                        + "{ batchSize:10, parallel:true })",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertNotNull(row.get("updateStatistics"));
                    Map<String, Long> updateStats = (Map<String, Long>) row.get("updateStatistics");
                    assertNotNull(updateStats);
                    assertEquals(0, (long) updateStats.get("nodesCreated"));
                    assertEquals(0, (long) updateStats.get("nodesDeleted"));
                    assertEquals(0, (long) updateStats.get("relationshipsCreated"));
                    assertEquals(0, (long) updateStats.get("relationshipsDeleted"));
                    assertEquals(0, (long) updateStats.get("propertiesSet"));
                    assertEquals(0, (long) updateStats.get("labelsAdded"));
                    assertEquals(100, (long) updateStats.get("labelsRemoved"));
                });

        testResult(
                db,
                "CALL apoc.periodic.iterate(" + "'MATCH (n) RETURN n', "
                        + "'DETACH DELETE n', "
                        + "{ batchSize:10, parallel:true })",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertNotNull(row.get("updateStatistics"));
                    Map<String, Long> updateStats = (Map<String, Long>) row.get("updateStatistics");
                    assertNotNull(updateStats);
                    assertEquals(0, (long) updateStats.get("nodesCreated"));
                    assertEquals(100, (long) updateStats.get("nodesDeleted"));
                    assertEquals(0, (long) updateStats.get("relationshipsCreated"));
                    assertEquals(100, (long) updateStats.get("relationshipsDeleted"));
                    assertEquals(0, (long) updateStats.get("propertiesSet"));
                    assertEquals(0, (long) updateStats.get("labelsAdded"));
                    assertEquals(0, (long) updateStats.get("labelsRemoved"));
                });
    }

    @Test
    public void testIteratePrefix() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count")));
    }

    @Test
    public void testIteratePassThroughBatch() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'UNWIND $_batch AS batch WITH batch.p AS p  SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true, batchMode: 'BATCH_SINGLE'})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count")));
    }

    @Test
    public void testIterateBatch() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname = p.name REMOVE p.name', {batchSize:10, iterateList:true, parallel:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count")));
    }

    @Test
    public void testIterateBatchPrefix() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");

        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname = p.name REMOVE p.name', {batchSize:10, iterateList:true, parallel:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count")));
    }

    @Test
    public void testIterateWithReportingFailed() {
        testResult(
                db,
                "CALL apoc.periodic.iterate('UNWIND range(-5, 5) AS x RETURN x', 'return sum(1000/x)', {batchSize:3, failedParams:9999})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(4L, row.get("batches"));
                    assertEquals(1L, row.get("failedBatches"));
                    assertEquals(11L, row.get("total"));
                    Map<String, List<Map<String, Object>>> failedParams =
                            (Map<String, List<Map<String, Object>>>) row.get("failedParams");
                    assertEquals(1, failedParams.size());
                    List<Map<String, Object>> failedParamsForBatch = failedParams.get("1");
                    assertEquals(3, failedParamsForBatch.size());

                    List<Object> values = stream(failedParamsForBatch.spliterator(), false)
                            .map(map -> map.get("x"))
                            .collect(toList());
                    assertEquals(values, Stream.of(-2l, -1l, 0l).collect(toList()));
                });
    }

    @Test
    public void testIterateRetries() {
        testResult(
                db, "CALL apoc.periodic.iterate('return 1', 'CREATE (n {prop: 1/$_retry})', {retries:1})", result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(1L, row.get("batches"));
                    assertEquals(1L, row.get("total"));
                    assertEquals(1L, row.get("retries"));
                });
    }

    @Test
    public void testIterateFail() {
        db.executeTransactionally("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})");
        testResult(
                db,
                "CALL apoc.periodic.iterate('match (p:Person) return p', 'WITH $p as p SET p.lastname = p.name REMOVE x.name', {batchSize:10,parallel:true})",
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(10L, row.get("batches"));
                    assertEquals(100L, row.get("total"));
                    assertEquals(100L, row.get("failedOperations"));
                    assertEquals(0L, row.get("committedOperations"));
                    Map<String, Object> failedParams = (Map<String, Object>) row.get("failedParams");
                    assertTrue(failedParams.isEmpty());
                });

        testCall(
                db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(0L, row.get("count")));
    }

    @Test
    public void testIterateWithNullRebind() {
        testResult(
                db,
                """
                CALL apoc.periodic.iterate(
                    "WITH {a: null, b: {c: 1}, c: [null]} as value RETURN value",
                    "RETURN 1",
                     {})
                """,
                result -> {
                    Map<String, Object> row = Iterators.single(result);
                    assertEquals(1L, row.get("batches"));
                    assertEquals(1L, row.get("total"));
                    assertEquals(0L, row.get("failedBatches"));
                });
    }

    @Test
    public void testCountdown() {
        int startValue = 3;
        int rate = 1;

        db.executeTransactionally(
                "CREATE (counter:Counter {c: $startValue})", Collections.singletonMap("startValue", startValue));
        String statementToRepeat = "MATCH (counter:Counter) SET counter.c = counter.c - 1 RETURN counter.c as count";

        Map<String, Object> params = map("statement", statementToRepeat, "rate", rate);
        testResult(db, "CALL apoc.periodic.countdown('decrement', $statement, $rate)", params, r -> {
            try {
                // Number of iterations per rate (in seconds)
                Thread.sleep(startValue * rate * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            long count = TestUtil.singleResultFirstColumn(db, "MATCH (counter:Counter) RETURN counter.c as c");
            assertEquals(0L, count);
            r.close();
        });
    }

    @Test
    public void testRepeatParams() {
        db.executeTransactionally(
                "CALL apoc.periodic.repeat('repeat-params', 'MERGE (person:Person {name: $nameValue})', 2, {params: {nameValue: 'John Doe'}} ) YIELD name RETURN name");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {

        }

        testCall(
                db,
                "MATCH (p:Person {name: 'John Doe'}) RETURN p.name AS name",
                row -> assertEquals(row.get("name"), "John Doe"));
    }

    private long tryReadCount(int maxAttempts, String statement, long expected) throws InterruptedException {
        int attempts = 0;
        long count;
        do {
            Thread.sleep(100);
            attempts++;
            count = TestUtil.singleResultFirstColumn(db, statement);
            System.out.println("for " + statement + " we have " + count + " results");
        } while (attempts < maxAttempts && count != expected);
        return count;
    }

    @Test
    public void testCommitFail() {
        final String query =
                "CALL apoc.periodic.commit('UNWIND range(0,1000) as id WITH id CREATE (::Foo {id: id}) limit 1000', {})";
        testCypherFail(query);
    }

    @Test
    public void testSubmitFail() {
        final String query = "CALL apoc.periodic.submit('foo','create (::Foo)')";
        testCypherFail(query);
    }

    @Test
    public void testRepeatFail() {
        final String query =
                "CALL apoc.periodic.repeat('repeat-params', 'MERGE (person:Person {name: $nameValue})', 2, {params: {nameValue: 'John Doe'}}) YIELD name RETURN nam";
        testCypherFail(query);
    }

    @Test
    public void testCountdownFail() {
        final String query =
                "CALL apoc.periodic.countdown('decrement', 'MATCH (counter:Counter) SET counter.c == counter.c - 1 RETURN counter.c as count', 1)";
        testCypherFail(query);
    }

    @Test
    public void testIterateQueryFail() {
        final String query = "CALL apoc.periodic.iterate('UNWIND range(0, 1000) as id RETURN ids', "
                + "'WITH $id as id CREATE (:Foo {id: $id})', "
                + "{batchSize:1,parallel:true})";
        testCypherFail(query);
    }

    @Test
    public void testIterateQueryFailInvalidConcurrency() {
        final String query = "CALL apoc.periodic.iterate('UNWIND range(0, 10) AS x RETURN x', " + "'RETURN x', "
                + "{concurrency:0 ,parallel:true})";

        QueryExecutionException e = assertThrows(QueryExecutionException.class, () -> testCall(db, query, (r) -> {}));
        assertTrue(e.getMessage().contains("concurrency parameter must be > 0"));
    }

    @Test
    public void testIterateQueryFailInvalidBatchSize() {
        final String query = "CALL apoc.periodic.iterate('UNWIND range(0, 10) AS x RETURN x', " + "'RETURN x', "
                + "{batchSize:0 ,parallel:true})";

        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testCall(db, query, row -> fail("The test should fail but it didn't")));
        assertTrue(e.getMessage().contains("batchSize parameter must be > 0"));
    }

    @Test
    public void testTruncate() {
        createDatasetForTruncate();

        TestUtil.testCallEmpty(db, "CALL apoc.periodic.truncate", Collections.emptyMap());
        assertCountEntitiesAndIndexes(0, 0, 4, 2);

        dropSchema();

        assertCountEntitiesAndIndexes(0, 0, 0, 0);
    }

    @Test
    public void testTruncateWithDropSchema() {
        createDatasetForTruncate();

        TestUtil.testCallEmpty(db, "CALL apoc.periodic.truncate({dropSchema: true})", Collections.emptyMap());
        assertCountEntitiesAndIndexes(0, 0, 0, 0);
    }

    @Test
    public void testMergeNodesInApocPeriodicIterate() {
        db.executeTransactionally("UNWIND range(1,1000) as i CREATE (p1:Person) RETURN 1");
        final var query =
                """
        CALL apoc.periodic.iterate(
          'MATCH (p:Person) RETURN p',
          'CALL apoc.refactor.mergeNodes([item in $_batch | item.p]) YIELD node RETURN node',
          {batchSize: 100, parallel: false}
        )
        YIELD batch, operations
        """;
        final var expected = Map.of(
                "batch", Map.of("committed", 10L, "errors", Map.of(), "failed", 0L, "total", 10L),
                "operations", Map.of("committed", 1000L, "errors", Map.of(), "failed", 0L, "total", 1000L));
        testCall(db, query, map(), (r) -> {
            // The important assertion is that we don't cause any deadlocks or exceptions
            assertThat(r).containsExactlyInAnyOrderEntriesOf(expected);
        });
    }

    private void dropSchema() {
        try (Transaction tx = db.beginTx()) {
            Schema schema = tx.schema();
            schema.getConstraints().forEach(ConstraintDefinition::drop);
            schema.getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    private void createDatasetForTruncate() {
        // drop schema to remove existing default token indexes if any
        dropSchema();

        int iterations = 999;
        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("iterations", iterations);
        db.executeTransactionally(
                "UNWIND range(1,$iterations) AS x CREATE (:One{name:'Person_'+x})-[:FOO {id: x}]->(:Two {surname: 'Two'+x})<-[:BAR {idBar: x}]-(:Three {other: x+'Three'})",
                parameters);

        db.executeTransactionally("CREATE INDEX FOR (n:One) ON (n.name)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (a:Two) REQUIRE a.surname IS UNIQUE");
        db.executeTransactionally("CREATE INDEX FOR (n:Three) ON (n.other)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (a:Actor) REQUIRE a.name IS UNIQUE");

        final int expectedNodes = iterations * 3;
        final int expectedRels = iterations * 2;
        assertCountEntitiesAndIndexes(expectedNodes, expectedRels, 4, 2);
    }

    private void assertCountEntitiesAndIndexes(
            long expectedNodes, long expectedRels, long expectedIndexes, long expectedContraints) {
        try (Transaction tx = db.beginTx()) {
            assertEquals(expectedNodes, count(tx.getAllNodes()));
            assertEquals(expectedRels, count(tx.getAllRelationships()));
            Schema schema = tx.schema();
            assertEquals(expectedIndexes, count(schema.getIndexes()));
            assertEquals(expectedContraints, count(schema.getConstraints()));
        }
    }

    private void testCypherFail(String query) {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testCall(db, query, row -> fail("The test should fail but it didn't")));
        assertTrue(ExceptionUtils.getRootCause(e) instanceof org.neo4j.exceptions.SyntaxException);
    }

    @Test
    public void testDifferentCypherVersionsApocPeriodicCommit() {
        int id = 0;
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.periodic.commit('%s CREATE (n:$(apoc.cypherVersion()) {id: %d}) RETURN 0 LIMIT 1')",
                    cypherVersion.outerVersion, cypherVersion.innerVersion, id);
            db.executeTransactionally(query);
            // Check the node was created with the right label
            var checkerQuery =
                    String.format("MATCH (n:%s {id : %d}) RETURN count(n) AS count", cypherVersion.result, id);
            testCall(db, checkerQuery, r -> assertEquals(1L, r.get("count")));
            id++;
        }
    }

    @Test
    public void testDifferentCypherVersionsApocPeriodicCountdown() throws InterruptedException {
        int id = 0;
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.periodic.countdown('test', '%s CREATE (n:$(apoc.cypherVersion()) {id: %d}) RETURN 0 LIMIT 1', 0)",
                    cypherVersion.outerVersion, cypherVersion.innerVersion, id);
            db.executeTransactionally(query);
            Thread.sleep(2000); // Wait 3s to make sure the countdown has been called
            // Check the node was created with the right label
            var checkerQuery =
                    String.format("MATCH (n:%s {id : %d}) RETURN count(n) AS count", cypherVersion.result, id);
            testCall(db, checkerQuery, r -> assertEquals(1L, r.get("count")));
            id++;
        }
    }

    @Test
    public void testDifferentCypherVersionsApocPeriodicIterate() {
        db.executeTransactionally("CREATE (:CYPHER_5 {id: -1}), (:CYPHER_25 {id: -1})");
        int id = 0;
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.periodic.iterate('%s MATCH (p:$(apoc.cypherVersion())) RETURN p', 'SET p.id = %d', {})",
                    cypherVersion.outerVersion, cypherVersion.innerVersion, id);
            db.executeTransactionally(query);
            // Check the node was created with the right label
            var checkerQuery =
                    String.format("MATCH (n:%s {id: %d}) RETURN count(n) AS count", cypherVersion.result, id);
            testCall(db, checkerQuery, r -> assertEquals(1L, r.get("count")));
            id++;
        }
    }

    @Test
    public void testDifferentCypherVersionsApocPeriodicRepeat() throws InterruptedException {
        int id = 0;
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.periodic.repeat('test%d', '%s MERGE (n:$(apoc.cypherVersion()) {id: %d})', 1)",
                    cypherVersion.outerVersion, id, cypherVersion.innerVersion, id);
            db.executeTransactionally(query);
            Thread.sleep(3000); // Wait 3s to make sure the repeat has been called
            db.executeTransactionally(String.format("CALL apoc.periodic.cancel('test%d')", id));
            // Check the node was created with the right label
            var checkerQuery =
                    String.format("MATCH (n:%s {id : %d}) RETURN count(n) AS count", cypherVersion.result, id);
            testCall(db, checkerQuery, r -> assertEquals(1L, r.get("count")));
            id++;
        }
    }

    @Test
    public void testDifferentCypherVersionsApocPeriodicSubmit() throws InterruptedException {
        int id = 0;
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.periodic.submit('test%d', '%s CREATE (n:$(apoc.cypherVersion()) {id: %d})')",
                    cypherVersion.outerVersion, id, cypherVersion.innerVersion, id);
            db.executeTransactionally(query);
            Thread.sleep(1000); // Wait 1s to make sure the submit has been called
            // Check the node was created with the right label
            var checkerQuery =
                    String.format("MATCH (n:%s {id : %d}) RETURN count(n) AS count", cypherVersion.result, id);
            testCall(db, checkerQuery, r -> assertEquals(1L, r.get("count")));
            id++;
        }
    }

    @Test
    void concurrentRepeat() throws InterruptedException {
        try (final var executors = Executors.newWorkStealingPool()) {
            for (var i = 0; i < 2048; i++) {
                executors.submit(() -> db.executeTransactionally(
                        "CALL apoc.periodic.repeat('repeat-concurrent', 'CALL test.tick()', 1)"));
            }
            executors.shutdown();
            assertThat(executors.awaitTermination(5, TimeUnit.MINUTES)).isTrue();
        }
        try (final var tx = db.beginTx();
                final var res = tx.execute("CALL apoc.periodic.cancel('repeat-concurrent')")) {
            assertThat(res.stream()).hasSize(1);
        }
        Thread.sleep(1_000); // Allow time for the cancel to have effect.
        final var ticks = TickTockProcedure.counter.get();
        Thread.sleep(4_000);
        assertThat(TickTockProcedure.counter.get()).isEqualTo(ticks);
    }

    public static class TickTockProcedure {
        public static final AtomicLong counter = new AtomicLong();

        @Procedure(name = "test.tick")
        public void tick() {
            counter.incrementAndGet();
        }
    }
}
