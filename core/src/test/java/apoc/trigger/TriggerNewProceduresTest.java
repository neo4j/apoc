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
package apoc.trigger;

import static apoc.trigger.TriggerNewProcedures.*;
import static apoc.trigger.TriggerTestUtil.TIMEOUT;
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.trigger.TriggerTestUtil.awaitTriggerDiscovered;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertEventually;

import apoc.HelperProcedures;
import apoc.nodes.Nodes;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class TriggerNewProceduresTest {
    private static final File directory = new File("target/conf");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    // we cannot set via ApocConfig.apocConfig().setProperty("apoc.trigger.refresh", "2000") in `setUp`, because is too
    // late
    @ClassRule
    public static final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(
                    "apoc.trigger.refresh", String.valueOf(TRIGGER_DEFAULT_REFRESH))
            .and("apoc.trigger.enabled", "true");

    @BeforeClass
    public static void beforeClass() {
        databaseManagementService = new TestDatabaseManagementServiceBuilder(
                        storeDir.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"))
                .setConfig(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true)
                .build();
        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        // Procedures are global for the DBMS, so it is sufficient to register them via one DB.
        TestUtil.registerProcedure(db, TriggerNewProcedures.class, Nodes.class, Trigger.class);
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }

    @After
    public void after() {
        sysDb.executeTransactionally("CALL apoc.trigger.dropAll('neo4j')");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    //
    // test cases taken and adapted from TriggerTest.java
    //

    @Test
    public void testListTriggers() {
        String name = "count-removals";
        String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";
        testCallCount(
                sysDb, "CALL apoc.trigger.install('neo4j', $name, $query,{})", map("query", query, "name", name), 1);

        testCallEventually(
                db,
                "CALL apoc.trigger.list",
                row -> {
                    assertEquals("count-removals", row.get("name"));
                    assertTrue(row.get("query").toString().contains(query));
                    assertEquals(true, row.get("installed"));
                },
                TIMEOUT);
    }

    @Test
    public void testRemoveNode() {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)");
        final String name = "count-removals";
        final String query = "MATCH (c:Counter) SET c.count = c.count + size($deletedNodes)";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (f:Foo) DELETE f");
        testCallEventually(
                db, "MATCH (c:Counter) RETURN c.count as count", (row) -> assertEquals(1L, row.get("count")), TIMEOUT);
    }

    @Test
    public void testDropStopAndStartNotExistentTrigger() {
        testCallEmpty(
                sysDb,
                "CALL apoc.trigger.stop('neo4j', $name)",
                map("name", UUID.randomUUID().toString()));

        testCallEmpty(
                sysDb,
                "CALL apoc.trigger.start('neo4j', $name)",
                map("name", UUID.randomUUID().toString()));

        testCallEmpty(
                sysDb,
                "CALL apoc.trigger.drop('neo4j', $name)",
                map("name", UUID.randomUUID().toString()));
    }

    @Test
    public void testOverwriteTrigger() {
        final String name = UUID.randomUUID().toString();

        String queryOne = "RETURN 111";
        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query, {})",
                map("name", name, "query", queryOne),
                r -> {
                    assertEquals(name, r.get("name"));
                    assertTrue(r.get("query").toString().contains(queryOne));
                });

        String queryTwo = "RETURN 999";
        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query, {})",
                map("name", name, "query", queryTwo),
                r -> {
                    assertEquals(name, r.get("name"));
                    assertTrue(r.get("query").toString().contains(queryTwo));
                });
    }

    @Test
    public void testIssue2247() {
        db.executeTransactionally("CREATE (n:ToBeDeleted)");
        final String name = "myTrig";
        final String query = "RETURN 1";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (n:ToBeDeleted) DELETE n");
        sysDb.executeTransactionally("CALL apoc.trigger.drop('neo4j', 'myTrig')");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
    }

    @Test
    public void testRemoveRelationship() {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)-[:X]->(f)");

        String name = "count-removed-rels";
        String query = "MATCH (c:Counter) SET c.count = c.count + size($deletedRelationships)";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (f:Foo) DETACH DELETE f");
        testCall(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> assertEquals(1L, row.get("count")));
    }

    @Test
    public void testRemoveTrigger() {
        testCallCount(sysDb, "CALL apoc.trigger.install('neo4j', 'to-be-removed','RETURN 1',{})", 1);
        testCallEventually(
                db,
                "CALL apoc.trigger.list()",
                (row) -> {
                    assertEquals("to-be-removed", row.get("name"));
                    assertTrue(row.get("query").toString().contains("RETURN 1"));
                    assertEquals(true, row.get("installed"));
                },
                TIMEOUT);

        testCall(sysDb, "CALL apoc.trigger.drop('neo4j', 'to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertTrue(row.get("query").toString().contains("RETURN 1"));
            assertEquals(false, row.get("installed"));
        });
        testCallCountEventually(db, "CALL apoc.trigger.list()", 0, TIMEOUT);

        testCallEmpty(sysDb, "CALL apoc.trigger.drop('neo4j', 'to-be-removed')", Collections.emptyMap());
    }

    @Test
    public void testRemoveAllTrigger() {
        testCallCount(sysDb, "CALL apoc.trigger.dropAll('neo4j')", 0);
        testCallCountEventually(db, "call apoc.trigger.list", 0, TIMEOUT);

        testCallCount(sysDb, "CALL apoc.trigger.install('neo4j', 'to-be-removed-1','RETURN 1',{})", 1);
        testCallCount(sysDb, "CALL apoc.trigger.install('neo4j', 'to-be-removed-2','RETURN 2',{})", 1);

        testCallCountEventually(db, "call apoc.trigger.list", 2, TIMEOUT);

        TestUtil.testResult(sysDb, "CALL apoc.trigger.dropAll('neo4j')", (res) -> {
            Map<String, Object> row = res.next();
            assertEquals("to-be-removed-1", row.get("name"));
            assertTrue(row.get("query").toString().contains("RETURN 1"));
            assertEquals(false, row.get("installed"));
            row = res.next();
            assertEquals("to-be-removed-2", row.get("name"));
            assertTrue(row.get("query").toString().contains("RETURN 2"));
            assertEquals(false, row.get("installed"));
            assertFalse(res.hasNext());
        });
        testCallCountEventually(db, "call apoc.trigger.list", 0, TIMEOUT);

        testCallCount(sysDb, "CALL apoc.trigger.dropAll('neo4j')", 0);
    }

    @Test
    public void testTimeStampTrigger() {
        String name = "timestamp";
        String query = "UNWIND $createdNodes AS n SET n.ts = timestamp()";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (f:Foo)");
        testCall(db, "MATCH (f:Foo) RETURN f", (row) -> assertTrue(((Node) row.get("f")).hasProperty("ts")));
    }

    @Test
    public void testTxId() {
        final long start = System.currentTimeMillis();
        db.executeTransactionally("CREATE (f:Another)");
        final String name = "txinfo";
        final String query =
                """
                UNWIND $createdNodes AS n
                MATCH (a:Another) WITH a, n
                SET a.txId = $transactionId, a.txTime = $commitTime""";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase:'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);
        db.executeTransactionally("CREATE (f:Bar)");
        TestUtil.testCall(db, "MATCH (f:Another) RETURN f", (row) -> {
            assertTrue((Long) ((Node) row.get("f")).getProperty("txId") > -1L);
            assertTrue((Long) ((Node) row.get("f")).getProperty("txTime") > start);
        });
        db.executeTransactionally("MATCH (n:Another) DELETE n");
    }

    @Test
    public void testTxIdWithRelationshipsAndAfter() {
        db.executeTransactionally("CREATE (:RelationshipCounter {count:0})");
        String name = UUID.randomUUID().toString();

        // We need to filter $createdRelationships, i.e. `size(..) > 0`, not to cause a deadlock
        String query =
                """
                 WITH size($createdRelationships) AS sizeRels
                 WHERE sizeRels > 0
                 MATCH (c:RelationshipCounter)
                 SET c.txId = $transactionId  SET c.count = c.count + sizeRels""";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (a:A)<-[r:C]-(b:B)");

        testCall(db, "MATCH (n:RelationshipCounter) RETURN n", this::testTxIdWithRelsAssertionsCommon);
    }

    @Test
    public void testTxIdWithRelationshipsAndAfterAsync() {
        testTxIdWithRelationshipsCommon("afterAsync");

        testCallEventually(db, "MATCH (n:RelationshipCounter) RETURN n", this::testTxIdWithRelsAssertionsCommon, 10);
    }

    private void testTxIdWithRelsAssertionsCommon(Map<String, Object> r) {
        final Node n = (Node) r.get("n");
        assertEquals(1L, n.getProperty("count"));
        final long txId = (long) n.getProperty("txId");
        assertTrue("Current txId is: " + txId, txId > -1L);
    }

    @Test
    public void testTxIdWithRelationshipsAndBefore() {
        testTxIdWithRelationshipsCommon("before");

        testCall(db, "MATCH (n:RelationshipCounter) RETURN n", r -> {
            final Node n = (Node) r.get("n");
            assertEquals(-1L, n.getProperty("txId"));
            assertEquals(1L, n.getProperty("count"));
        });
    }

    private static void testTxIdWithRelationshipsCommon(String phase) {
        String query =
                """
                MATCH (c:RelationshipCounter)
                  SET c.count = c.count + size($createdRelationships)
                  SET c.txId = $transactionId""";

        db.executeTransactionally("CREATE (:RelationshipCounter {count:0})");
        String name = UUID.randomUUID().toString();

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install( 'neo4j', $name, $query, {phase: $phase})",
                Map.of("name", name, "query", query, "phase", phase));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("CREATE (a:A)<-[r:C]-(b:B)");
    }

    @Test
    public void testMetaDataBefore() {
        String triggerQuery = "UNWIND $createdNodes AS n SET n.label = labels(n)[0], n += $metaData";
        String matchQuery = "MATCH (n:Bar) RETURN n";
        testMetaData(triggerQuery, "before", matchQuery);
    }

    @Test
    public void testMetaDataAfter() {
        db.executeTransactionally("CREATE (n:Another)");
        String triggerQuery = "UNWIND $createdNodes AS n MATCH (a:Another) SET a.label = labels(n)[0], a += $metaData";
        String matchQuery = "MATCH (n:Another) RETURN n";
        testMetaData(triggerQuery, "after", matchQuery);
    }

    private void testMetaData(String triggerQuery, String phase, String matchQuery) {
        String name = "txinfo";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase:$phase})",
                Map.of("name", name, "query", triggerQuery, "phase", phase));
        awaitTriggerDiscovered(db, name, triggerQuery);

        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            ktx.setMetaData(Collections.singletonMap("txMeta", "hello"));
            tx.execute("CREATE (f:Bar)");
            tx.commit();
        }

        testCall(db, matchQuery, (row) -> {
            final Node node = (Node) row.get("n");
            assertEquals("Bar", node.getProperty("label"));
            assertEquals("hello", node.getProperty("txMeta"));
        });
    }

    @Test
    public void testPauseResult() {
        String name = "pausedTest";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);
        testCall(sysDb, "CALL apoc.trigger.stop('neo4j', 'pausedTest')", (row) -> {
            assertEquals("pausedTest", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    public void testPauseOnCallList() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        testCallEventually(
                db,
                "CALL apoc.trigger.list()",
                (row) -> {
                    assertEquals("test", row.get("name"));
                    assertEquals(true, row.get("installed"));
                    assertEquals(true, row.get("paused"));
                },
                TIMEOUT);
    }

    @Test
    public void testResumeResult() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        testCallEventually(db, "CALL apoc.trigger.list", row -> assertEquals(true, row.get("paused")), TIMEOUT);
        testCall(sysDb, "CALL apoc.trigger.start('neo4j', 'test')", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(false, row.get("paused"));
        });
        testCallEventually(db, "CALL apoc.trigger.list", row -> assertEquals(false, row.get("paused")), TIMEOUT);
    }

    @Test
    public void testTriggerPause() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        awaitTriggerDiscovered(db, name, query, true);

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertFalse(((Node) row.get("f")).hasProperty("txId"));
            assertFalse(((Node) row.get("f")).hasProperty("txTime"));
            assertTrue(((Node) row.get("f")).hasProperty("name"));
        });
    }

    @Test
    public void testTriggerResume() {
        String name = "test";
        String query = "UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', 'test')");
        awaitTriggerDiscovered(db, name, query, true);

        sysDb.executeTransactionally("CALL apoc.trigger.start('neo4j', 'test')");
        awaitTriggerDiscovered(db, name, query, false);

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue(((Node) row.get("f")).hasProperty("txId"));
            assertTrue(((Node) row.get("f")).hasProperty("txTime"));
            assertTrue(((Node) row.get("f")).hasProperty("name"));
        });
    }

    @Test
    public void testCreatedRelationshipsAsync() {
        db.executeTransactionally("CREATE (:A {name: \"A\"})-[:R1]->(:Z {name: \"Z\"})");
        String name = "trigger-after-async";
        final String query =
                """
                UNWIND $createdRelationships AS r
                MATCH (a:A)-[r]->(z:Z)
                WHERE type(r) IN ["R2", "R3"]
                MATCH (a)-[r1:R1]->(z)
                SET r1.triggerAfterAsync = true""";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"})\n" + "MERGE (a)-[:R2]->(z)");

        assertEventually(
                () -> db.executeTransactionally("MATCH ()-[r:R1]->() RETURN r", Map.of(), result ->
                        (boolean) result.<Relationship>columnAs("r").next().getProperty("triggerAfterAsync", false)),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteRelationshipsAsync() {
        db.executeTransactionally(
                "CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        String name = "trigger-after-async-1";
        final String query =
                """
                UNWIND $deletedRelationships AS r
                MATCH (a)-[r1:R1]->(z)
                SET a.alpha = apoc.any.property(r, "alpha"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *""";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    public void testDeleteRelationshipsAsyncWithCreationInQuery() {
        db.executeTransactionally(
                "CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        String name = "trigger-after-async-2";
        final String query =
                """
                UNWIND $deletedRelationships AS r
                MATCH (a)-[r1:R1]->(z)
                SET a.alpha = apoc.any.property(r, "alpha"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *""";
        db.executeTransactionally(
                "CALL apoc.trigger.add($name, $query, {phase: 'afterAsync'})", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    public void testDeleteNodesAsync() {
        db.executeTransactionally(
                "CREATE (a:A {name: 'A'})-[:R1 {omega: 3}]->(z:Z {name: 'Z'}), (:R2:Other {alpha: 1})");
        String name = "trigger-after-async-3";
        final String query =
                """
                UNWIND $deletedNodes AS n
                MATCH (a)-[r1:R1]->(z)
                SET a.alpha = apoc.any.property(n, "alpha"), r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = "R2" RETURN *""";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', 'trigger-after-async-3', $query, {phase: 'afterAsync'})",
                map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    @Test
    public void testDeleteNodesAsyncWithCreationQuery() {
        db.executeTransactionally("CREATE (:R2:Other {alpha: 1})");
        String name = "trigger-after-async-4";
        final String query =
                """
                UNWIND $deletedNodes AS n
                CREATE (a:A)-[r1:R1 {omega: 3}]->(z:Z)
                SET a.alpha = 1, r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = apoc.node.labels(n)[0] RETURN *""";

        db.executeTransactionally(
                "CALL apoc.trigger.add($name, $query, {phase: 'afterAsync'})", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    private void commonDeleteAfterAsync(String deleteQuery) {
        db.executeTransactionally(deleteQuery);

        assertEventually(
                () -> db.executeTransactionally("MATCH (a:A {alpha: 1})-[r:R1]->() RETURN r", Map.of(), result -> {
                    final ResourceIterator<Relationship> relIterator = result.columnAs("r");
                    return relIterator.hasNext();
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    public void testDeleteRelationships() {
        db.executeTransactionally("CREATE (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"}), (a)-[:R2]->(z)");
        String name = "trigger-after-async-3";
        final String query =
                """
                UNWIND $deletedRelationships AS r
                MERGE (a:AA{name: "AA"})
                SET a.triggerAfter = size($deletedRelationships) = 1, a.deleted = type(r)""";
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: 'after'})",
                Map.of("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);

        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[r:R2]->(z:Z {name: \"Z\"})\n" + "DELETE r");

        assertEventually(
                () -> db.executeTransactionally("MATCH (a:AA) RETURN a", Map.of(), result -> {
                    final Node r = result.<Node>columnAs("a").next();
                    return (boolean) r.getProperty("triggerAfter", false)
                            && r.getProperty("deleted", "").equals("R2");
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    //
    // new test cases
    //

    @Test
    public void testTriggerShouldNotCauseCascadeTransactionWithPhaseBefore() {
        testCascadeTransactionCommon("before");
    }

    @Test
    public void testTriggerShouldNotCauseCascadeTransactionWithPhaseAfter() {
        testCascadeTransactionCommon("after");
    }

    @Test
    public void testTriggerShouldNotCauseCascadeTransactionWithPhaseAfterAsync() {
        testCascadeTransactionCommon("afterAsync");
    }

    private static void testCascadeTransactionCommon(String phase) {
        db.executeTransactionally("CREATE (:TransactionCounter {count:0});");

        // create the trigger
        String query = "MATCH (c:TransactionCounter) SET c.count = c.count + 1";
        String name = "count-relationships";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {phase: $phase})",
                Map.of("name", name, "query", query, "phase", phase));

        awaitTriggerDiscovered(db, name, query);

        String countQuery = "MATCH (n:TransactionCounter) RETURN n.count as count";
        long countBefore = singleResultFirstColumn(db, countQuery);
        assertEquals(0L, countBefore);

        // activate the trigger handler
        db.executeTransactionally("CREATE (a:A)");

        // assert that `count` eventually increment by 1
        assertEventually(
                () -> (long) singleResultFirstColumn(db, countQuery), (value) -> value == 1L, 10L, TimeUnit.SECONDS);

        // assert that `count` remains 1 and doesn't increment in subsequent transactions
        for (int i = 0; i < 10; i++) {
            long count = singleResultFirstColumn(db, countQuery);
            assertEquals(1L, count);
            Util.sleep(100);
        }
    }

    @Test
    public void testTriggerShow() {
        String name = "test-show1";
        String name2 = "test-show2";
        String query = "MATCH (c:TestShow) SET c.count = 1";

        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query,{}) YIELD name",
                map("query", query, "name", name),
                r -> assertEquals(name, r.get("name")));

        testCall(
                sysDb,
                "CALL apoc.trigger.install('neo4j', $name, $query,{}) YIELD name",
                map("query", query, "name", name2),
                r -> assertEquals(name2, r.get("name")));

        // not updated
        testResult(
                sysDb,
                "CALL apoc.trigger.show('neo4j') " + "YIELD name, query RETURN * ORDER BY name",
                map("query", query, "name", name),
                res -> {
                    Map<String, Object> row = res.next();
                    assertEquals(name, row.get("name"));
                    assertTrue(row.get("query").toString().contains(query));
                    row = res.next();
                    assertEquals(name2, row.get("name"));
                    assertTrue(row.get("query").toString().contains(query));
                    assertFalse(res.hasNext());
                });
    }

    @Test
    public void testInstallTriggerInUserDb() {
        try {
            testCall(
                    db,
                    "CALL apoc.trigger.install('neo4j', 'userDb', 'RETURN 1',{})",
                    r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(TRIGGER_NOT_ROUTED_ERROR));
        }
    }

    @Test
    public void testShowTriggerInUserDb() {
        try {
            testCall(db, "CALL apoc.trigger.show('neo4j')", r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertTrue(e.getMessage().contains(NON_SYS_DB_ERROR));
        }
    }

    @Test
    public void testInstallTriggerInNotExistentDb() {
        final String dbName = "nonexistent";
        try {
            testCall(
                    sysDb,
                    "CALL apoc.trigger.install($dbName, 'dbNotFound', 'SHOW DATABASES', {})",
                    Map.of("dbName", dbName),
                    r -> fail(""));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(String.format(DB_NOT_FOUND_ERROR, dbName)));
        }
    }

    @Test
    public void testInstallTriggerInSystemDb() {
        try {
            testCall(sysDb, "CALL apoc.trigger.install('system', 'name', 'SHOW DATABASES', {})", r -> fail(""));
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(TRIGGER_BAD_TARGET_ERROR));
        }
    }

    @Test
    public void testEventualConsistency() {
        long count = 0L;
        final String name = UUID.randomUUID().toString();
        final String query = "UNWIND $createdNodes AS n SET n.count = " + count;

        // this does nothing, just to test consistency with multiple triggers
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, 'return 1', {})",
                map("name", UUID.randomUUID().toString()));

        // create trigger
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query);
        db.executeTransactionally("CREATE (n:Something)");

        // check trigger
        testCall(db, "MATCH (c:Something) RETURN c.count as count", (row) -> assertEquals(count, row.get("count")));

        // stop trigger
        sysDb.executeTransactionally("CALL apoc.trigger.stop('neo4j', $name)", map("name", name, "query", query));
        awaitTriggerDiscovered(db, name, query, true);

        // this does nothing, just to test consistency with multiple triggers
        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, 'return 1', {})",
                map("name", UUID.randomUUID().toString()));
    }

    @Test
    public void testCypherVersions() {
        int id = 0;
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            String name = "cypher-versions-" + id;
            String triggerQuery =
                    "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";

            var query = String.format(
                    "%s CALL apoc.trigger.install('neo4j', $name, $query,{})", cypherVersion.outerVersion);
            sysDb.executeTransactionally(
                    query, map("name", name, "query", cypherVersion.innerVersion + " " + triggerQuery));

            // Check the query got given the correct Cypher Version
            testCallEventually(
                    db,
                    "CALL apoc.trigger.list() YIELD name, query, installed WHERE name = $name RETURN *",
                    map("name", name),
                    row -> {
                        assertTrue(row.get("query").toString().contains(triggerQuery));
                        assertTrue(row.get("query").toString().contains(cypherVersion.result.replace('_', ' ')));
                        assertEquals(true, row.get("installed"));
                    },
                    5L);
            id++;
        }
    }
}
