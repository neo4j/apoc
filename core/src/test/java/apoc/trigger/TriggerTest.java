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

import static apoc.trigger.TriggerTestUtil.TIMEOUT;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.assertExceptionMessageContains;
import static apoc.util.TestUtil.testCallCountEventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.nodes.Nodes;
import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

/**
 * CYPHER 5 only; moved to extended for Cypher 25
 */
@EnterpriseDbmsExtension(configurationCallback = "configure", createDatabasePerTest = false)
class TriggerTest {

    @Inject
    GraphDatabaseService db;

    GraphDatabaseService sysDb;

    @Inject
    DatabaseManagementService dbms;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        System.setProperty("apoc.trigger.enabled", "true");
        builder.setConfigRaw(Map.of("internal.dbms.debug.track_cursor_close", "true"));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5);
    }

    @BeforeAll
    void beforeAll() {
        start = System.currentTimeMillis();
        this.sysDb = dbms.database("system");
        TestUtil.registerProcedure(db, Nodes.class, Trigger.class);
    }

    @BeforeEach
    void beforeEach() {
        start = System.currentTimeMillis();
    }

    @AfterEach
    void after() {
        db.executeTransactionally("CALL apoc.trigger.removeAll()");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    private long start;

    @Test
    void testInstallTriggerInSystemDb() {
        // Can't add triggers for system because apoc.trigger.add is does not have the @System annotation
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> sysDb.executeTransactionally("CALL apoc.trigger.add('name', 'SHOW DATABASES', {})"));
        assertExceptionMessageContains(e, "The following unsupported clauses were used: CALL apoc.trigger.add.");
    }

    @Test
    void testListTriggers() {
        String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";

        TestUtil.testCallCount(
                db, "CALL apoc.trigger.add('count-removals',$query,{}) YIELD name RETURN name", map("query", query), 1);
        TestUtil.testCall(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("count-removals", row.get("name"));
            assertTrue(row.get("query").toString().contains(query));
            assertEquals(true, row.get("installed"));
        });
    }

    @Test
    void testRemoveNode() {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)");
        final String triggerQuery = "MATCH (c:Counter) SET c.count = c.count + size($deletedNodes)";
        db.executeTransactionally("CALL apoc.trigger.add('count-removals', $query, {})", map("$query", triggerQuery));

        // Wait for trigger
        TriggerTestUtil.awaitTriggerDiscovered(db, "count-removals", triggerQuery);

        db.executeTransactionally("MATCH (f:Foo) DELETE f");

        TestUtil.testCallEventually(
                db, "MATCH (c:Counter) RETURN c.count as count", (row) -> assertEquals(1L, row.get("count")), TIMEOUT);
    }

    @Test
    void testIssue2247() {
        db.executeTransactionally("CREATE (n:ToBeDeleted)");
        db.executeTransactionally("CALL apoc.trigger.add('myTrig', 'RETURN 1', {phase: 'afterAsync'})");

        db.executeTransactionally("MATCH (n:ToBeDeleted) DELETE n");

        db.executeTransactionally("CALL apoc.trigger.remove('myTrig')");
    }

    @Test
    void testRemoveRelationship() {
        db.executeTransactionally("CREATE (:Counter {count:0})");
        db.executeTransactionally("CREATE (f:Foo)-[:X]->(f)");
        db.executeTransactionally(
                "CALL apoc.trigger.add('count-removed-rels','MATCH (c:Counter) SET c.count = c.count + size($deletedRelationships)',{})");
        db.executeTransactionally("MATCH (f:Foo) DETACH DELETE f");
        TestUtil.testCall(db, "MATCH (c:Counter) RETURN c.count as count", (row) -> {
            assertEquals(1L, row.get("count"));
        });
    }

    @Test
    void testRemoveTrigger() {
        TestUtil.testCallCount(db, "CALL apoc.trigger.add('to-be-removed','RETURN 1',{}) YIELD name RETURN name", 1);
        TestUtil.testCall(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertTrue(row.get("query").toString().contains("RETURN 1"));
            assertEquals(true, row.get("installed"));
        });
        TestUtil.testCall(db, "CALL apoc.trigger.remove('to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertTrue(row.get("query").toString().contains("RETURN 1"));
            assertEquals(false, row.get("installed"));
        });

        TestUtil.testCallCount(db, "CALL apoc.trigger.list()", 0);
        TestUtil.testCall(db, "CALL apoc.trigger.remove('to-be-removed')", (row) -> {
            assertEquals("to-be-removed", row.get("name"));
            assertEquals(null, row.get("query"));
            assertEquals(false, row.get("installed"));
        });
    }

    @Test
    void testRemoveAllTrigger() {
        TestUtil.testCallCount(db, "CALL apoc.trigger.removeAll()", 0);
        TestUtil.testCallCount(db, "CALL apoc.trigger.add('to-be-removed-1','RETURN 1',{}) YIELD name RETURN name", 1);
        TestUtil.testCallCount(db, "CALL apoc.trigger.add('to-be-removed-2','RETURN 2',{}) YIELD name RETURN name", 1);
        TestUtil.testCallCount(db, "CALL apoc.trigger.list()", 2);
        TestUtil.testResult(db, "CALL apoc.trigger.removeAll()", (res) -> {
            final var rows = res.stream()
                    .sorted(Comparator.comparing(r -> (String) r.get("name")))
                    .toList();
            assertEquals(2, rows.size());
            assertEquals("to-be-removed-1", rows.get(0).get("name"));
            assertTrue(rows.get(0).get("query").toString().contains("RETURN 1"));

            assertEquals(false, rows.get(0).get("installed"));
            assertEquals("to-be-removed-2", rows.get(1).get("name"));
            assertTrue(rows.get(1).get("query").toString().contains("RETURN 2"));
            assertEquals(false, rows.get(1).get("installed"));
        });
        TestUtil.testCallCount(db, "CALL apoc.trigger.list()", 0);
        TestUtil.testCallCount(db, "CALL apoc.trigger.removeAll()", 0);
    }

    @Test
    void testTimeStampTrigger() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('timestamp','UNWIND $createdNodes AS n SET n.ts = timestamp()',{})");
        db.executeTransactionally("CREATE (f:Foo)");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node) row.get("f")).hasProperty("ts"));
        });
    }

    @Test
    void testTxIdAfterAsync() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('txinfo','UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime',{phase:'afterAsync'})");
        db.executeTransactionally("CREATE (f:Bar)");
        org.neo4j.test.assertion.Assert.assertEventually(
                () -> db.executeTransactionally("MATCH (n:Bar) RETURN n", Map.of(), result -> {
                    final Node node = result.<Node>columnAs("n").next();
                    return (long) node.getProperty("txId", -1L) > -1L && (long) node.getProperty("txTime") > start;
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    void testTxId() {
        db.executeTransactionally("CREATE (f:Another)");
        db.executeTransactionally(
                "CALL apoc.trigger.add('txinfo','UNWIND $createdNodes AS n \n" + "MATCH (a:Another) WITH a, n\n"
                        + "SET a.txId = $transactionId, a.txTime = $commitTime',{phase:'after'})");
        db.executeTransactionally("CREATE (f:Bar)");
        TestUtil.testCall(db, "MATCH (f:Another) RETURN f", (row) -> {
            assertEquals(true, (Long) ((Node) row.get("f")).getProperty("txId") > -1L);
            assertEquals(true, (Long) ((Node) row.get("f")).getProperty("txTime") > start);
        });
        db.executeTransactionally("MATCH (n:Another) DELETE n");
    }

    @Test
    void testMetaDataBefore() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('txinfo','UNWIND $createdNodes AS n SET n.label = labels(n)[0], n += $metaData', {phase: 'before'})");
        testMetaData("MATCH (n:Bar) RETURN n");
    }

    @Test
    void testMetaDataAfter() {
        db.executeTransactionally("CREATE (n:Another)");
        db.executeTransactionally(
                "CALL apoc.trigger.add('txinfo', 'UNWIND $createdNodes AS n MATCH (a:Another) SET a.label = labels(n)[0], a += $metaData', {phase: 'after'})");
        testMetaData("MATCH (n:Another) RETURN n");
        db.executeTransactionally("MATCH (n:Another) DELETE n");
    }

    private void testMetaData(String matchQuery) {
        try (Transaction tx = db.beginTx()) {
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            ktx.setMetaData(Collections.singletonMap("txMeta", "hello"));
            tx.execute("CREATE (f:Bar)");
            tx.commit();
        }
        TestUtil.testCall(db, matchQuery, (row) -> {
            final Node node = (Node) row.get("n");
            assertEquals("Bar", node.getProperty("label"));
            assertEquals("hello", node.getProperty("txMeta"));
        });
    }

    @Test
    void testPauseResult() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('pausedTest', 'UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime', {phase: 'after'})");
        TestUtil.testCall(db, "CALL apoc.trigger.pause('pausedTest')", (row) -> {
            assertEquals("pausedTest", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    void testPauseOnCallList() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('test', 'UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime', {phase: 'after'})");
        db.executeTransactionally("CALL apoc.trigger.pause('test')");
        TestUtil.testCall(db, "CALL apoc.trigger.list()", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(true, row.get("paused"));
        });
    }

    @Test
    void testResumeResult() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('test', 'UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime', {phase: 'after'})");
        db.executeTransactionally("CALL apoc.trigger.pause('test')");
        TestUtil.testCall(db, "CALL apoc.trigger.resume('test')", (row) -> {
            assertEquals("test", row.get("name"));
            assertEquals(true, row.get("installed"));
            assertEquals(false, row.get("paused"));
        });
    }

    @Test
    void testTriggerPause() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('test','UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime',{})");
        db.executeTransactionally("CALL apoc.trigger.pause('test')");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(false, ((Node) row.get("f")).hasProperty("txId"));
            assertEquals(false, ((Node) row.get("f")).hasProperty("txTime"));
            assertEquals(true, ((Node) row.get("f")).hasProperty("name"));
        });
    }

    @Test
    void testTriggerResume() {
        db.executeTransactionally(
                "CALL apoc.trigger.add('test','UNWIND $createdNodes AS n SET n.txId = $transactionId, n.txTime = $commitTime',{})");
        db.executeTransactionally("CALL apoc.trigger.pause('test')");
        db.executeTransactionally("CALL apoc.trigger.resume('test')");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertEquals(true, ((Node) row.get("f")).hasProperty("txId"));
            assertEquals(true, ((Node) row.get("f")).hasProperty("txTime"));
            assertEquals(true, ((Node) row.get("f")).hasProperty("name"));
        });
    }

    @Test
    void showThrowAnExceptionOnBrokenCypherQuery() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> db.executeTransactionally(
                        "CALL apoc.trigger.add('test','UNWIND $createdNodes AS n SET n.txId = , n.txTime = $commitTime',{})"));
        assertTrue(
                e.getMessage()
                        .contains(
                                "Failed to invoke procedure `apoc.trigger.add`: Caused by: org.neo4j.exceptions.SyntaxException: Invalid input"));
    }

    @Test
    void testCreatedRelationshipsAsync() {
        db.executeTransactionally("CREATE (:A {name: \"A\"})-[:R1]->(:Z {name: \"Z\"})");
        db.executeTransactionally("CALL apoc.trigger.add('trigger-after-async', 'UNWIND $createdRelationships AS r\n"
                + "MATCH (a:A)-[r]->(z:Z)\n"
                + "WHERE type(r) IN [\"R2\", \"R3\"]\n"
                + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET r1.triggerAfterAsync = true', {phase: 'afterAsync'})");
        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"})\n" + "MERGE (a)-[:R2]->(z)");

        org.neo4j.test.assertion.Assert.assertEventually(
                () -> db.executeTransactionally("MATCH ()-[r:R1]->() RETURN r", Map.of(), result ->
                        (boolean) result.<Relationship>columnAs("r").next().getProperty("triggerAfterAsync", false)),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    void testDeleteRelationshipsAsync() {
        db.executeTransactionally(
                "CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        final String query = "UNWIND $deletedRelationships AS r\n" + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET a.alpha = apoc.any.property(r, \"alpha\"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *";
        db.executeTransactionally(
                "CALL apoc.trigger.add('trigger-after-async-1', $query, {phase: 'afterAsync'})", map("query", query));

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    void testDeleteRelationshipsAsyncWithCreationInQuery() {
        db.executeTransactionally(
                "CREATE (a:A {name: \"A\"})-[:R1 {omega: 3}]->(z:Z {name: \"Z\"}), (a)-[:R2 {alpha: 1}]->(z)");
        final String query = "UNWIND $deletedRelationships AS r\n" + "CREATE (a:A)-[r1:R1 {omega: 3}]->(z)\n"
                + "SET a.alpha = apoc.any.property(r, \"alpha\"), r1.triggerAfterAsync = size($deletedRelationships) > 0, r1.size = size($deletedRelationships), r1.deleted = type(r) RETURN *";
        db.executeTransactionally(
                "CALL apoc.trigger.add('trigger-after-async-2', $query, {phase: 'afterAsync'})", map("query", query));

        // delete rel
        commonDeleteAfterAsync("MATCH (a:A {name: 'A'})-[r:R2]->(z:Z {name: 'Z'}) DELETE r");
    }

    @Test
    void testDeleteNodesAsync() {
        db.executeTransactionally(
                "CREATE (a:A {name: 'A'})-[:R1 {omega: 3}]->(z:Z {name: 'Z'}), (:R2:Other {alpha: 1})");
        final String query = "UNWIND $deletedNodes AS n\n" + "MATCH (a)-[r1:R1]->(z)\n"
                + "SET a.alpha = apoc.any.property(n, \"alpha\"), r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = apoc.node.labels(n)[0] RETURN *";

        db.executeTransactionally(
                "CALL apoc.trigger.add('trigger-after-async-3', $query, {phase: 'afterAsync'})", map("query", query));

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    @Test
    void testDeleteNodesAsyncWithCreationQuery() {
        db.executeTransactionally("CREATE (:R2:Other {alpha: 1})");
        final String query = "UNWIND $deletedNodes AS n\n" + "CREATE (a:A)-[r1:R1 {omega: 3}]->(z:Z)\n"
                + "SET a.alpha = apoc.any.property(n, \"alpha\"), r1.triggerAfterAsync = size($deletedNodes) > 0, r1.size = size($deletedNodes), r1.deleted = apoc.node.labels(n)[0] RETURN *";

        db.executeTransactionally(
                "CALL apoc.trigger.add('trigger-after-async-4', $query, {phase: 'afterAsync'})", map("query", query));

        // delete node
        commonDeleteAfterAsync("MATCH (n:R2) DELETE n");
    }

    private void commonDeleteAfterAsync(String deleteQuery) {
        db.executeTransactionally(deleteQuery);

        final Map<String, Object> expectedProps =
                Map.of("deleted", "R2", "triggerAfterAsync", true, "size", 1L, "omega", 3L);

        org.neo4j.test.assertion.Assert.assertEventually(
                () -> db.executeTransactionally("MATCH (a:A {alpha: 1})-[r:R1]->() RETURN r", Map.of(), result -> {
                    final ResourceIterator<Relationship> relIterator = result.columnAs("r");
                    return relIterator.hasNext()
                            && relIterator.next().getAllProperties().equals(expectedProps);
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }

    @Test
    void testDeleteRelationships() {
        db.executeTransactionally("CREATE (a:A {name: \"A\"})-[:R1]->(z:Z {name: \"Z\"}), (a)-[:R2]->(z)");
        db.executeTransactionally("CALL apoc.trigger.add('trigger-after', 'UNWIND $deletedRelationships AS r\n"
                + "MERGE (a:AA{name: \"AA\"})\n"
                + "SET a.triggerAfter = size($deletedRelationships) = 1, a.deleted = type(r)', {phase: 'after'})");
        db.executeTransactionally("MATCH (a:A {name: \"A\"})-[r:R2]->(z:Z {name: \"Z\"})\n" + "DELETE r");

        org.neo4j.test.assertion.Assert.assertEventually(
                () -> db.executeTransactionally("MATCH (a:AA) RETURN a", Map.of(), result -> {
                    final Node r = result.<Node>columnAs("a").next();
                    return (boolean) r.getProperty("triggerAfter", false)
                            && r.getProperty("deleted", "").equals("R2");
                }),
                (value) -> value,
                30L,
                TimeUnit.SECONDS);
    }
}
