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
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.trigger.TriggerTestUtil.awaitTriggerDiscovered;
import static apoc.util.TestUtil.testCallCountEventually;
import static apoc.util.TestUtil.waitDbsAvailable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(configurationCallback = "configure", createDatabasePerTest = false)
public class TriggerRestartTest {
    GraphDatabaseService db;
    GraphDatabaseService sysDb;

    @Inject
    DatabaseManagementService databaseManagementService;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        System.setProperty("apoc.trigger.refresh", String.valueOf(TRIGGER_DEFAULT_REFRESH));
        System.setProperty("apoc.trigger.enabled", "true");
        builder.setConfigRaw(Map.of("internal.dbms.debug.track_cursor_close", "true"));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5);
    }

    @BeforeAll
    void beforeAll() {
        this.sysDb = databaseManagementService.database("system");
        this.db = databaseManagementService.database("neo4j");
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(db, TriggerNewProcedures.class, Trigger.class);
    }

    private void restartDb() {
        final Path homeDir =
                ((GraphDatabaseAPI) db).databaseLayout().getNeo4jLayout().homeDirectory();
        databaseManagementService.shutdown();
        // Recreate DBMS with the same configuration used in the @ExtensionCallback configure()
        // and using the same home/store directory as the currently running database
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder(homeDir);
        builder.setConfigRaw(Map.of("internal.dbms.debug.track_cursor_close", "true"));
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5);
        databaseManagementService = builder.build();
        db = databaseManagementService.database("neo4j");
        sysDb = databaseManagementService.database("system");
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(db, TriggerNewProcedures.class, Trigger.class);
    }

    @AfterEach
    public void after() {
        db.executeTransactionally("CALL apoc.trigger.removeAll()");
        testCallCountEventually(db, "CALL apoc.trigger.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    public void testTriggerRunsAfterRestart() {
        final String query =
                "CALL apoc.trigger.add('myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerWorksBeforeAndAfterRestart(db, query, Collections.emptyMap(), () -> {});
    }

    @Test
    public void testTriggerViaInstallRunsAfterRestart() {
        final String name = "myTrigger";
        final String innerQuery = "unwind $createdNodes as n set n.trigger = n.trigger + 1";
        final Map<String, Object> params = Map.of("name", name, "query", innerQuery);
        final String triggerQuery =
                "CALL apoc.trigger.install('neo4j', 'myTrigger', 'unwind $createdNodes as n set n.trigger = n.trigger + 1', {phase:'before'})";
        testTriggerWorksBeforeAndAfterRestart(
                sysDb, triggerQuery, params, () -> awaitTriggerDiscovered(db, name, innerQuery));
    }

    @Test
    public void testTriggerViaBothAddAndInstall() {
        // executing both trigger add and install with the same name will not duplicate the eventListeners
        final String name = "myTrigger";
        final String innerQuery = "unwind $createdNodes as n set n.trigger = n.trigger + 1";

        final String triggerQuery = "CALL apoc.trigger.add($name, $query, {phase:'before'})";

        final Map<String, Object> params = Map.of("name", name, "query", innerQuery);

        final Runnable runnable = () -> {
            sysDb.executeTransactionally("CALL apoc.trigger.install('neo4j', $name, $query, {phase:'before'})", params);
            awaitTriggerDiscovered(db, name, innerQuery);
        };
        testTriggerWorksBeforeAndAfterRestart(db, triggerQuery, params, runnable);
    }

    private void testTriggerWorksBeforeAndAfterRestart(
            GraphDatabaseService gbs, String query, Map<String, Object> params, Runnable runnable) {
        TestUtil.testCall(gbs, query, params, row -> {});
        runnable.run();

        db.executeTransactionally("CREATE (p:Person{id:1, trigger: 0})");
        TestUtil.testCall(
                db, "match (n:Person{id:1}) return n.trigger as trigger", r -> assertEquals(1L, r.get("trigger")));

        restartDb();

        db.executeTransactionally("CREATE (p:Person{id:2, trigger: 0})");
        TestUtil.testCallEventually(
                db,
                "match (n:Person{id:1}) return n.trigger as trigger",
                r -> assertEquals(1L, r.get("trigger")),
                TIMEOUT);
        TestUtil.testCallEventually(
                db,
                "match (n:Person{id:2}) return n.trigger as trigger",
                r -> assertEquals(1L, r.get("trigger")),
                TIMEOUT);
    }
}
