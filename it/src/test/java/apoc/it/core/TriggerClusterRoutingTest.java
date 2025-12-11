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
package apoc.it.core;

import static apoc.trigger.Trigger.SYS_DB_NON_WRITER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

class TriggerClusterRoutingTest {
    private static TestcontainersCausalCluster cluster;
    private static List<Neo4jContainerExtension> clusterMembers;

    @BeforeAll
    public static void setupCluster() {
        cluster = TestContainerUtil.createEnterpriseCluster(
                List.of(TestContainerUtil.ApocPackage.CORE),
                3,
                0,
                Collections.emptyMap(),
                Map.of(
                        "NEO4J_dbms_routing_enabled", "true",
                        "apoc.trigger.enabled", "true"));

        clusterMembers = cluster.getClusterMembers();
        Assertions.assertEquals(3, clusterMembers.size());
    }

    @AfterAll
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    // TODO: fabric tests once the @SystemOnlyProcedure annotation is added to Neo4j

    @Test
    void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String addTrigger = "CYPHER 5 CALL apoc.trigger.add($name, 'RETURN 1', {})";
        String triggerName = randomTriggerName();

        succeedsInLeader(addTrigger, triggerName, DEFAULT_DATABASE_NAME);
        failsInFollowers(addTrigger, triggerName, SYS_DB_NON_WRITER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String addTrigger = "CYPHER 5 CALL apoc.trigger.add($name, 'RETURN 1', {})";
        final String removeTrigger = "CYPHER 5 CALL apoc.trigger.remove($name)";
        String triggerName = randomTriggerName();

        succeedsInLeader(addTrigger, triggerName, DEFAULT_DATABASE_NAME);
        succeedsInLeader(removeTrigger, triggerName, DEFAULT_DATABASE_NAME);
        failsInFollowers(removeTrigger, triggerName, SYS_DB_NON_WRITER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String installTrigger = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        String triggerName = randomTriggerName();
        succeedsInLeader(installTrigger, triggerName, SYSTEM_DATABASE_NAME);
        failsInFollowers(installTrigger, triggerName, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    @Test
    void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String installTrigger = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        final String dropTrigger = "CALL apoc.trigger.drop('neo4j', $name)";
        String triggerName = randomTriggerName();

        succeedsInLeader(installTrigger, triggerName, SYSTEM_DATABASE_NAME);
        succeedsInLeader(dropTrigger, triggerName, SYSTEM_DATABASE_NAME);
        failsInFollowers(dropTrigger, triggerName, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    @Test
    void testTriggerShowAllowedInAllSystemInstances() {
        final String installTrigger = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        final String showTrigger = "CALL apoc.trigger.show('neo4j')";

        String triggerName = randomTriggerName();
        Consumer<Iterator<Map<String, Object>>> checkTriggerIsListed = rows -> {
            AtomicBoolean showedTrigger = new AtomicBoolean(false);
            rows.forEachRemaining(row -> {
                if (row.get("name").equals(triggerName)) showedTrigger.set(true);
            });

            Assertions.assertTrue(showedTrigger.get());
        };

        succeedsInLeader(installTrigger, triggerName, SYSTEM_DATABASE_NAME);

        // Triggers are installed eventually
        await("triggers installed")
                .atMost(Duration.ofMinutes(5))
                .pollDelay(Duration.ofMillis(50))
                .pollInSameThread()
                .untilAsserted(() -> {
                    succeedsInLeader(showTrigger, triggerName, SYSTEM_DATABASE_NAME, checkTriggerIsListed);
                    succeedsInFollowers(showTrigger, triggerName, SYSTEM_DATABASE_NAME, checkTriggerIsListed);
                });
    }

    private static void succeedsInLeader(String triggerOperation, String triggerName, String dbName) {
        for (Neo4jContainerExtension instance : clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                testCall(
                        session,
                        triggerOperation,
                        Map.of("name", triggerName),
                        row -> Assertions.assertEquals(triggerName, row.get("name")));
            }
        }
    }

    private static void succeedsInLeader(
            String triggerOperation,
            String triggerName,
            String dbName,
            Consumer<Iterator<Map<String, Object>>> assertion) {
        for (Neo4jContainerExtension instance : clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                testResult(session, triggerOperation, Map.of("name", triggerName), assertion);
            }
        }
    }

    private static void succeedsInFollowers(
            String triggerOperation,
            String triggerName,
            String dbName,
            Consumer<Iterator<Map<String, Object>>> assertion) {
        for (Neo4jContainerExtension instance : clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (!dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                testResult(session, triggerOperation, Map.of("name", triggerName), assertion);
            }
        }
    }

    private static void failsInFollowers(
            String triggerOperation, String triggerName, String expectedError, String dbName) {
        for (Neo4jContainerExtension instance : clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (!dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                Exception e = assertThrows(
                        Exception.class,
                        () -> testCall(
                                session,
                                triggerOperation,
                                Map.of("name", triggerName),
                                row -> Assertions.fail("Should fail because of non writer trigger addition")));
                String errorMsg = e.getMessage();
                Assertions.assertTrue(errorMsg.contains(expectedError), "The actual message is: " + errorMsg);
            }
        }
    }

    private static String randomTriggerName() {
        return UUID.randomUUID().toString();
    }

    private static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run(
                        "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress))
                .single()
                .get("writer")
                .asBoolean();
    }

    private static String getBoltAddress(Neo4jContainerExtension instance) {
        return instance.getEnvMap().get("NEO4J_server_bolt_advertised__address");
    }

    private static Session getSessionForDb(Neo4jContainerExtension instance, String dbName) {
        final Driver driver = instance.getDriver();
        return driver.session(SessionConfig.forDatabase(dbName));
    }
}
