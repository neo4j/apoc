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

import static apoc.ApocConfig.APOC_CONFIG_INITIALIZER;
import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.SystemPropertyKeys.database;
import static apoc.it.core.CreateAndDropTriggers.CreateTrigger;
import static apoc.trigger.TriggerHandler.TRIGGER_REFRESH;
import static apoc.trigger.TriggerTestUtil.TIMEOUT;
import static apoc.trigger.TriggerTestUtil.TRIGGER_DEFAULT_REFRESH;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static apoc.util.TestContainerUtil.testResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.SessionConfig.forDatabase;
import static org.neo4j.test.assertion.Assert.assertEventually;

import apoc.SystemLabels;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

public class TriggerEnterpriseFeaturesTest {
    private static final String FOO_DB = "foo";
    private static final String INIT_DB = "initdb";

    private static final String NO_ADMIN_USER = "nonadmin";
    private static final String NO_ADMIN_PWD = "test1234";

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        final String cypherInitializer = String.format("%s.%s.0", APOC_CONFIG_INITIALIZER, SYSTEM_DATABASE_NAME);
        final String createInitDb = String.format("CREATE DATABASE %s IF NOT EXISTS", INIT_DB);

        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withEnv(APOC_TRIGGER_ENABLED, "true")
                .withEnv(TRIGGER_REFRESH, String.valueOf(TRIGGER_DEFAULT_REFRESH))
                .withEnv(cypherInitializer, createInitDb);
        neo4jContainer.start();
        session = neo4jContainer.getSession();

        assertTrue(neo4jContainer.isRunning());

        try (Session sysSession = neo4jContainer.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            sysSession.executeWrite(tx ->
                    tx.run(String.format("CREATE DATABASE %s WAIT;", FOO_DB)).consume());

            sysSession.run(String.format(
                    "CREATE USER %s SET PASSWORD '%s' SET PASSWORD CHANGE NOT REQUIRED", NO_ADMIN_USER, NO_ADMIN_PWD));
        }
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @After
    public void after() throws IOException, InterruptedException {
        // drop all triggers
        try (Session sysSession = neo4jContainer.getDriver().session(forDatabase(SYSTEM_DATABASE_NAME))) {
            Stream.of(DEFAULT_DATABASE_NAME, FOO_DB)
                    .forEach(dbName -> sysSession.run("call apoc.trigger.dropAll($dbName)", Map.of("dbName", dbName)));
        }
    }

    @Test
    public void testTriggerShowInCorrectDatabase() {
        final String defaultTriggerName = UUID.randomUUID().toString();
        final String fooTriggerName = UUID.randomUUID().toString();

        try (Session sysSession = session(SYSTEM_DATABASE_NAME)) {
            // install and show in default db
            testCall(
                    sysSession,
                    "CALL apoc.trigger.install($dbName, $name, 'return 1', {})",
                    Map.of("dbName", DEFAULT_DATABASE_NAME, "name", defaultTriggerName),
                    r -> assertEquals(defaultTriggerName, r.get("name")));

            testCall(
                    sysSession,
                    "CALL apoc.trigger.show($dbName)",
                    Map.of("dbName", DEFAULT_DATABASE_NAME),
                    r -> assertEquals(defaultTriggerName, r.get("name")));

            // install and show in foo db
            testCall(
                    sysSession,
                    "CALL apoc.trigger.install($dbName, $name, 'return 1', {})",
                    Map.of("dbName", FOO_DB, "name", fooTriggerName),
                    r -> assertEquals(fooTriggerName, r.get("name")));

            testCall(
                    sysSession,
                    "CALL apoc.trigger.show($dbName)",
                    Map.of("dbName", FOO_DB),
                    r -> assertEquals(fooTriggerName, r.get("name")));
        }
    }

    @Test
    public void testTriggerInstallInNewDatabase() {
        final String fooTriggerName = UUID.randomUUID().toString();

        try (Session sysSession = session(SYSTEM_DATABASE_NAME)) {
            testCall(
                    sysSession,
                    "call apoc.trigger.install($dbName, $name, 'UNWIND $createdNodes AS n SET n.created = true', {})",
                    Map.of("dbName", FOO_DB, "name", fooTriggerName),
                    r -> assertEquals(fooTriggerName, r.get("name")));
        }

        final String queryTriggerList = "CALL apoc.trigger.list() YIELD name WHERE name = $name RETURN name";
        try (Session fooDbSession = session(FOO_DB)) {
            assertEventually(
                    () -> {
                        final Result res = fooDbSession.run(queryTriggerList, Map.of("name", fooTriggerName));
                        assertTrue("Should have an element", res.hasNext());
                        final Record next = res.next();
                        assertEquals(fooTriggerName, next.get("name").asString());
                        return !res.hasNext();
                    },
                    value -> value,
                    TIMEOUT,
                    TimeUnit.SECONDS);

            fooDbSession.run("CREATE (:Something)");

            testCall(
                    fooDbSession,
                    "MATCH (n:Something) RETURN n.created AS created",
                    r -> assertEquals(true, r.get("created")));
        }

        // check that the trigger is correctly installed in 'foo' db only
        try (Session defaultDbSession = session(DEFAULT_DATABASE_NAME)) {
            testResult(
                    defaultDbSession, queryTriggerList, Map.of("name", fooTriggerName), r -> assertFalse(r.hasNext()));

            defaultDbSession.run("CREATE (:Something)");

            testCall(defaultDbSession, "MATCH (n:Something) RETURN n.created", r -> assertNull(r.get("created")));
        }
    }

    @Test
    public void testDeleteTriggerAfterDatabaseDeletion() {
        try (Session sysSession = session(SYSTEM_DATABASE_NAME)) {
            final String dbToDelete = "todelete";

            // create database with name `todelete`
            sysSession.executeWrite(tx -> tx.run(String.format("CREATE DATABASE %s WAIT;", dbToDelete))
                    .consume());

            testDeleteTriggerAfterDropDb(dbToDelete, sysSession);
        }
    }

    @Test
    public void testDeleteTriggerAfterDatabaseDeletionCreatedViaCypherInit() {
        await("initdb exists")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(1))
                .pollInSameThread()
                .untilAsserted(() -> {
                    try (Session session = session(SYSTEM_DATABASE_NAME)) {
                        final var result = session.run("show databases yield name").stream()
                                .map(row -> row.get(0).asString())
                                .toList();
                        assertThat(result).contains(INIT_DB);
                    }
                });

        try (Session sysSession = session(SYSTEM_DATABASE_NAME)) {
            // the database `initDb` is created via `apoc.initializer.*`
            testDeleteTriggerAfterDropDb(INIT_DB, sysSession);
        }
    }

    private static void testDeleteTriggerAfterDropDb(String dbToDelete, Session sysSession) {
        final String defaultTriggerName = UUID.randomUUID().toString();

        // install and show a trigger in the database and check existence
        testCall(
                sysSession,
                "CALL apoc.trigger.install($dbName, $name, 'return 1', {})",
                Map.of("dbName", dbToDelete, "name", defaultTriggerName),
                r -> assertEquals(defaultTriggerName, r.get("name")));

        testCall(
                sysSession,
                "CALL apoc.trigger.show($dbName)",
                Map.of("dbName", dbToDelete),
                r -> assertEquals(defaultTriggerName, r.get("name")));

        // drop database
        sysSession.executeWrite(tx ->
                tx.run(String.format("DROP DATABASE %s WAIT;", dbToDelete)).consume());

        // check that the trigger has been removed
        testCallEmpty(sysSession, "CALL apoc.trigger.show($dbName)", Map.of("dbName", dbToDelete));
    }

    @Test
    public void testNotDeleteUserDbTriggerNodeAfterDatabaseDeletion() {
        final String dbToDelete = "todelete";

        // create a node in the Neo4j db that looks like a trigger
        try (Session defaultSession = session(DEFAULT_DATABASE_NAME)) {
            defaultSession.executeWrite(tx -> tx.run(String.format(
                            "CREATE (:%s {%s:'%s'})", SystemLabels.ApocTrigger, database.name(), dbToDelete))
                    .consume());
        }

        try (Session sysSession = session(SYSTEM_DATABASE_NAME)) {
            // create database with name `todelete`
            sysSession.executeWrite(tx -> tx.run(String.format("CREATE DATABASE %s WAIT;", dbToDelete))
                    .consume());

            // install a trigger for the database
            final String defaultTriggerName = UUID.randomUUID().toString();
            testCall(
                    sysSession,
                    "CALL apoc.trigger.install($dbName, $name, 'return 1', {})",
                    Map.of("dbName", dbToDelete, "name", defaultTriggerName),
                    r -> assertEquals(defaultTriggerName, r.get("name")));

            // drop database
            sysSession.executeWrite(tx ->
                    tx.run(String.format("DROP DATABASE %s WAIT;", dbToDelete)).consume());
        }

        // check that the node in Neo4j database is still there
        try (Session defaultSession = session(DEFAULT_DATABASE_NAME)) {
            testCall(
                    defaultSession,
                    String.format("MATCH (n:%s) RETURN n.%s AS result", SystemLabels.ApocTrigger, database.name()),
                    Map.of(),
                    r -> assertEquals(dbToDelete, r.get("result")));
        }
    }

    @Test
    public void testTriggersAllowedOnlyWithAdmin() {

        try (Driver userDriver =
                GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic(NO_ADMIN_USER, NO_ADMIN_PWD))) {

            try (Session sysUserSession = userDriver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
                failsWithNonAdminUser(
                        sysUserSession,
                        "apoc.trigger.install",
                        "call apoc.trigger.install('neo4j', 'qwe', 'return 1', {})");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.drop", "call apoc.trigger.drop('neo4j', 'qwe')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.dropAll", "call apoc.trigger.dropAll('neo4j')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.stop", "call apoc.trigger.stop('neo4j', 'qwe')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.start", "call apoc.trigger.start('neo4j', 'qwe')");
                failsWithNonAdminUser(sysUserSession, "apoc.trigger.show", "call apoc.trigger.show('neo4j')");
            }

            try (Session neo4jUserSession = userDriver.session(forDatabase(DEFAULT_DATABASE_NAME))) {
                failsWithNonAdminUser(
                        neo4jUserSession, "apoc.trigger.add", "CYPHER 5 call apoc.trigger.add('abc', 'return 1', {})");
                failsWithNonAdminUser(
                        neo4jUserSession, "apoc.trigger.remove", "CYPHER 5 call apoc.trigger.remove('abc')");
                failsWithNonAdminUser(
                        neo4jUserSession, "apoc.trigger.removeAll", "CYPHER 5 call apoc.trigger.removeAll");
                failsWithNonAdminUser(
                        neo4jUserSession, "apoc.trigger.pause", "CYPHER 5 call apoc.trigger.pause('abc')");
                failsWithNonAdminUser(
                        neo4jUserSession, "apoc.trigger.resume", "CYPHER 5 call apoc.trigger.resume('abc')");
                failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.list", "call apoc.trigger.list");
            }
        }
    }

    @Test
    public void stressTest() throws InterruptedException, ExecutionException, TimeoutException {
        final var db = DEFAULT_DATABASE_NAME;

        try (final var sysSession = session(SYSTEM_DATABASE_NAME)) {
            // We assert on the result of this trigger
            sysSession
                    .run(
                            CreateTrigger,
                            Map.of(
                                    "db",
                                    db,
                                    "name",
                                    "static-trigger-1",
                                    "trigger",
                                    "UNWIND $createdNodes AS n SET n.created = true"))
                    .consume();

            // Create a bunch of no op triggers to make TriggerHandler slower
            for (int i = 0; i < 50; ++i) {
                sysSession
                        .run(CreateTrigger, Map.of("db", db, "name", "rand-trigger-" + i, "trigger", "RETURN 1"))
                        .consume();
            }

            // We assert on the result of this trigger
            sysSession
                    .run(
                            CreateTrigger,
                            Map.of(
                                    "db",
                                    db,
                                    "name",
                                    "static-trigger-2",
                                    "trigger",
                                    "UNWIND $createdNodes AS n SET n.created2 = true"))
                    .consume();
        }

        waitForTrigger(db, "static-trigger-1");
        waitForTrigger(db, "static-trigger-2");

        final var iterations = 200;
        final var executor = Executors.newCachedThreadPool();

        final var driver = neo4jContainer.getDriver();
        final var createNodes = new CreateNodes(driver, db);
        try {
            final var nodesFuture1 = executor.submit(createNodes);
            final var nodesFuture2 = executor.submit(createNodes);
            final var createTriggersFuture = executor.submit(new CreateAndDropTriggers(driver, db, iterations));
            createTriggersFuture.get(5, TimeUnit.MINUTES);
            createNodes.stop();
            nodesFuture1.get(30, TimeUnit.SECONDS);
            nodesFuture2.get(30, TimeUnit.SECONDS);
        } finally {
            createNodes.stop();
            executor.shutdownNow();
        }

        try (final var s = session(DEFAULT_DATABASE_NAME)) {
            final var assertTriggerRanQuery =
                    """
                       match (n:%s)
                       where n.created is null or n.created2 is null
                       return n"""
                            .formatted(CreateNodes.Label);
            final var size = s.run(assertTriggerRanQuery).stream().count();
            assertEquals(0, size);

            final var totCountQuery = """
                       match (n:%s)
                       return count(n)"""
                    .formatted(CreateNodes.Label);
            final var totCount = s.run(totCountQuery).stream().count();
            assertTrue(totCount > 0);
        }
    }

    private void waitForTrigger(final String db, final String name) {
        try (final var s = session(db)) {
            assertEventually(
                    () -> s.run("call apoc.trigger.list() yield name").stream()
                            .map(r -> r.get(0).asString())
                            .toList(),
                    names -> names.contains(name),
                    TIMEOUT,
                    TimeUnit.SECONDS);
        }
    }

    private void failsWithNonAdminUser(Session session, String procName, String query) {
        try {
            testCall(session, query, row -> fail("Should fail because of non admin user"));
        } catch (Exception e) {
            String actual = e.getMessage();
            final String expected = String.format(
                    "Executing admin procedure '%s' permission has not been granted for user 'nonadmin'", procName);
            assertTrue("Actual error message is: " + actual, actual.contains(expected));
        }
    }

    private Session session(final String db) {
        return neo4jContainer.getDriver().session(forDatabase(db));
    }
}

/** Creates nodes in a loop until stopped */
class CreateNodes implements Runnable {
    public static final String Label = "StressTest";
    private final Driver driver;
    private final String db;
    private final AtomicBoolean done = new AtomicBoolean(false);

    CreateNodes(Driver driver, String db) {
        this.driver = driver;
        this.db = db;
    }

    @Override
    public void run() {
        try (final var session = driver.session(forDatabase(db))) {
            while (!done.get()) {
                session.run("create (:%s)".formatted(Label)).consume();
            }
        }
    }

    public void stop() {
        done.set(true);
    }
}

record CreateAndDropTriggers(Driver driver, String db, int iterations) implements Runnable {
    public static final String CreateTrigger = "call apoc.trigger.install($db, $name, $trigger,{})";
    static final String DropTrigger = "call apoc.trigger.drop($db, $name)";

    @Override
    public void run() {
        final var rand = new Random();
        try (final var session = driver.session(forDatabase(SYSTEM_DATABASE_NAME))) {
            for (int i = 0; i < iterations; ++i) {
                final var name = "temp-trigger-" + i;
                session.run(CreateTrigger, Map.of("db", db, "name", name, "trigger", "RETURN 1"))
                        .consume();
                final var deleteName = "temp-trigger-" + rand.nextInt(iterations);
                session.run(DropTrigger, Map.of("db", db, "name", deleteName)).consume();
            }
        }
    }
}
