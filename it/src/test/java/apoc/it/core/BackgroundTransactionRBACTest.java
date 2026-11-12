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

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

/*
 * Procedures that mutate data in background transactions (apoc.refactor.rename.*, apoc.periodic.iterate,
 * apoc.nodes.delete, apoc.refactor.categorize, apoc.refactor.extractNode, apoc.refactor.cloneSubgraph) must run
 * those transactions with the security context of the caller, so that fine-grained write privileges are enforced
 * exactly as they are for plain Cypher.
 *
 * The user can read the whole graph and execute apoc procedures (and has a general WRITE grant, which is what
 * lets them call WRITE-mode procedures at all), but is denied the specific writes below.
 *
 * This test effectively checks that privileges are respected, and if that were to change in the future,
 * this test would break.
 */
class BackgroundTransactionRBACTest {

    private static final String USER = "limitedUser";
    private static final String PASSWORD = "password1234";

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver userDriver;
    private static Session adminSession;
    private static Session userSession;

    @BeforeAll
    static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        adminSession = neo4jContainer.getSession();

        List<String> setup = List.of(
                "CREATE ROLE limited",
                "CREATE USER " + USER + " SET PASSWORD '" + PASSWORD + "' SET PASSWORD CHANGE NOT REQUIRED",
                "GRANT ROLE limited TO " + USER,
                "GRANT ACCESS ON DATABASE neo4j TO limited",
                "GRANT EXECUTE PROCEDURE apoc.* ON DBMS TO limited",
                "GRANT MATCH {*} ON GRAPH neo4j TO limited",
                "GRANT WRITE ON GRAPH neo4j TO limited",
                // everything below is denied, deny takes precedence over the grant above
                "DENY SET LABEL * ON GRAPH neo4j TO limited",
                "DENY REMOVE LABEL * ON GRAPH neo4j TO limited",
                "DENY CREATE ON GRAPH neo4j NODES Admin, Extracted, Category TO limited",
                "DENY DELETE ON GRAPH neo4j NODES Admin, User TO limited",
                "DENY CREATE ON GRAPH neo4j RELATIONSHIPS * TO limited",
                "DENY DELETE ON GRAPH neo4j RELATIONSHIPS * TO limited",
                "DENY SET PROPERTY {*} ON GRAPH neo4j NODES Admin TO limited",
                "DENY SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIPS * TO limited");
        setup.forEach(q -> adminSession.run(q).consume());

        adminSession
                .run("CREATE CONSTRAINT category_name FOR (c:Category) REQUIRE c.name IS UNIQUE")
                .consume();

        userDriver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic(USER, PASSWORD));
        userDriver.verifyConnectivity();
        userSession = userDriver.session();
    }

    @AfterAll
    static void afterAll() {
        userSession.close();
        userDriver.close();
        adminSession.close();
        neo4jContainer.close();
    }

    @BeforeEach
    void resetData() {
        adminSession.run("MATCH (n) DETACH DELETE n").consume();
        adminSession
                .run(
                        """
                        CREATE (a1:Admin {name: 'a1', oldProp: 1, cat: 'c1'}),
                               (a2:Admin {name: 'a2', oldProp: 2, cat: 'c2'}),
                               (u:User {name: 'u'}),
                               (a1)-[:KNOWS {oldProp: 1}]->(u),
                               (a2)-[:KNOWS {oldProp: 2}]->(u)
                        """)
                .consume();
    }

    private long count(String query) {
        return adminSession.run(query).single().get("c").asLong();
    }

    private void assertGraphUntouched() {
        assertEquals(2L, count("MATCH (n:Admin) RETURN count(n) AS c"));
        assertEquals(0L, count("MATCH (n:Disabled) RETURN count(n) AS c"));
        assertEquals(1L, count("MATCH (n:User) RETURN count(n) AS c"));
        assertEquals(2L, count("MATCH ()-[r:KNOWS]->() RETURN count(r) AS c"));
        assertEquals(0L, count("MATCH ()-[r:RENAMED]->() RETURN count(r) AS c"));
        assertEquals(2L, count("MATCH (n:Admin) WHERE n.oldProp IS NOT NULL RETURN count(n) AS c"));
        assertEquals(0L, count("MATCH (n) WHERE n.newProp IS NOT NULL RETURN count(n) AS c"));
        assertEquals(2L, count("MATCH ()-[r:KNOWS]->() WHERE r.oldProp IS NOT NULL RETURN count(r) AS c"));
        assertEquals(0L, count("MATCH ()-[r]->() WHERE r.newProp IS NOT NULL RETURN count(r) AS c"));
        assertEquals(0L, count("MATCH (n:Category) RETURN count(n) AS c"));
        assertEquals(0L, count("MATCH (n:Extracted) RETURN count(n) AS c"));
    }

    /** For procedures that report failed batch actions in their result rather than throwing. */
    private void assertAllActionsFailed(String query) {
        List<Record> rows = userSession.run(query).list();
        assertEquals(1, rows.size());
        assertTrue(rows.getFirst().get("failedOperations").asLong() > 0, "expected failed operations: " + rows);
        assertEquals(0L, rows.getFirst().get("committedOperations").asLong());
        assertGraphUntouched();
    }

    @Test
    void controlPlainCypherIsDenied() {
        assertThrows(Exception.class, () -> userSession
                .run("MATCH (n:Admin) REMOVE n:Admin SET n:Disabled")
                .consume());
        assertThrows(
                Exception.class,
                () -> userSession.run("MATCH (n:User) DETACH DELETE n").consume());
        assertGraphUntouched();
    }

    @Test
    void renameLabel() {
        assertAllActionsFailed("CALL apoc.refactor.rename.label('Admin', 'Disabled')");
    }

    @Test
    void renameType() {
        assertAllActionsFailed("CALL apoc.refactor.rename.type('KNOWS', 'RENAMED')");
    }

    @Test
    void renameNodeProperty() {
        assertAllActionsFailed("CALL apoc.refactor.rename.nodeProperty('oldProp', 'newProp')");
    }

    @Test
    void renameTypeProperty() {
        assertAllActionsFailed("CALL apoc.refactor.rename.typeProperty('oldProp', 'newProp')");
    }

    @Test
    void periodicIterate() {
        assertAllActionsFailed(
                "CALL apoc.periodic.iterate('MATCH (n:Admin) RETURN n', 'SET n:Disabled', {batchSize: 1})");
    }

    @Test
    void nodesDelete() {
        assertThrows(Exception.class, () -> userSession
                .run(
                        "MATCH (u:User) WITH collect(u) AS users CALL apoc.nodes.delete(users, 1000) YIELD value RETURN value")
                .consume());
        assertGraphUntouched();
    }

    @Test
    void categorize() {
        assertThrows(Exception.class, () -> userSession
                .run("CALL apoc.refactor.categorize('cat', 'IN_CATEGORY', true, 'Category', 'name', [], 100)")
                .consume());
        assertGraphUntouched();
        assertEquals(0L, count("MATCH ()-[r:IN_CATEGORY]->() RETURN count(r) AS c"));
    }

    @Test
    void extractNode() {
        // the denied write happens in an inner transaction; the error is reported per row
        List<Record> rows = userSession
                .run(
                        "MATCH ()-[r:KNOWS]->() WITH collect(r) AS rels CALL apoc.refactor.extractNode(rels, ['Extracted'], 'OUT', 'IN') YIELD error RETURN error")
                .list();
        assertTrue(
                !rows.isEmpty() && rows.stream().noneMatch(r -> r.get("error").isNull()), "expected errors: " + rows);
        assertGraphUntouched();
    }

    @Test
    void cloneSubgraphInNewTransactions() {
        List<Record> rows = userSession
                .run(
                        "MATCH (a:Admin) WITH collect(a) AS nodes CALL apoc.refactor.cloneSubgraph(nodes, [], {createNodesInNewTransactions: true}) YIELD error RETURN error")
                .list();
        assertTrue(
                !rows.isEmpty() && rows.stream().noneMatch(r -> r.get("error").isNull()), "expected errors: " + rows);
        assertGraphUntouched();
    }

    @Test
    void adminCanStillRunTheSameProcedures() {
        adminSession
                .run("CALL apoc.refactor.rename.label('Admin', 'Disabled')", Map.of())
                .consume();
        assertEquals(2L, count("MATCH (n:Disabled) RETURN count(n) AS c"));
        assertEquals(0L, count("MATCH (n:Admin) RETURN count(n) AS c"));
    }
}
