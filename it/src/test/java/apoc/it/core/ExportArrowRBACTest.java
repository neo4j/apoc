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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

/*
 * The schema of an Arrow export is derived by running apoc.meta.nodeTypeProperties / relTypeProperties. That scan
 * must not disclose the property names of labels / relationship types the caller is not allowed to read, including
 * when the caller steers the scan with includeLabels / includeRels in the config.
 *
 * CYPHER 5 only; the Arrow export is moved to extended for Cypher 25.
 *
 * This test effectively checks that privileges are respected, and if that were to change in the future,
 * this test would break.
 */
class ExportArrowRBACTest {

    private static final String USER = "arrowUser";
    private static final String PASSWORD = "password1234";

    // property keys that only exist on data the restricted user cannot read
    private static final Set<String> SECRET_KEYS = Set.of("iban", "amount", "secretRef");

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
                "CREATE ROLE arrowRole",
                "CREATE USER " + USER + " SET PASSWORD '" + PASSWORD + "' SET PASSWORD CHANGE NOT REQUIRED",
                "GRANT ROLE arrowRole TO " + USER,
                "GRANT ACCESS ON DATABASE neo4j TO arrowRole",
                "GRANT EXECUTE PROCEDURE apoc.* ON DBMS TO arrowRole",
                "GRANT MATCH {*} ON GRAPH neo4j TO arrowRole",
                "DENY MATCH {*} ON GRAPH neo4j NODES Salary TO arrowRole",
                "DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS PAID TO arrowRole");
        setup.forEach(q -> adminSession.run(q).consume());

        adminSession
                .run(
                        """
                        CREATE (p:Person {name: 'p'})-[:KNOWS {since: 1}]->(:Person {name: 'q'}),
                               (p)-[:PAID {secretRef: 'r'}]->(:Salary {amount: 1000, iban: 'XX00'})
                        """)
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

    /** The field names of the Arrow schemas of all the exported byte arrays. */
    private static Set<String> schemaFieldNames(Session session, String query) {
        Set<String> names = new HashSet<>();
        for (Record row : session.run(query).list()) {
            try (var allocator = new RootAllocator();
                    var reader = new ArrowStreamReader(
                            new ByteArrayInputStream(row.get("value").asByteArray()), allocator)) {
                for (Field field : reader.getVectorSchemaRoot().getSchema().getFields()) {
                    names.add(field.getName());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return names;
    }

    private static void assertNoSecretKeys(Set<String> fields) {
        Set<String> leaked = new HashSet<>(fields);
        leaked.retainAll(SECRET_KEYS);
        assertTrue(leaked.isEmpty(), "schema discloses properties of unreadable data: " + leaked + " in " + fields);
    }

    @Test
    void controlAdminSeesTheSchemaOfSalary() {
        // proves the test data and the includeLabels / includeRels override do surface these keys
        Set<String> fields = schemaFieldNames(
                adminSession,
                "CYPHER 5 CALL apoc.export.arrow.stream.all({includeLabels: ['Salary'], includeRels: ['PAID']})");
        assertTrue(fields.containsAll(Set.of("iban", "amount")), "unexpected schema: " + fields);
    }

    @Test
    void controlAdminRelOverride() {
        Set<String> fields =
                schemaFieldNames(adminSession, "CYPHER 5 CALL apoc.export.arrow.stream.all({includeRels: ['PAID']})");
        assertTrue(fields.contains("secretRef"), "unexpected schema: " + fields);
    }

    @Test
    void streamAllDoesNotDiscloseUnreadableSchema() {
        Set<String> fields = schemaFieldNames(userSession, "CYPHER 5 CALL apoc.export.arrow.stream.all()");
        assertNoSecretKeys(fields);
        assertTrue(fields.contains("name"), "readable data should still be exported: " + fields);
    }

    @Test
    void streamAllIncludeLabelsOverrideDoesNotDiscloseUnreadableSchema() {
        assertNoSecretKeys(schemaFieldNames(
                userSession, "CYPHER 5 CALL apoc.export.arrow.stream.all({includeLabels: ['Salary']})"));
    }

    @Test
    void streamAllIncludeRelsOverrideDoesNotDiscloseUnreadableSchema() {
        assertNoSecretKeys(
                schemaFieldNames(userSession, "CYPHER 5 CALL apoc.export.arrow.stream.all({includeRels: ['PAID']})"));
    }

    @Test
    void streamGraphIncludeLabelsOverrideDoesNotDiscloseUnreadableSchema() {
        assertNoSecretKeys(schemaFieldNames(
                userSession,
                "CYPHER 5 MATCH (n:Person) WITH collect(n) AS nodes "
                        + "CALL apoc.export.arrow.stream.graph({nodes: nodes, relationships: []}, "
                        + "{includeLabels: ['Salary'], includeRels: ['PAID']}) YIELD value RETURN value"));
    }

    @Test
    void controlPlainCypherCannotReadSalary() {
        assertFalse(userSession.run("MATCH (s:Salary) RETURN s").hasNext());
    }
}
