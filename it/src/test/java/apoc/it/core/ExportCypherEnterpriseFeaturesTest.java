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

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CONSTRAINTS;
import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestUtil.readFileToString;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Session;

class ExportCypherEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeAll
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), !TestUtil.isRunningInCI())
                .withInitScript("init_neo4j_export_csv.cypher");
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    public static void afterAll() {
        neo4jContainer.close();
    }

    private static void beforeTwoLabelsWithOneCompoundConstraintEach() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT compositeBase FOR (t:Base) REQUIRE (t.tenantId, t.id) IS NODE KEY"));
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE (a:Person:Base {name: 'Phil', surname: 'Meyer', tenantId: 'neo4j', id: 'waBfk3z'}) "
                        + "CREATE (b:Person:Base {name: 'Silvia', surname: 'Jones', tenantId: 'random', id: 'waBfk3z'}) "
                        + "CREATE (a)-[:KNOWS {foo:2}]->(b)"));
    }

    private static void afterTwoLabelsWithOneCompoundConstraintEach() {
        session.executeWriteWithoutResult(tx -> tx.run("MATCH (a:Person:Base) DETACH DELETE a"));
        session.executeWriteWithoutResult(tx -> tx.run("DROP CONSTRAINT compositeBase"));
    }

    @Test
    void testExportWithCompoundConstraintCypherShell() {
        String fileName = "testCypherShellWithCompoundConstraint.cypher";
        testCall(
                session,
                "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", Util.map("format", "cypher-shell")),
                (r) -> assertExportStatement(
                        ExportCypherResults.EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT, fileName));
    }

    @Test
    void testExportDataWithCompoundConstraintCypherShell() {
        testCall(
                session,
                """
                MATCH (start:Person)-[rel:KNOWS]->(end)
                CALL apoc.export.cypher.data([start, end], [rel], null, $config)
                YIELD nodeStatements, schemaStatements, relationshipStatements RETURN *""",
                map("config", Util.map("format", "cypher-shell", "separateFiles", true)),
                this::assertExportNodesAndRels);
    }

    @Test
    void testExportGraphWithCompoundConstraintCypherShell() {
        String fileName = "testGraphCypherShellWithCompoundConstraint.cypher";
        testCall(
                session,
                """
                MATCH (start:Person)-[rel:KNOWS]->(end)
                WITH {nodes: [start, end], relationships: [rel]} AS graph
                CALL apoc.export.cypher.graph(graph, null, $config)
                YIELD nodeStatements, schemaStatements, relationshipStatements RETURN *""",
                map("file", fileName, "config", Util.map("format", "cypher-shell", "separateFiles", true)),
                this::assertExportNodesAndRels);
    }

    @Test
    void testExportQueryWithCompoundConstraintCypherShell() {
        String fileName = "testQueryCypherShellWithCompoundConstraint.cypher";
        testCall(
                session,
                "CALL apoc.export.cypher.query($query, null, {separateFiles: true})",
                map("query", "MATCH (start:Person)-[rel:KNOWS]->(end) RETURN start, rel, end", "file", fileName),
                this::assertExportNodesAndRels);
    }

    private void assertExportNodesAndRels(Map<String, Object> r) {
        List<String> possibleNodeStatements = List.of(
                """
                        UNWIND [{surname:"Jackson", name:"Matt", properties:{}}, {surname:"Snow", name:"John", properties:{}}] AS row
                        CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;""",
                """
                        UNWIND [{surname:"Snow", name:"John", properties:{}}, {surname:"Jackson", name:"Matt", properties:{}}] AS row
                        CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;""");
        String actual = (String) r.get("nodeStatements");
        Assertions.assertTrue(possibleNodeStatements.stream().anyMatch(actual::contains));
        String schemaStatements = (String) r.get("schemaStatements");
        List.of(
                        "CREATE CONSTRAINT KnowsConsNotNull FOR ()-[rel:KNOWS]-() REQUIRE (rel.foo) IS NOT NULL;",
                        "CREATE CONSTRAINT PersonRequiresNamesConstraint FOR (node:Person) REQUIRE (node.name, node.surname) IS NODE KEY;",
                        "CREATE CONSTRAINT KnowsConsUnique FOR ()-[rel:KNOWS]-() REQUIRE (rel.two) IS UNIQUE;")
                .forEach(constraint -> Assertions.assertTrue(
                        schemaStatements.contains(constraint),
                        String.format("Constraint '%s' not in result", constraint)));
        Assertions.assertEquals(
                """
                    :begin
                    UNWIND [{start: {name:"John", surname:"Snow"}, end: {name:"Matt", surname:"Jackson"}, properties:{foo:1}}] AS row
                    MATCH (start:Person{surname: row.start.surname, name: row.start.name})
                    MATCH (end:Person{surname: row.end.surname, name: row.end.name})
                    CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
                    :commit
                    """,
                r.get("relationshipStatements"));
    }

    @Test
    void testExportDataOnlyRelWithCompoundConstraintCypherShell() {
        String fileName = "testDataCypherShellWithCompoundConstraint.cypher";
        testCall(
                session,
                """
                MATCH (start:Person)-[rel:KNOWS]->(end)
                CALL apoc.export.cypher.data([], [rel], $file, $config)
                YIELD nodes, relationships, properties RETURN *""",
                map("file", fileName, "config", Util.map("format", "cypher-shell")),
                (r) -> {
                    assertExportOnlyRels(fileName);
                });
    }

    @Test
    void testExportGraphOnlyRelWithCompoundConstraintCypherShell() {
        String fileName = "testGraphCypherShellWithCompoundConstraint.cypher";
        testCall(
                session,
                """
                MATCH (start:Person)-[rel:KNOWS]->(end)
                WITH {nodes: [], relationships: [rel]} AS graph
                CALL apoc.export.cypher.graph(graph, $file, $config)
                YIELD nodes, relationships, properties RETURN *""",
                map("file", fileName, "config", Util.map("format", "cypher-shell")),
                (r) -> {
                    assertExportOnlyRels(fileName);
                });
    }

    @Test
    void testExportQueryOnlyRelWithCompoundConstraintCypherShell() {
        String fileName = "testQueryCypherShellWithCompoundConstraint.cypher";
        testCall(
                session,
                "CALL apoc.export.cypher.query($query, $file)",
                map("query", "MATCH (start:Person)-[rel:KNOWS]->(end) RETURN rel", "file", fileName),
                (r) -> {
                    assertExportOnlyRels(fileName);
                });
    }

    @Test
    void testExportWithCompoundConstraintPlain() {
        String fileName = "testPlainFormatWithCompoundConstraint.cypher";
        testCall(
                session,
                "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", Util.map("format", "plain")),
                (r) -> assertExportStatement(
                        ExportCypherResults.EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT, fileName));
    }

    @Test
    void testExportWithCompoundConstraintNeo4jShell() {
        String fileName = "testNeo4jShellWithCompoundConstraint.cypher";
        testCall(
                session,
                "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", Util.map("format", "neo4j-shell")),
                (r) -> assertExportStatement(
                        ExportCypherResults.EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT, fileName));
    }

    @Test
    void shouldHandleTwoLabelsWithOneCompoundConstraintEach() {
        final String query = "MATCH (a:Person:Base)-[r:KNOWS]-(b:Person) RETURN a, b, r";
        /* The bug was:
           UNWIND [{start: {name:"Phil", surname:"Meyer"}, end: {name:"Silvia", surname:"Jones"}, properties:{}}] AS row
           MATCH (start:Person{tenantId: row.start.tenantId, id: row.start.id, surname: row.start.surname, name: row.start.name})
           MATCH (end:Person{surname: row.end.surname, name: row.end.name})
           CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
        */
        final String expected =
                """
                UNWIND [{start: {name:"Phil", surname:"Meyer"}, end: {name:"Silvia", surname:"Jones"}, properties:{foo:2}}] AS row
                MATCH (start:Person{surname: row.start.surname, name: row.start.name})
                MATCH (end:Person{surname: row.end.surname, name: row.end.name})
                CREATE (start)-[r:KNOWS]->(end) SET r += row.properties""";

        try {
            beforeTwoLabelsWithOneCompoundConstraintEach();
            testCallInReadTransaction(
                    session,
                    "CALL apoc.export.cypher.query($query, $file, $config)",
                    Util.map("file", null, "query", query, "config", Util.map("format", "plain", "stream", true)),
                    (r) -> {
                        final String cypherStatements = (String) r.get("cypherStatements");
                        String unwind = Stream.of(cypherStatements.split(";"))
                                .map(String::trim)
                                .filter(s -> s.startsWith("UNWIND"))
                                .filter(s -> s.contains("Meyer"))
                                .skip(1)
                                .findFirst()
                                .orElse(null);
                        Assertions.assertEquals(expected, unwind);
                    });
        } finally {
            afterTwoLabelsWithOneCompoundConstraintEach();
        }
    }

    private void assertExportOnlyRels(String fileName) {
        String actual = readFileToString(new File(importFolder, fileName));
        final String expected =
                """
            :begin
            CREATE CONSTRAINT KnowsConsNotNull FOR ()-[rel:KNOWS]-() REQUIRE (rel.foo) IS NOT NULL;
            CREATE CONSTRAINT KnowsConsUnique FOR ()-[rel:KNOWS]-() REQUIRE (rel.two) IS UNIQUE;
            :commit
            CALL db.awaitIndexes(300);
            :begin
            UNWIND [{start: {_id:0}, end: {_id:1}, properties:{foo:1}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
            :commit
            """;
        Assertions.assertEquals(expected, actual);
    }

    private void assertExportStatement(String expectedStatement, String fileName) {
        // The constraints are exported in arbitrary order, so we cannot assert on the entire file
        String actual = readFileToString(new File(importFolder, fileName));
        MatcherAssert.assertThat(actual, Matchers.containsString(expectedStatement));
        EXPECTED_CONSTRAINTS.forEach(constraint -> Assertions.assertTrue(
                actual.contains(constraint), String.format("Constraint '%s' not in result", constraint)));
    }
}
