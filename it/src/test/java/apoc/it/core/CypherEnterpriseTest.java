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

import static apoc.cypher.CypherTestUtil.CREATE_RESULT_NODES;
import static apoc.cypher.CypherTestUtil.CREATE_RETURNQUERY_NODES;
import static apoc.cypher.CypherTestUtil.SET_AND_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.SET_NODE;
import static apoc.cypher.CypherTestUtil.SIMPLE_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.assertResultNode;
import static apoc.cypher.CypherTestUtil.assertReturnQueryNode;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSetAndReturnResults;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSimpleReturnResults;
import static apoc.util.TestContainerUtil.ApocPackage;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import apoc.util.Neo4jContainerExtension;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;

public class CypherEnterpriseTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @After
    public void after() {
        session.executeWrite(tx -> tx.run("MATCH (n) DETACH DELETE n").consume());
    }

    @Test
    public void testRunManyWithSetAndResults() {
        String query = "CALL apoc.cypher.runMany($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_AND_RETURN_QUERIES);

        testRunProcedureWithSetAndReturnResults(session, query, params);
    }

    @Test
    public void testRunManyWithResults() {
        String query = "CALL apoc.cypher.runMany($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

    @Test
    public void testRunManyReadOnlyWithSetAndResults() {
        String query = "CALL apoc.cypher.runManyReadOnly($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_AND_RETURN_QUERIES);

        session.executeWrite(tx -> tx.run(CREATE_RESULT_NODES).consume());

        // even if this procedure is read-only and execute a write operation, it doesn't fail but just skip the
        // statements
        testCallEmpty(session, query, params);
    }

    @Test
    public void testRunManyReadOnlyWithResults() {
        String query = "CALL apoc.cypher.runManyReadOnly($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

    @Test
    public void testRunWriteWithSetAndResults() {
        String query = "CALL apoc.cypher.runWrite($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_NODE);

        testRunSingleStatementProcedureWithSetAndResults(query, params);
    }

    @Test
    public void testRunWriteWithResults() {
        String query = "CALL apoc.cypher.runWrite($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunSingleStatementProcedureWithResults(query, params);
    }

    @Test
    public void testDoItWithSetAndResults() {
        String query = "CALL apoc.cypher.doIt($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_NODE);

        testRunSingleStatementProcedureWithSetAndResults(query, params);
    }

    @Test
    public void testDoItWithResults() {
        String query = "CALL apoc.cypher.doIt($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunSingleStatementProcedureWithResults(query, params);
    }

    @Test
    public void testRunWithSetAndResults() {
        String query = "CALL apoc.cypher.run($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_NODE);

        session.executeWrite(tx -> tx.run(CREATE_RESULT_NODES).consume());

        RuntimeException e = assertThrows(RuntimeException.class, () -> testCall(session, query, params, (res) -> {}));
        String expectedMessage =
                "Set property for property 'updated' on database 'neo4j' is not allowed for user 'neo4j' with roles [PUBLIC, admin] overridden by READ.";
        assertThat(e.getMessage()).contains(expectedMessage);
    }

    @Test
    public void testRunWithResults() {
        String query = "CALL apoc.cypher.run($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunSingleStatementProcedureWithResults(query, params);
    }

    private static void testRunSingleStatementProcedureWithResults(String query, Map<String, Object> params) {
        session.executeWrite(tx -> tx.run(CREATE_RETURNQUERY_NODES).consume());

        assertThat(session.run(query, params).list())
                .satisfiesExactly(
                        row -> assertReturnQueryNode(0L, row.get("value").asMap(Value::asNode)),
                        row -> assertReturnQueryNode(1L, row.get("value").asMap(Value::asNode)),
                        row -> assertReturnQueryNode(2L, row.get("value").asMap(Value::asNode)),
                        row -> assertReturnQueryNode(3L, row.get("value").asMap(Value::asNode)));
    }

    private static void testRunSingleStatementProcedureWithSetAndResults(String query, Map<String, Object> params) {
        session.executeWrite(tx -> tx.run(CREATE_RESULT_NODES).consume());

        assertThat(session.run(query, params).list())
                .satisfiesExactly(
                        row -> assertResultNode(0L, row.get("value").asMap(Value::asNode)),
                        row -> assertResultNode(1L, row.get("value").asMap(Value::asNode)),
                        row -> assertResultNode(2L, row.get("value").asMap(Value::asNode)),
                        row -> assertResultNode(3L, row.get("value").asMap(Value::asNode)));
    }
}
