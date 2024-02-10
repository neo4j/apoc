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

import static apoc.refactor.GraphRefactoringTest.CLONE_NODES_QUERY;
import static apoc.refactor.GraphRefactoringTest.CLONE_SUBGRAPH_QUERY;
import static apoc.refactor.GraphRefactoringTest.EXTRACT_QUERY;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

public class GraphRefactoringEnterpriseTest {
    private static final String CREATE_REL_FOR_EXTRACT_NODE =
            "CREATE (:Start)-[r:TO_MOVE {name: 'foobar', surname: 'baz'}]->(:End)";
    private static final String DELETE_REL_FOR_EXTRACT_NODE = "MATCH p=(:Start)-[r:TO_MOVE]->(:End) DELETE p";
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

    @Test
    public void testCloneNodesWithNodeKeyConstraint() {
        nodeKeyCommons(CLONE_NODES_QUERY);
    }

    @Test
    public void testCloneNodesWithBothExistenceAndUniqueConstraint() {
        uniqueNotNullCommons(CLONE_NODES_QUERY);
    }

    @Test
    public void testCloneSubgraphWithNodeKeyConstraint() {
        nodeKeyCommons(CLONE_SUBGRAPH_QUERY);
    }

    @Test
    public void testCloneSubgraphWithBothExistenceAndUniqueConstraint() {
        uniqueNotNullCommons(CLONE_SUBGRAPH_QUERY);
    }

    @Test
    public void testExtractNodesWithNodeKeyConstraint() {
        session.executeWrite(tx -> tx.run(CREATE_REL_FOR_EXTRACT_NODE).consume());
        nodeKeyCommons(EXTRACT_QUERY);
        session.executeWrite(tx -> tx.run(DELETE_REL_FOR_EXTRACT_NODE).consume());
    }

    @Test
    public void testExtractNodesWithBothExistenceAndUniqueConstraint() {
        session.executeWrite(tx -> tx.run(CREATE_REL_FOR_EXTRACT_NODE).consume());
        uniqueNotNullCommons(EXTRACT_QUERY);
        session.executeWrite(tx -> tx.run(DELETE_REL_FOR_EXTRACT_NODE).consume());
    }

    private void nodeKeyCommons(String query) {
        session.executeWrite(
                tx -> tx.run("CREATE CONSTRAINT nodeKey FOR (p:MyBook) REQUIRE (p.name, p.surname) IS NODE KEY")
                        .consume());
        cloneNodesAssertions(
                query, "already exists with label `MyBook` and properties `name` = 'foobar', `surname` = 'baz'");
        session.executeWrite(tx -> tx.run("DROP CONSTRAINT nodeKey").consume());
    }

    private void uniqueNotNullCommons(String query) {
        session.executeWrite(tx -> tx.run("CREATE CONSTRAINT unique FOR (p:MyBook) REQUIRE (p.name) IS UNIQUE")
                .consume());
        session.executeWrite(tx -> tx.run("CREATE CONSTRAINT notNull FOR (p:MyBook) REQUIRE (p.name) IS NOT NULL")
                .consume());

        cloneNodesAssertions(query, "already exists with label `MyBook` and property `name` = 'foobar'");
        session.executeWrite(tx -> tx.run("DROP CONSTRAINT unique").consume());
        session.executeWrite(tx -> tx.run("DROP CONSTRAINT notNull").consume());
    }

    private void cloneNodesAssertions(String query, String message) {
        session.executeWrite(tx ->
                tx.run("CREATE (n:MyBook {name: 'foobar', surname: 'baz'})").consume());
        testCall(session, query, r -> {
            final String error = (String) r.get("error");
            assertTrue(error.contains(message));
            assertNull(r.get("output"));
        });
        session.executeWrite(tx -> tx.run("MATCH (n:MyBook) DELETE n").consume());
    }
}
