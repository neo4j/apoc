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
package apoc.path;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import apoc.algo.Cover;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import java.util.List;
import java.util.Map;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class SubgraphTest {

    private static Long fullGraphCount;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, PathExplorer.class, Cover.class);
        String movies = Util.readResourceFile("movies.cypher");
        String bigbrother =
                "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.execute(bigbrother);
            tx.commit();
        }

        String getCounts = "match (n) \n" + "return count(n) as graphCount";
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(getCounts);

            Map<String, Object> row = result.next();
            fullGraphCount = (Long) row.get("graphCount");
        }
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testFullSubgraphShouldContainAllNodes() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{}) yield node return count(distinct node) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
    }

    @Test
    public void testSubgraphWithMaxDepthShouldContainExpectedNodes() {
        String controlQuery =
                "MATCH (m:Movie {title: 'The Matrix'})-[*0..3]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
        List<NodeResult> subgraph;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            subgraph = (List<NodeResult>) result.next().get("subgraph");
        }

        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{maxLevel:3}) yield node return COLLECT(node) as subgraphNodes";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            assertEquals(subgraph.size(), subgraphNodes.size());
            assertTrue(subgraph.containsAll(subgraphNodes));
        });
    }

    @Test
    public void testSubgraphWithLabelFilterShouldContainExpectedNodes() {
        String controlQuery =
                "MATCH path = (:Person {name: 'Keanu Reeves'})-[*0..3]-(subgraphNode) where all(node in nodes(path) where node:Person) return collect(distinct subgraphNode) as subgraph";
        List<NodeResult> subgraph;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            subgraph = (List<NodeResult>) result.next().get("subgraph");
        }

        String query =
                "MATCH (k:Person {name: 'Keanu Reeves'}) CALL apoc.path.subgraphNodes(k,{maxLevel:3, labelFilter:'+Person'}) yield node return COLLECT(node) as subgraphNodes";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            assertEquals(subgraph.size(), subgraphNodes.size());
            assertTrue(subgraph.containsAll(subgraphNodes));
        });
    }

    @Test
    public void testSubgraphWithRelationshipFilterShouldContainExpectedNodes() {
        String controlQuery =
                "MATCH path = (:Person {name: 'Keanu Reeves'})-[:ACTED_IN*0..3]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
        List<NodeResult> subgraph;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            subgraph = (List<NodeResult>) result.next().get("subgraph");
        }

        String query =
                "MATCH (k:Person {name: 'Keanu Reeves'}) CALL apoc.path.subgraphNodes(k,{maxLevel:3, relationshipFilter:'ACTED_IN'}) yield node return COLLECT(node) as subgraphNodes";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            assertEquals(subgraph.size(), subgraphNodes.size());
            assertTrue(subgraph.containsAll(subgraphNodes));
        });
    }

    @Test
    public void testOptionalSubgraphNodesShouldReturnNull() {
        String query = "MATCH (k:Person {name: 'Keanu Reeves'}) "
                + "CALL apoc.path.subgraphNodes(k,{labelFilter:'+nonExistent', maxLevel:3, optional:true, filterStartNode:true}) yield node "
                + "return node";
        TestUtil.testResult(db, query, (result) -> {
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals(null, row.get("node"));
        });
    }

    @Test
    public void testSubgraphAllShouldContainExpectedNodesAndRels() {
        String controlQuery =
                """
				MATCH path = (:Person {name: 'Keanu Reeves'})-[*0..3]-(subgraphNode)
				WITH collect(DISTINCT subgraphNode) AS subgraph
				CALL apoc.algo.cover([node in subgraph | elementId(node)])
				YIELD rel
				RETURN subgraph, collect(rel) AS relationships
				""";
        final List<NodeResult> subgraph;
        final List<RelationshipResult> relationships;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            Map<String, Object> row = result.next();
            subgraph = (List<NodeResult>) row.get("subgraph");
            relationships = (List<RelationshipResult>) row.get("relationships");
        }

        String query =
                """
				MATCH (k:Person {name: 'Keanu Reeves'})
				CALL apoc.path.subgraphAll(k, {maxLevel:3})
				YIELD nodes, relationships
				RETURN nodes AS subgraphNodes, relationships AS subgraphRelationships
				""";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            List<RelationshipResult> subgraphRelationships =
                    (List<RelationshipResult>) row.get("subgraphRelationships");
            assertEquals(subgraph.size(), subgraphNodes.size());
            assertTrue(subgraph.containsAll(subgraphNodes));
            assertEquals(relationships.size(), subgraphRelationships.size());
            assertTrue(relationships.containsAll(subgraphRelationships));
        });
    }

    @Test
    public void testOptionalSubgraphAllWithNoResultsShouldReturnEmptyLists() {
        String query = "MATCH (k:Person {name: 'Keanu Reeves'}) "
                + "CALL apoc.path.subgraphAll(k,{labelFilter:'+nonExistent', maxLevel:3, optional:true, filterStartNode:true}) yield nodes, relationships "
                + "return nodes as subgraphNodes, relationships as subgraphRelationships";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            List<RelationshipResult> subgraphRelationships =
                    (List<RelationshipResult>) row.get("subgraphRelationships");
            assertEquals(0, subgraphNodes.size());
            assertEquals(0, subgraphRelationships.size());
        });
    }

    @Test
    public void testSubgraphAllWithNoResultsShouldReturnEmptyLists() {
        String query = "MATCH (k:Person {name: 'Keanu Reeves'}) "
                + "CALL apoc.path.subgraphAll(k,{labelFilter:'+nonExistent', maxLevel:3, filterStartNode:true}) yield nodes, relationships "
                + "return nodes as subgraphNodes, relationships as subgraphRelationships";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            List<RelationshipResult> subgraphRelationships =
                    (List<RelationshipResult>) row.get("subgraphRelationships");
            assertEquals(0, subgraphNodes.size());
            assertEquals(0, subgraphRelationships.size());
        });
    }

    @Test
    public void testSubgraphNodesWorksWithAllowList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (allowlist:Movie)
						WHERE allowlist.title = "The Matrix"
						WITH k, collect(allowlist) AS allowlistNodes
						CALL apoc.path.subgraphAll(k, {relationshipFilter:'ACTED_IN', allowlistNodes: allowlistNodes})
						YIELD nodes UNWIND nodes as node
						RETURN node""",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node1 = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node1.getProperty("name"));
                    Node node2 = (Node) maps.get(1).get("node");
                    assertEquals("The Matrix", node2.getProperty("title"));
                });
    }

    @Test
    public void testSubgraphNodesWithDifferentNodeInputs() {
        List<String> nodeRepresentations = List.of("k", "id(k)", "elementId(k)", "[k]", "[id(k)]", "[elementId(k)]");
        for (String nodeRep : nodeRepresentations) {
            TestUtil.testResult(
                    db,
                    String.format(
                            """
							MATCH (k:Person {name:'Keanu Reeves'})
							MATCH (allowlist:Movie)
							WHERE allowlist.title = "The Matrix"
							WITH k, collect(allowlist) AS allowlistNodes
							CALL apoc.path.subgraphAll(%s, {relationshipFilter:'ACTED_IN', allowlistNodes: allowlistNodes})
							YIELD nodes UNWIND nodes as node
							RETURN node""",
                            nodeRep),
                    result -> {
                        List<Map<String, Object>> maps = Iterators.asList(result);
                        assertEquals(2, maps.size());
                        Node node1 = (Node) maps.get(0).get("node");
                        assertEquals("Keanu Reeves", node1.getProperty("name"));
                        Node node2 = (Node) maps.get(1).get("node");
                        assertEquals("The Matrix", node2.getProperty("title"));
                    });
        }
    }

    @Test
    public void testSubgraphAllAllowListTakesPriority() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (allowlist:Movie)
						WHERE allowlist.title = "The Matrix"
						MATCH (allowlistDeprecated:Movie)
						WHERE allowlistDeprecated.title <> "The Matrix"
						WITH k, collect(allowlist) AS allowlistNodes, collect(allowlistDeprecated) AS allowlistDeprecatedNodes
						CALL apoc.path.subgraphAll(k, {relationshipFilter:'ACTED_IN', allowlistNodes: allowlistNodes, whitelistNodes: allowlistDeprecatedNodes})
						YIELD nodes UNWIND nodes as node
						RETURN node""",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node1 = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node1.getProperty("name"));
                    Node node2 = (Node) maps.get(1).get("node");
                    assertEquals("The Matrix", node2.getProperty("title"));
                });
    }

    @Test
    public void testSubgraphAllWorksWithDeprecatedAllowList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (allowlist:Movie)
						WHERE allowlist.title = "The Matrix"
						WITH k, collect(allowlist) AS allowlistNodes
						CALL apoc.path.subgraphAll(k, {relationshipFilter:'ACTED_IN', whitelistNodes: allowlistNodes})
						YIELD nodes UNWIND nodes as node
						RETURN node""",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node1 = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node1.getProperty("name"));
                    Node node2 = (Node) maps.get(1).get("node");
                    assertEquals("The Matrix", node2.getProperty("title"));
                });
    }

    @Test
    public void testSubgraphAllWorksWithDenyList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (denylist:Movie)
						WHERE denylist.title <> "The Matrix"
						WITH k, collect(denylist) AS denylistNodes
						CALL apoc.path.subgraphAll(k, {relationshipFilter:'ACTED_IN', labelFilter: 'Movie', denylistNodes: denylistNodes})
						YIELD nodes UNWIND nodes as node
						RETURN node""",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node1 = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node1.getProperty("name"));
                    Node node2 = (Node) maps.get(1).get("node");
                    assertEquals("The Matrix", node2.getProperty("title"));
                });
    }

    @Test
    public void testSubgraphAllDenyListTakesPriority() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (denylist:Movie)
						WHERE denylist.title <> "The Matrix"
						MATCH (denylistDeprecated:Movie)
						WHERE denylistDeprecated.title = "The Matrix"
						WITH k, collect(denylist) AS denylistNodes, collect(denylistDeprecated) AS denylistDeprecatedNodes
						CALL apoc.path.subgraphAll(k, {relationshipFilter:'ACTED_IN', labelFilter: 'Movie', denylistNodes: denylistNodes, blacklistNodes: denylistDeprecatedNodes})
						YIELD nodes UNWIND nodes as node
						RETURN node""",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node1 = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node1.getProperty("name"));
                    Node node2 = (Node) maps.get(1).get("node");
                    assertEquals("The Matrix", node2.getProperty("title"));
                });
    }

    @Test
    public void testSubgraphAllWorksWithDeprecatedDenyList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (denylist:Movie)
						WHERE denylist.title <> "The Matrix"
						WITH k, collect(denylist) AS denylistNodes
						CALL apoc.path.subgraphAll(k, {relationshipFilter:'ACTED_IN', labelFilter: 'Movie', blacklistNodes: denylistNodes})
						YIELD nodes UNWIND nodes as node
						RETURN node""",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node1 = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node1.getProperty("name"));
                    Node node2 = (Node) maps.get(1).get("node");
                    assertEquals("The Matrix", node2.getProperty("title"));
                });
    }

    @Test
    public void testSpanningTreeShouldHaveOnlyOnePathToEachNode() {
        String controlQuery =
                "MATCH (m:Movie {title: 'The Matrix'})-[*0..4]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
        List<NodeResult> subgraph;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            subgraph = (List<NodeResult>) result.next().get("subgraph");
        }

        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) " + "CALL apoc.path.spanningTree(m,{maxLevel:4}) yield path "
                        + "with collect(path) as paths "
                        + "with paths, size(paths) as pathCount "
                        + "unwind paths as path "
                        + "with pathCount, collect(distinct last(nodes(path))) as subgraphNodes "
                        + "return pathCount, subgraphNodes";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            long pathCount = (Long) row.get("pathCount");
            assertEquals(subgraph.size(), subgraphNodes.size());
            assertTrue(subgraph.containsAll(subgraphNodes));
            // assert every node has a single path to that node - no cycles
            assertEquals(pathCount, subgraphNodes.size());
        });
    }

    @Test
    public void testOptionalSpanningTreeWithNoResultsShouldReturnNull() {
        String query = "MATCH (k:Person {name: 'Keanu Reeves'}) "
                + "CALL apoc.path.spanningTree(k,{labelFilter:'+nonExistent', maxLevel:3, optional:true, filterStartNode:true}) yield path "
                + "return path";
        TestUtil.testResult(db, query, (result) -> {
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals(null, row.get("path"));
        });
    }

    @Test
    public void testOptionalSubgraphWithResultsShouldYieldExpectedResults() {
        String controlQuery =
                "MATCH (m:Movie {title: 'The Matrix'})-[*0..3]-(subgraphNode) return collect(distinct subgraphNode) as subgraph";
        List<NodeResult> subgraph;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            subgraph = (List<NodeResult>) result.next().get("subgraph");
        }

        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{maxLevel:3, optional:true}) yield node return COLLECT(node) as subgraphNodes";
        TestUtil.testCall(db, query, (row) -> {
            List<NodeResult> subgraphNodes = (List<NodeResult>) row.get("subgraphNodes");
            assertEquals(subgraph.size(), subgraphNodes.size());
            assertTrue(subgraph.containsAll(subgraphNodes));
        });
    }

    @Test
    public void testSubgraphNodesAllowsMinLevel0() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{minLevel:0}) yield node return count(distinct node) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
    }

    @Test
    public void testSubgraphNodesAllowsMinLevel1() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{minLevel:1}) yield node return count(distinct node) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount - 1, row.get("cnt")));
    }

    @Test
    public void testSubgraphNodesErrorsAboveMinLevel1() {
        thrown.expect(QueryExecutionException.class);
        thrown.expect(new RootCauseMatcher<>(
                IllegalArgumentException.class, "minLevel can only be 0 or 1 in subgraphNodes()"));
        TestUtil.singleResultFirstColumn(
                db,
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphNodes(m,{minLevel:2}) yield node return count(distinct node) as cnt");
    }

    @Test
    public void testSubgraphAllAllowsMinLevel0() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphAll(m,{minLevel:0}) yield nodes return size(nodes) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
    }

    @Test
    public void testSubgraphAllAllowsMinLevel1() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphAll(m,{minLevel:1}) yield nodes return size(nodes) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount - 1, row.get("cnt")));
    }

    @Test
    public void testSubgraphAllErrorsAboveMinLevel1() {
        thrown.expect(QueryExecutionException.class);
        thrown.expect(
                new RootCauseMatcher<>(IllegalArgumentException.class, "minLevel can only be 0 or 1 in subgraphAll()"));
        TestUtil.singleResultFirstColumn(
                db,
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.subgraphAll(m,{minLevel:2}) yield nodes return size(nodes) as cnt");
    }

    @Test
    public void testSpanningTreeAllowsMinLevel0() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.spanningTree(m,{minLevel:0}) yield path return count(distinct path) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeAllowsMinLevel1() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.spanningTree(m,{minLevel:1}) yield path return count(distinct path) as cnt";
        TestUtil.testCall(db, query, (row) -> assertEquals(fullGraphCount - 1, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeErrorsAboveMinLevel1() {
        thrown.expect(QueryExecutionException.class);
        thrown.expect(new RootCauseMatcher<>(
                IllegalArgumentException.class, "minLevel can only be 0 or 1 in spanningTree()"));
        TestUtil.singleResultFirstColumn(
                db,
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.spanningTree(m,{minLevel:2}) yield path return count(distinct path) as cnt");
    }

    @Test
    public void testSpanningTreeWithAllowList() {
        TestUtil.testCall(
                db,
                """
				MATCH (m:Movie {title: 'The Matrix'})
				MATCH (allowlist:Person)
				WHERE allowlist.name IN ["Keanu Reeves", "Laurence Fishburne"]
				WITH m, collect(allowlist) AS allowlistNodes
				CALL apoc.path.spanningTree(m,{minLevel:1, maxLevel: 3, allowlistNodes: allowlistNodes})
				YIELD path RETURN count(distinct path) as cnt""",
                (row) -> assertEquals(2L, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeWithDifferentNodeInputs() {
        List<String> nodeRepresentations = List.of("m", "id(m)", "elementId(m)", "[m]", "[id(m)]", "[elementId(m)]");
        for (String nodeRep : nodeRepresentations) {
            TestUtil.testCall(
                    db,
                    String.format(
                            """
							MATCH (m:Movie {title: 'The Matrix'})
							MATCH (allowlist:Person)
							WHERE allowlist.name IN ["Keanu Reeves", "Laurence Fishburne"]
							WITH m, collect(allowlist) AS allowlistNodes
							CALL apoc.path.spanningTree(%s,{minLevel:1, maxLevel: 3, allowlistNodes: allowlistNodes})
							YIELD path RETURN count(distinct path) as cnt""",
                            nodeRep),
                    (row) -> assertEquals(2L, row.get("cnt")));
        }
    }

    @Test
    public void testSpanningTreeWithAllowListTakesPriority() {
        TestUtil.testCall(
                db,
                """
				MATCH (m:Movie {title: 'The Matrix'})
				MATCH (allowlist:Person)
				WHERE allowlist.name IN ["Keanu Reeves", "Laurence Fishburne"]
				MATCH (allowlistDeprecated:Person)
				WHERE allowlistDeprecated.name IN ["Keanu Reeves"]
				WITH m, collect(allowlist) AS allowlistNodes, collect(allowlistDeprecated) AS allowlistDeprecatedNodes
				CALL apoc.path.spanningTree(m,{minLevel:1, maxLevel: 3, allowlistNodes: allowlistNodes, whitelistNodes: allowlistDeprecatedNodes})
				YIELD path RETURN count(distinct path) as cnt""",
                (row) -> assertEquals(2L, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeWithDeprecatedAllowListStillWorks() {
        TestUtil.testCall(
                db,
                """
				MATCH (m:Movie {title: 'The Matrix'})
				MATCH (allowlist:Person)
				WHERE allowlist.name IN ["Keanu Reeves", "Laurence Fishburne"]
				WITH m, collect(allowlist) AS allowlistNodes
				CALL apoc.path.spanningTree(m,{minLevel:1, maxLevel: 3, whitelistNodes: allowlistNodes})
				YIELD path RETURN count(distinct path) as cnt""",
                (row) -> assertEquals(2L, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeWithDenyList() {
        TestUtil.testCall(
                db,
                """
				MATCH (m:Movie {title: 'The Matrix'})
				MATCH (denylist:Person)
				WHERE denylist.name IN ["Keanu Reeves", "Laurence Fishburne"]
				WITH m, collect(denylist) AS denylistNodes
				CALL apoc.path.spanningTree(m,{minLevel:1, maxLevel: 3, denylistNodes: denylistNodes})
				YIELD path RETURN count(distinct path) as cnt""",
                (row) -> assertEquals(136L, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeWithDenyListTakesPriority() {
        TestUtil.testCall(
                db,
                """
				MATCH (m:Movie {title: 'The Matrix'})
				MATCH (denylist:Person)
				WHERE denylist.name IN ["Keanu Reeves", "Laurence Fishburne"]
				MATCH (denylistDeprecated:Person)
				WHERE denylistDeprecated.name IN ["Keanu Reeves"]
				WITH m, collect(denylist) AS denylistNodes, collect(denylistDeprecated) AS denylistDeprecatedNodes
				CALL apoc.path.spanningTree(m,{minLevel:1, maxLevel: 3, denylistNodes: denylistNodes, blacklistNodes: denylistDeprecatedNodes})
				YIELD path RETURN count(distinct path) as cnt""",
                (row) -> assertEquals(136L, row.get("cnt")));
    }

    @Test
    public void testSpanningTreeWithDeprecatedDenyListStillWorks() {
        TestUtil.testCall(
                db,
                """
				MATCH (m:Movie {title: 'The Matrix'})
				MATCH (denylist:Person)
				WHERE denylist.name IN ["Keanu Reeves", "Laurence Fishburne"]
				WITH m, collect(denylist) AS denylistNodes
				CALL apoc.path.spanningTree(m,{minLevel:1, maxLevel: 3, blacklistNodes: denylistNodes})
				YIELD path RETURN count(distinct path) as cnt""",
                (row) -> assertEquals(136L, row.get("cnt")));
    }

    public class RootCauseMatcher<T> extends TypeSafeMatcher<Throwable> {
        private final Class<T> rootCause;
        private final String message;
        private Throwable cause;

        public RootCauseMatcher(Class<T> rootCause, String message) {
            this.rootCause = rootCause;
            this.message = message;
        }

        @Override
        protected boolean matchesSafely(Throwable item) {
            cause = getRootCause(item);
            return rootCause.isInstance(cause) && cause.getMessage().startsWith(message);
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("Expected root cause of ")
                    .appendValue(rootCause)
                    .appendText(" with message: ")
                    .appendValue(message)
                    .appendText(", but ");
            if (cause != null) {
                description
                        .appendText("was: ")
                        .appendValue(cause.getClass())
                        .appendText(" with message: ")
                        .appendValue(cause.getMessage());
            } else {
                description.appendText("actual exception was never thrown.");
            }
        }
    }
}
