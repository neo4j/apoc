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

import static apoc.util.Util.labelStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ExpandPathTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = Util.readResourceFile("movies.cypher");
        String bigbrother =
                "MATCH (per:Person) MERGE (bb:BigBrother {name : 'Big Brother' })  MERGE (bb)-[:FOLLOWS]->(per)";
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.execute(bigbrother);
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @After
    public void removeOtherLabels() {
        db.executeTransactionally(
                "OPTIONAL MATCH (c:Western) REMOVE c:Western WITH DISTINCT 1 as ignore OPTIONAL MATCH (c:Denylist) REMOVE c:Denylist");
    }

    @Test
    public void testExplorePathAnyRelTypeTest() {
        List<String> nodeRepresentations = List.of("m", "id(m)", "elementId(m)", "[m]", "[id(m)]", "[elementId(m)]");
        for (String nodeRep : nodeRepresentations) {
            TestUtil.testCall(
                    db,
                    String.format(
                            "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(%s,'>','',0,2) YIELD path RETURN count(*) AS c",
                            nodeRep),
                    (row) -> assertEquals(1L, row.get("c")));
        }

        TestUtil.testCall(
                db,
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'<','',0,2) YIELD path RETURN count(*) AS c",
                (row) -> assertEquals(17L, row.get("c")));
        TestUtil.testCall(
                db,
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'','',0,2) YIELD path RETURN count(*) AS c",
                (row) -> assertEquals(52L, row.get("c")));
        TestUtil.testCall(
                db,
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'',0,2) YIELD path RETURN count(*) AS c",
                (row) -> assertEquals(52L, row.get("c")));
    }

    @Test
    public void testExplorePathRelationshipsTest() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'<ACTED_IN|PRODUCED>|FOLLOWS','',0,2) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(11L, row.get("c")));
    }

    @Test
    public void testExplorePathLabelAllowListTest() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,'ACTED_IN|PRODUCED|FOLLOWS','+Person|+Movie',0,3) yield path return count(*) as c";
        TestUtil.testCall(
                db, query, (row) -> assertEquals(107L, row.get("c"))); // 59 with Uniqueness.RELATIONSHIP_GLOBAL
    }

    @Test
    public void testExplorePathLabelDenyListTest() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expand(m,null,'-BigBrother',0,2) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(44L, row.get("c")));
    }

    @Test
    public void testExplorePathWithTerminationLabel() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL'}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size()); // since Gene blocks any path to Clint
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Gene Hackman", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testExplorePathWithFilterStartNodeFalseIgnoresLabelFilter() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Person', maxLevel:2, filterStartNode:false}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(9L, row.get("c")));
    }

    @Test
    public void testExplorePathWithLimitReturnsLimitedResults() {
        db.executeTransactionally(
                "MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Christian Bale', 'Tom Cruise'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', limit: 2}) yield path "
                        + "RETURN nodes(path)[-1].name AS node",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());

                    MatcherAssert.assertThat(maps, Matchers.hasItem(MapUtil.map("node", "Tom Cruise")));

                    MatcherAssert.assertThat(maps, Matchers.hasItem(MapUtil.map("node", "Clint Eastwood")));
                });
    }

    @Test
    public void testExplorePathWithEndNodeLabel() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', uniqueness: 'NODE_GLOBAL'}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Gene Hackman", path.endNode().getProperty("name"));
                    path = (Path) maps.get(1).get("path");
                    assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testExplorePathWithEndNodeLabelAndLimit() {
        db.executeTransactionally(
                "MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman', 'Christian Bale'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', uniqueness: 'NODE_GLOBAL', limit:2}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Gene Hackman", path.endNode().getProperty("name"));
                    path = (Path) maps.get(1).get("path");
                    assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
                });
    }

    // label filter precedence tests

    @Test
    public void testDenylistBeforeAllowlist() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person|-Person', uniqueness: 'NODE_GLOBAL', filterStartNode:true}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testDenylistBeforeTerminationFilter() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western|-Western', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testDenylistBeforeEndNodeFilter() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western|-Western', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testTerminationFilterBeforeAllowlist() {
        db.executeTransactionally(
                "MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman', 'Christian Bale'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western|+Movie', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Gene Hackman", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testTerminationFilterBeforeEndNodeFilter() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western|>Western', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Gene Hackman", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testEndNodeFilterAsAllowlist() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western|+Movie', uniqueness: 'NODE_GLOBAL', filterStartNode:false}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Gene Hackman", path.endNode().getProperty("name"));
                    path = (Path) maps.get(1).get("path");
                    assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testLimitPlaysNiceWithMinLevel() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', uniqueness: 'NODE_GLOBAL', limit:1, minLevel:3}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testTerminationFilterDoesNotPruneBelowMinLevel() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.expandConfig(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', uniqueness: 'NODE_GLOBAL', minLevel:3}) yield path "
                        + "return path",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Path path = (Path) maps.get(0).get("path");
                    assertEquals("Clint Eastwood", path.endNode().getProperty("name"));
                });
    }

    @Test
    public void testFilterStartNodeFalseDoesNotFilterStartNodeWhenBelowMinLevel() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Person', minLevel:1, maxLevel:2, filterStartNode:false}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(8L, row.get("c")));
    }

    @Test
    public void testOptionalExpandConfigWithNoResultsYieldsNull() {
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Agent', minLevel:1, maxLevel:2, filterStartNode:false, optional:true}) YIELD path RETURN path";
        TestUtil.testResult(db, query, (result) -> {
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals(null, row.get("path"));
        });
    }

    @Test
    public void testFilterStartNodeDefaultsToFalse() {
        // was default true prior to 3.2.x
        String query =
                "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{labelFilter:'+Person'}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(9L, row.get("c")));
    }

    @Test
    public void testCompoundLabelMatchesOnlyNodeWithBothLabels() {
        db.executeTransactionally(
                "MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Eastwood");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western:Eastwood', uniqueness: 'NODE_GLOBAL'}) yield node "
                        + "return node",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals(
                            "Clint Eastwood", node.getProperty("name")); // otherwise Gene would block path to Clint
                });
    }

    @Test
    public void testCompoundLabelWorksInDenylist() {
        db.executeTransactionally(
                "MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Denylist");

        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western|-Western:Denylist', uniqueness: 'NODE_GLOBAL'}) yield node "
                        + "return node",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
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
						CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN', allowlistNodes: allowlistNodes})
						YIELD node RETURN node""",
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
							CALL apoc.path.subgraphNodes(%s, {relationshipFilter:'ACTED_IN', allowlistNodes: allowlistNodes})
							YIELD node RETURN node""",
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
    public void testSubgraphNodesAllowListTakesPriority() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (allowlist:Movie)
						WHERE allowlist.title = "The Matrix"
						MATCH (allowlistDeprecated:Movie)
						WHERE allowlistDeprecated.title <> "The Matrix"
						WITH k, collect(allowlist) AS allowlistNodes, collect(allowlistDeprecated) AS allowlistDeprecatedNodes
						CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN', allowlistNodes: allowlistNodes, whitelistNodes: allowlistDeprecatedNodes})
						YIELD node RETURN node""",
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
    public void testSubgraphNodesWorksWithDeprecatedAllowList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (allowlist:Movie)
						WHERE allowlist.title = "The Matrix"
						WITH k, collect(allowlist) AS allowlistNodes
						CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN', whitelistNodes: allowlistNodes})
						YIELD node RETURN node""",
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
    public void testSubgraphNodesWorksWithDenyList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (denylist:Movie)
						WHERE denylist.title <> "The Matrix"
						WITH k, collect(denylist) AS denylistNodes
						CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN', labelFilter: 'Movie', denylistNodes: denylistNodes})
						YIELD node RETURN node""",
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
    public void testSubgraphNodesDenyListTakesPriority() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (denylist:Movie)
						WHERE denylist.title <> "The Matrix"
						MATCH (denylistDeprecated:Movie)
						WHERE denylistDeprecated.title = "The Matrix"
						WITH k, collect(denylist) AS denylistNodes, collect(denylistDeprecated) AS denylistDeprecatedNodes
						CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN', labelFilter: 'Movie', denylistNodes: denylistNodes, blacklistNodes: denylistDeprecatedNodes})
						YIELD node RETURN node""",
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
    public void testSubgraphNodesWorksWithDeprecatedDenyList() {
        TestUtil.testResult(
                db,
                """
						MATCH (k:Person {name:'Keanu Reeves'})
						MATCH (denylist:Movie)
						WHERE denylist.title <> "The Matrix"
						WITH k, collect(denylist) AS denylistNodes
						CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN', labelFilter: 'Movie', blacklistNodes: denylistNodes})
						YIELD node RETURN node""",
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
    public void testRelationshipFilterWorksWithoutTypeOutgoing() {
        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'>', labelFilter:'>Movie', uniqueness: 'NODE_GLOBAL'}) yield node "
                        + "return collect(node.title) as titles",
                result -> {
                    List<String> expectedTitles = new ArrayList<>(Arrays.asList(
                            "Something's Gotta Give",
                            "Johnny Mnemonic",
                            "The Replacements",
                            "The Devil's Advocate",
                            "The Matrix Revolutions",
                            "The Matrix Reloaded",
                            "The Matrix"));
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    List<String> titles = (List<String>) maps.get(0).get("titles");
                    assertEquals(7, titles.size());
                    assertTrue(titles.containsAll(expectedTitles));
                });
    }

    @Test
    public void testRelationshipFilterWorksWithoutTypeIncoming() {
        TestUtil.testResult(
                db,
                "MATCH (k:Person {name:'Keanu Reeves'}) "
                        + "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'<', labelFilter:'>BigBrother', uniqueness: 'NODE_GLOBAL'}) yield node "
                        + "return node",
                result -> {
                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                });
    }

    @Test
    public void testLabelWithSpecialChar() {
        db.executeTransactionally(
                """
						CREATE (n:`http://example.com/abc#Object` {one: 'alpha'}) WITH n
						CREATE (n)-[:REL]->(:`http://www.w3.org/2002/07/owl#Class`:OwlClass {two: 'beta'}),
						 (n)-[:REL]->(:foo:bar {two: 'gamma'}),
						 (n)-[:REL]->(:foo:baz {two: 'delta'})""");

        String pathFilter = ">|<";
        // match using a single label (`http://www.w3.org/2002/07/owl#Class`)
        String labelFilter = "/http\\://www.w3.org/2002/07/owl#Class|/foo:bar|/another";

        Map<String, Object> config = Map.of("pathFilter", pathFilter, "labelFilter", labelFilter);

        TestUtil.testResult(
                db,
                "MATCH (node:`http://example.com/abc#Object`) CALL apoc.path.expand(node, $pathFilter, $labelFilter, 0, 2) "
                        + "YIELD path "
                        + "return nodes(path) AS nodes",
                config,
                this::specialCharAssertions);

        // with apoc.path.expandConfig
        TestUtil.testResult(
                db,
                "MATCH (node:`http://example.com/abc#Object`) CALL apoc.path.expandConfig(node, $config) "
                        + "YIELD path "
                        + "return distinct nodes(path) AS nodes",
                Map.of("config", config),
                this::specialCharAssertions);
    }

    @Test
    public void testCompoundLabelWithSpecialChar() {
        db.executeTransactionally(
                """
						CREATE (n:`http://compound.example/abc#Object` {one: 'alpha'}) WITH n
						CREATE (n)-[:REL]->(o:`/http://www.w3.org/2002/07/owl#Class`:OwlClass:`Go:ku` {two: 'beta'}),
						 (n)-[:REL]->(:foo:bar {two: 'gamma'}),
						 (n)-[:REL]->(:foo:baz {two: 'delta'})""");

        // match using multiple labels (`http://www.w3.org/2002/07/owl#Class`, `OwlClass` and `Go:ku`)
        String labelFilter = "//http\\://www.w3.org/2002/07/owl#Class:OwlClass:Go\\:ku|/foo:bar|/another";

        Map<String, Object> config = Map.of("labelFilter", labelFilter);

        TestUtil.testResult(
                db,
                "MATCH (node:`http://compound.example/abc#Object`) CALL apoc.path.expand(node, null, $labelFilter, 0, 2) "
                        + "YIELD path "
                        + "return distinct nodes(path) AS nodes",
                config,
                this::specialCharAssertions);

        // with apoc.path.expandConfig
        TestUtil.testResult(
                db,
                "MATCH (node:`http://compound.example/abc#Object`) " + "CALL apoc.path.expandConfig(node, $config) "
                        + "YIELD path "
                        + "RETURN distinct nodes(path) AS nodes",
                Map.of("config", config),
                this::specialCharAssertions);
    }

    @Test
    public void testLabelWithTwoDots() {
        db.executeTransactionally("CREATE (n:Multiple:End)-[:REL]->(o:`foo:bar` {two: 'beta'})");

        String labelFilter = "foo:bar";
        String labelFilterEscaped = "foo\\:bar";
        Map<String, Object> config = Map.of("labelFilter", labelFilter);
        Map<String, Object> configEscaped = Map.of("labelFilter", labelFilterEscaped);

        // this doesn't work because search for compound labels, `foo` and `bar`
        TestUtil.testCallEmpty(
                db,
                "MATCH (n:Multiple:End) CALL apoc.path.expand(n, null, $labelFilter, 1, 2) YIELD path RETURN path",
                config);

        // this works correctly, because search for `foo:bar` label
        TestUtil.testCall(
                db,
                "MATCH (n:Multiple:End) CALL apoc.path.expand(n, null, $labelFilter, 1, 2) YIELD path RETURN path",
                configEscaped,
                r -> {
                    Path path = (Path) r.get("path");
                    Iterator<Node> iterator = path.nodes().iterator();

                    Node node = iterator.next();
                    assertEquals(List.of("End", "Multiple"), labelStrings(node));

                    node = iterator.next();
                    assertEquals(List.of("foo:bar"), labelStrings(node));

                    assertFalse(iterator.hasNext());
                });
    }

    @Test
    public void testShouldFailOnInvalidUniqueness() {
        String statement =
                """
                                MATCH (k:Person {name:'Keanu Reeves'})
                                CALL apoc.path.expandConfig(k, {uniqueness: 'NODE_GLOBALS'})
                                YIELD path
                                RETURN path
                        """;

        RuntimeException e = assertThrows(RuntimeException.class, () -> TestUtil.testCall(db, statement, (res) -> {}));
        String expectedMessage =
                "Failed to invoke procedure `apoc.path.expandConfig`: Caused by: java.lang.RuntimeException: Invalid uniqueness: 'NODE_GLOBALS'. Must be one of: NODE_GLOBAL, NODE_PATH, NODE_RECENT, NODE_LEVEL, RELATIONSHIP_GLOBAL, RELATIONSHIP_PATH, RELATIONSHIP_RECENT, RELATIONSHIP_LEVEL, NONE.";
        assertEquals(expectedMessage, e.getMessage());
    }

    private void specialCharAssertions(Result result) {
        Map<String, Object> row = result.next();
        assertSinglePath(row);
        row = result.next();
        assertSinglePath(row);
        assertFalse(result.hasNext());
    }

    private static void assertSinglePath(Map<String, Object> row) {
        List<Node> nodes = (List<Node>) row.get("nodes");
        assertEquals(2, nodes.size());
        Map<String, Object> allProperties = nodes.get(0).getAllProperties();
        assertEquals(Map.of("one", "alpha"), allProperties);
        Map<String, Object> allPropertiesEnd = nodes.get(1).getAllProperties();
        String beta = (String) allPropertiesEnd.get("two");
        assertTrue("Property `two` has value " + beta, List.of("beta", "gamma").contains(beta));
    }
}
