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

import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test path expanders with node filters (where we already have the nodes that will be used for the allowlist, denylist, endnodes, and terminator nodes
 */
public class NodeFilterTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, PathExplorer.class);
        String movies = Util.readResourceFile("movies.cypher");
        try (Transaction tx = db.beginTx()) {
            tx.execute(movies);
            tx.commit();
        }
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @After
    public void removeOtherLabels() {
        db.executeTransactionally("OPTIONAL MATCH (c:Western) REMOVE c:Western WITH DISTINCT 1 as ignore OPTIONAL MATCH (c:Denylist) REMOVE c:Denylist");
    }

    @Test
    public void testTerminatorNodesPruneExpansion() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene, clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesContinueTraversal() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Denylist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[gene, clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesAndTerminatorNodesReturnExpectedResults() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Denylist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[gene], terminatorNodes:[clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesAndTerminatorNodesReturnExpectedResultsReversed() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Denylist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene], endNodes:[clint]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testTerminatorNodesOverruleEndNodes1() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Denylist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene], endNodes:[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testTerminatorNodesOverruleEndNodes2() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western WITH c WHERE c.name = 'Clint Eastwood' SET c:Denylist");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[gene, clint], endNodes:[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testEndNodesWithTerminationFilterPrunesExpansion() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'/Western', endNodes:[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testTerminatorNodesWithEndNodeFilterPrunesExpansion() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', terminatorNodes" +
                        ":[clint, gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testDenylistNodesInPathPrunesPath() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', denylistNodes:[unforgiven]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testDenylistNodesWithEndNodesPrunesPath() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[clint, gene], denylistNodes:[unforgiven]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }

    @Test
    public void testDenylistNodesOverridesAllOtherNodeFilters() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, gene, [k, gene, clint, unforgiven, replacements] as allowlist\n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[clint], endNodes:[clint], allowlistNodes:allowlist, denylistNodes:[clint]}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testAllowlistNodes() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, gene, [k, gene, clint, unforgiven, replacements] as allowlist\n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[clint, gene], allowlistNodes:allowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testAllowlistNodesIncludesEndNodes() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, gene, [k, gene, unforgiven, replacements] as allowlist\n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', endNodes:[clint, gene], allowlistNodes:allowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(2, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                    node = (Node) maps.get(1).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testAllowlistNodesIncludesTerminatorNodes() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, clint, gene, [k, gene, unforgiven, replacements] as allowlist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[clint], allowlistNodes:allowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Clint Eastwood", node.getProperty("name"));
                });
    }

    @Test
    public void testAllowlistNodesAndLabelFiltersMustAgreeToInclude1() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, [k, replacements] as allowlist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person', allowlistNodes:allowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node.getProperty("name"));
                });
    }

    @Test
    public void testAllowlistNodesAndLabelFiltersMustAgreeToInclude2() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, [k] as allowlist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person|+Movie', allowlistNodes:allowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node.getProperty("name"));
                });
    }



    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresDenylistNodes() {
        String query = "MATCH (m:Movie {title: 'The Matrix'}) CALL apoc.path.expandConfig(m,{denylistNodes:[m], maxLevel:2, filterStartNode:false}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(44L,row.get("c")));
    }

    @Test
    public void testDenylistNodesStillAppliesBelowMinLevel() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}),  (replacements:Movie{title:'The Replacements'})\n" +
                        "WITH k, clint, unforgiven\n" +
                        "CALL apoc.path.expandConfig(clint, {uniqueness:'NODE_GLOBAL', relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[k], denylistNodes:[unforgiven], minLevel:3}) yield path return last(nodes(path))",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresAllowlistNodesFilter() {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{allowlistNodes:[k], minLevel:1, maxLevel:1, filterStartNode:false}) yield path return count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testAllowlistNodesStillAppliesBelowMinLevel() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}), (gene:Person {name:'Gene Hackman'}), (unforgiven:Movie{title:'Unforgiven'}), (clint:Person {name:'Clint Eastwood'})\n" +
                        "WITH clint, k, [k, replacements, gene, clint] as allowlist\n" +
                        "CALL apoc.path.expandConfig(clint, {uniqueness:'NODE_GLOBAL', relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', terminatorNodes:[k], allowlistNodes:allowlist, minLevel:3}) yield path return last(nodes(path))",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(0, maps.size());
                });
    }

    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresTerminatorNodesFilter() {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{terminatorNodes:[m,k], maxLevel:2, filterStartNode:false}) yield path WITH k, path WHERE last(nodes(path)) = k RETURN count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testTerminatorNodesDoesNotApplyBelowMinLevel() {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{terminatorNodes:[m,k], minLevel:1, maxLevel:2, filterStartNode:true}) yield path WITH k, path WHERE last(nodes(path)) = k RETURN count(*) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testStartNodeWithFilterStartNodeFalseIgnoresEndNodesFilter() {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{endNodes:[m,k], maxLevel:2, filterStartNode:false}) yield path RETURN count(DISTINCT last(nodes(path))) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testEndNodesDoesNotApplyBelowMinLevel() {
        String query = "MATCH (m:Movie {title: 'The Matrix'}), (k:Person {name:'Keanu Reeves'}) CALL apoc.path.expandConfig(m,{endNodes:[m,k], minLevel:1, maxLevel:2, filterStartNode:true}) yield path RETURN count(DISTINCT last(nodes(path))) as c";
        TestUtil.testCall(db, query, (row) -> assertEquals(1L,row.get("c")));
    }

    @Test
    public void testAllowlistNodesTakesPriority() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, [k, replacements] as allowlist, [replacements] as unusedAllowlist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person', allowlistNodes:allowlist, whitelistNodes:unusedAllowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node.getProperty("name"));
                });
    }
    @Test
    public void testDeprecatedAllowlistStillWorks() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (replacements:Movie{title:'The Replacements'}) \n" +
                        "WITH k, [k, replacements] as allowlist \n" +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'+Person', whitelistNodes:allowlist}) yield node return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Keanu Reeves", node.getProperty("name"));
                });
    }
    @Test
    public void testDenylistNodesTakesPriority() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', denylistNodes:[unforgiven], blacklistNodes:[gene]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }
    @Test
    public void testDeprecatedDenylistNodesStillWorks() {
        db.executeTransactionally("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western");

        TestUtil.testResult(db,
                "MATCH (k:Person {name:'Keanu Reeves'}), (gene:Person {name:'Gene Hackman'}), (clint:Person {name:'Clint Eastwood'}), (unforgiven:Movie{title:'Unforgiven'}) " +
                        "CALL apoc.path.subgraphNodes(k, {relationshipFilter:'ACTED_IN|PRODUCED|DIRECTED', labelFilter:'>Western', blacklistNodes:[unforgiven]}) yield node " +
                        "return node",
                result -> {

                    List<Map<String, Object>> maps = Iterators.asList(result);
                    assertEquals(1, maps.size());
                    Node node = (Node) maps.get(0).get("node");
                    assertEquals("Gene Hackman", node.getProperty("name"));
                });
    }
}
