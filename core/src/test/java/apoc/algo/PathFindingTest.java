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
package apoc.algo;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Result;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension
class PathFindingTest {

    private static final String SETUP_GEO =
            """
                    CREATE (b:City {name:'Berlin', coords: point({latitude:52.52464,longitude:13.40514}), lat:52.52464,lon:13.40514})
                    CREATE (m:City {name:'München', coords: point({latitude:48.1374,longitude:11.5755, height: 1}), lat:48.1374,lon:11.5755})
                    CREATE (f:City {name:'Frankfurt',coords: point({latitude:50.1167,longitude:8.68333, height: 1}), lat:50.1167,lon:8.68333})
                    CREATE (h:City {name:'Hamburg', coords: point({latitude:53.554423,longitude:9.994583, height: 1}), lat:53.554423,lon:9.994583})
                    CREATE (b)-[:DIRECT {dist:255.64*1000}]->(h)
                    CREATE (b)-[:DIRECT {dist:504.47*1000}]->(m)
                    CREATE (b)-[:DIRECT {dist:424.12*1000}]->(f)
                    CREATE (f)-[:DIRECT {dist:304.28*1000}]->(m)
                    CREATE (f)-[:DIRECT {dist:393.15*1000}]->(h)""";

    private static final String SETUP_MISSING_PROPERTY =
            """
            CREATE (a:Loc{name:'A'}), (b:Loc{name:'B'}), (c:Loc{name:'C'}), (d:Loc{name:'D'}),
            (a)-[:ROAD {d:100}]->(d),
            (a)-[:RAIL {d:5}]->(d),
            (a)-[:ROAD {d:'10'}]->(b),
            (b)-[:ROAD {d:20}]->(c),
            (c)-[:ROAD]->(d),
            (a)-[:ROAD {d:20}]->(c)""";

    private static final String SETUP_SIMPLE =
            """
            CREATE (a:Loc{name:'A'}), (b:Loc{name:'B'}), (c:Loc{name:'C'}), (d:Loc{name:'D'}),
            (a)-[:ROAD {d:100}]->(d),
            (a)-[:RAIL {d:5}]->(d),
            (a)-[:ROAD {d:10}]->(b),
            (b)-[:ROAD {d:20}]->(c),
            (c)-[:ROAD {d:30}]->(d),
            (a)-[:ROAD {d:20}]->(c)""";

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, PathFinding.class);
    }

    @BeforeEach
    void setUp() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    void testAStar() {
        db.executeTransactionally(SETUP_GEO);
        testResult(
                db,
                """
                        MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'})
                        CALL apoc.algo.aStar(from, to, 'DIRECT', 'dist', 'lat', 'lon')
                        YIELD path, weight
                        RETURN path, weight""",
                PathFindingTest::assertAStarResult);
    }

    @Test
    void testAStarConfig() {
        db.executeTransactionally(SETUP_GEO);
        testResult(
                db,
                """
                        MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'})
                        CALL apoc.algo.aStarConfig(from, to, 'DIRECT', {weight:'dist',y:'lat', x:'lon',default:100})
                        YIELD path, weight
                        RETURN path, weight""",
                PathFindingTest::assertAStarResult);
    }

    @Test
    void testAStarConfigWithPoint() {
        db.executeTransactionally(SETUP_GEO);
        testResult(
                db,
                """
                        MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'})
                        CALL apoc.algo.aStarConfig(from, to, 'DIRECT', {pointPropName:'coords', weight:'dist', default:100})
                        YIELD path, weight
                        RETURN path, weight""",
                PathFindingTest::assertAStarResult);
    }

    @Test
    void testDijkstra() {
        db.executeTransactionally(SETUP_SIMPLE);
        testCall(
                db,
                """
                        MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'})
                        CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd')
                        YIELD path, weight
                        RETURN path, weight""",
                row -> {
                    assertEquals(50.0, row.get("weight"));
                    assertEquals(2, ((Path) (row.get("path"))).length()); // 3nodes, 2 rels
                });
        testCall(
                db,
                """
                        MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'})
                        CALL apoc.algo.dijkstra(from, to, '', 'd')
                        YIELD path, weight
                        RETURN path, weight""",
                row -> {
                    assertEquals(5.0, row.get("weight"));
                    assertEquals(1, ((Path) (row.get("path"))).length()); // 2nodes, 1 rels
                });
    }

    @Test
    void testDijkstraMultipleShortest() {
        db.executeTransactionally(SETUP_SIMPLE);
        testResult(
                db,
                """
                        MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'})
                        CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd', 99999, 3)
                        YIELD path, weight
                        RETURN length(path) AS pathLength, weight ORDER BY weight""",
                result -> {
                    assertThat(result.stream())
                            .containsExactly(
                                    Map.of("weight", 50.0, "pathLength", 2L),
                                    Map.of("weight", 60.0, "pathLength", 3L),
                                    Map.of("weight", 100.0, "pathLength", 1L));
                });
    }

    @Test
    void testDijkstraNoPath() {
        db.executeTransactionally("CREATE (:NoPath {name:'X'}), (:NoPath {name:'Y'})");
        testResult(
                db,
                """
                        MATCH (from:NoPath{name:'X'}), (to:NoPath{name:'Y'})
                        CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd') YIELD path, weight
                        RETURN path, weight""",
                result -> assertFalse(result.hasNext()));
    }

    @Test
    void testDijkstraDefaultWeight() {
        db.executeTransactionally("CREATE (:DefW {name:'X'})-[:ROAD]->(:DefW {name:'Y'})");
        testCall(
                db,
                """
                        MATCH (from:DefW{name:'X'}), (to:DefW{name:'Y'})
                        CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd', 7.0) YIELD path, weight
                        RETURN weight""",
                row -> assertEquals(7.0, row.get("weight")));
    }

    @Test
    void testAllSimplePaths() {
        db.executeTransactionally(SETUP_MISSING_PROPERTY);
        testResult(
                db,
                """
                        MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'})
                        CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 3)
                        YIELD path
                        RETURN path ORDER BY length(path)""",
                res -> assertThat(res.columnAs("path").stream())
                        .satisfiesExactly(
                                row -> assertThat(row)
                                        .asInstanceOf(type(Path.class))
                                        .satisfies(p -> assertThat(p.length()).isEqualTo(1)),
                                row -> assertThat(row)
                                        .asInstanceOf(type(Path.class))
                                        .satisfies(p -> assertThat(p.length()).isEqualTo(2)),
                                row -> assertThat(row)
                                        .asInstanceOf(type(Path.class))
                                        .satisfies(p -> assertThat(p.length()).isEqualTo(3))));
    }

    @Test
    void testAllSimplePathResults() {
        db.executeTransactionally(SETUP_MISSING_PROPERTY);
        testResult(
                db,
                """
                        MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'})
                        CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 3)
                        YIELD path
                        RETURN nodes(path) as nodes ORDER BY length(path)""",
                res -> {
                    List<?> nodes;
                    nodes = (List<?>) res.next().get("nodes");
                    assertEquals(2, nodes.size());
                    nodes = (List<?>) res.next().get("nodes");
                    assertEquals(3, nodes.size());
                    nodes = (List<?>) res.next().get("nodes");
                    assertEquals(4, nodes.size());
                    assertFalse(res.hasNext());
                });
    }

    @Test
    void testAllSimplePathsNoPath() {
        db.executeTransactionally("CREATE (:NoSimple {name:'X'}), (:NoSimple {name:'Y'})");
        testResult(
                db,
                """
                        MATCH (from:NoSimple{name:'X'}), (to:NoSimple{name:'Y'})
                        CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 3)
                        YIELD path
                        RETURN path""",
                result -> assertFalse(result.hasNext()));
    }

    @Test
    void testAllSimplePathsDepthLimit() {
        // path A→B→C→D requires depth 3; maxNodes=2 should exclude it
        db.executeTransactionally(
                "CREATE (:Depth {name:'A'})-[:ROAD]->(:Depth {name:'B'})-[:ROAD]->(:Depth {name:'C'})-[:ROAD]->(:Depth {name:'D'})");
        testResult(
                db,
                """
                        MATCH (from:Depth{name:'A'}), (to:Depth{name:'D'})
                        CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 2)
                        YIELD path
                        RETURN path""",
                result -> assertFalse(result.hasNext()));
    }

    private static void assertAStarResult(Result r) {
        assertTrue(r.hasNext());
        Map<String, Object> row = r.next();
        assertEquals(697, ((Number) row.get("weight")).intValue() / 1000);
        Path path = (Path) row.get("path");
        assertEquals(2, path.length()); // 3nodes, 2 rels
        List<Node> nodes = Iterables.asList(path.nodes());
        assertEquals("München", nodes.get(0).getProperty("name"));
        assertEquals("Frankfurt", nodes.get(1).getProperty("name"));
        assertEquals("Hamburg", nodes.get(2).getProperty("name"));
        assertFalse(r.hasNext());
    }
}
