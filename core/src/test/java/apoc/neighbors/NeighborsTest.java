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
package apoc.neighbors;

import static apoc.ApocConfig.APOC_MAX_HOPS;
import static apoc.ApocConfig.DEFAULT_MAX_HOPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(createDatabasePerTest = false)
class NeighborsTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Neighbors.class);
        db.executeTransactionally("CREATE (a:First), " + "(b:Neighbor{name: 'b'}), "
                + "(c:Neighbor{name: 'c'}), "
                + "(d:Neighbor{name: 'd'}), "
                + "(a)-[:KNOWS]->(b), "
                + "(b)-[:KNOWS]->(a), "
                + "(b)-[:KNOWS]->(c), "
                + "(c)-[:KNOWS]->(d) ");
        db.executeTransactionally("CREATE (a:CycleStart)-[:CYCLE]->(b:CycleNode)-[:CYCLE]->(a)");
        db.executeTransactionally("CREATE (:PathStart)-[:STEP]->(:PathNode)-[:STEP]->(:PathNode)");
    }

    @Test
    void getNeighbors2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop(n,'KNOWS>', 2) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(2, neighbors.size());
                    assertEquals(
                            Arrays.asList("b", "c"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    void getNeighbors3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop(n,'KNOWS>', 3) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(3, neighbors.size());
                    assertEquals(
                            Arrays.asList("b", "c", "d"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    void getNeighborsCount2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop.count(n,'KNOWS>', 2) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(2L, row.get("number")));
    }

    @Test
    void getNeighborsCount3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.tohop.count(n,'KNOWS>', 3) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(3L, row.get("number")));
    }

    @Test
    void getNeighborsByHop2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop(n,'KNOWS>', 2) YIELD nodes AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<List<Node>> neighbors = (List<List<Node>>) row.get("neighbors");
                    assertEquals(2, neighbors.size());
                    assertEquals(
                            Arrays.asList(Arrays.asList("b"), Arrays.asList("c")),
                            neighbors.stream()
                                    .map(l -> l.stream()
                                            .map(n -> n.getProperty("name"))
                                            .collect(Collectors.toList()))
                                    .collect(Collectors.toList()));
                });
    }

    @Test
    void getNeighborsByHop3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop(n,'KNOWS>', 3) YIELD nodes AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<List<Node>> neighbors = (List<List<Node>>) row.get("neighbors");
                    assertEquals(3, neighbors.size());
                    assertEquals(
                            Arrays.asList(Arrays.asList("b"), Arrays.asList("c"), Arrays.asList("d")),
                            neighbors.stream()
                                    .map(l -> l.stream()
                                            .map(n -> n.getProperty("name"))
                                            .collect(Collectors.toList()))
                                    .collect(Collectors.toList()));
                });
    }

    @Test
    void getNeighborsByHopCount2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop.count(n,'KNOWS>', 2) YIELD value AS numbers "
                        + "RETURN numbers",
                (row) -> {
                    List<Long> numbers = (List<Long>) row.get("numbers");
                    assertEquals(2, numbers.size());
                });
    }

    @Test
    void getNeighborsByHopCount3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.byhop.count(n,'KNOWS>', 3) YIELD value AS numbers "
                        + "RETURN numbers",
                (row) -> {
                    List<Long> numbers = (List<Long>) row.get("numbers");
                    assertEquals(3, numbers.size());
                });
    }

    @Test
    void getNeighborsAt2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop(n,'KNOWS>', 2) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(1, neighbors.size());
                    assertEquals(
                            Arrays.asList("c"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    void getNeighborsAt3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop(n,'KNOWS>', 3) YIELD node AS neighbor "
                        + "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(1, neighbors.size());
                    assertEquals(
                            Arrays.asList("d"),
                            neighbors.stream().map(n -> n.getProperty("name")).collect(Collectors.toList()));
                });
    }

    @Test
    void getNeighborsCountAt2Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop.count(n,'KNOWS>', 2) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(1L, row.get("number")));
    }

    @Test
    void getNeighborsCountAt3Hops() {
        TestUtil.testCall(
                db,
                "MATCH (n:First) WITH n " + "CALL apoc.neighbors.athop.count(n,'KNOWS>', 3) YIELD value AS number "
                        + "RETURN number",
                (row) -> assertEquals(1L, row.get("number")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"tohop", "tohop.count"})
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void toHopWithMaxDistanceReturnsPromptlyWhenTheFrontierEmpties(String procedure) {
        // The hop counter used to be an int compared against a long bound, so this looped forever doing nothing
        TestUtil.testResult(
                db,
                "MATCH (n:First) CALL apoc.neighbors." + procedure + "(n, 'NOPE>', 9223372036854775807) YIELD "
                        + column(procedure)
                        + " "
                        + "RETURN count(*) AS rows",
                result -> assertEquals(
                        "tohop".equals(procedure) ? 0L : 1L, result.next().get("rows")));
        TestUtil.testResult(
                db,
                "MATCH (n:First) CALL apoc.neighbors." + procedure + "(n, 'KNOWS>', 9223372036854775807) YIELD "
                        + column(procedure)
                        + " "
                        + "RETURN count(*) AS rows",
                result -> assertEquals(
                        "tohop".equals(procedure) ? 3L : 1L, result.next().get("rows")));
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void cycleWithMaxDistanceReturnsPromptly() {
        // Every traversal skips nodes it has already seen, so even on a cycle the frontier empties after two hops.
        // The unfixed hop loop kept iterating over the empty frontier; it now stops there.
        String call = "MATCH (n:CycleStart) CALL apoc.neighbors.%s(n, 'CYCLE>', 9223372036854775807) ";
        TestUtil.testCall(
                db,
                String.format(call, "tohop") + "YIELD node RETURN count(node) AS value",
                row -> assertEquals(1L, row.get("value")));
        TestUtil.testCall(
                db,
                String.format(call, "tohop.count") + "YIELD value RETURN value",
                row -> assertEquals(1L, row.get("value")));
        TestUtil.testCall(
                db,
                String.format(call, "athop") + "YIELD node RETURN count(node) AS value",
                row -> assertEquals(0L, row.get("value")));
        TestUtil.testCall(
                db,
                String.format(call, "athop.count") + "YIELD value RETURN value",
                row -> assertEquals(0L, row.get("value")));
    }

    @ParameterizedTest
    @ValueSource(longs = {2000000000L, 2147483648L, 4294967296L, 9223372036854775807L})
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void atHopWithHugeDistanceReturnsPromptlyAndEmpty(long distance) {
        // These used to allocate one bitmap per hop up front, or fail on the int narrowing of `distance`
        TestUtil.testCall(
                db,
                "MATCH (n:First) CALL apoc.neighbors.athop(n, 'KNOWS>', $distance) YIELD node "
                        + "RETURN count(node) AS count",
                Map.of("distance", distance),
                row -> assertEquals(0L, row.get("count")));
        TestUtil.testCall(
                db,
                "MATCH (n:First) CALL apoc.neighbors.athop.count(n, 'KNOWS>', $distance) YIELD value " + "RETURN value",
                Map.of("distance", distance),
                row -> assertEquals(0L, row.get("value")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"byhop", "byhop.count"})
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void byHopRejectsDistanceAboveMaxHops(String procedure) {
        for (long distance : new long[] {DEFAULT_MAX_HOPS + 1L, 2000000000L, 9223372036854775807L}) {
            String query = "MATCH (n:First) CALL apoc.neighbors." + procedure + "(n, 'KNOWS>', " + distance + ") "
                    + "YIELD " + column(procedure) + " RETURN *";
            assertArgumentError(
                    query,
                    "'distance' must not exceed " + DEFAULT_MAX_HOPS + ", the value of " + APOC_MAX_HOPS + ", but was "
                            + distance);
        }
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void byHopAcceptsDistanceAtMaxHops() {
        TestUtil.testResult(
                db,
                "MATCH (n:PathStart) CALL apoc.neighbors.byhop(n, 'STEP>', $distance) YIELD nodes "
                        + "RETURN count(*) AS rows",
                Map.of("distance", DEFAULT_MAX_HOPS),
                result -> assertEquals((long) DEFAULT_MAX_HOPS, result.next().get("rows")));
        TestUtil.testCall(
                db,
                "MATCH (n:PathStart) CALL apoc.neighbors.byhop.count(n, 'STEP>', $distance) YIELD value "
                        + "RETURN value",
                Map.of("distance", DEFAULT_MAX_HOPS),
                row -> {
                    List<?> counts = (List<?>) row.get("value");
                    assertEquals((int) DEFAULT_MAX_HOPS, counts.size());
                    assertEquals(List.of(1L, 1L), counts.subList(0, 2));
                    assertTrue(counts.subList(2, counts.size()).stream().allMatch(c -> c.equals(0L)));
                });
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void byHopPadsToExactlyDistanceRows() {
        TestUtil.testResult(
                db,
                "MATCH (n:PathStart) CALL apoc.neighbors.byhop(n, 'STEP>', 1000) YIELD nodes RETURN nodes",
                result -> {
                    List<List<Node>> rows = result.stream()
                            .map(row -> (List<Node>) row.get("nodes"))
                            .collect(Collectors.toList());
                    assertEquals(1000, rows.size());
                    assertEquals(1, rows.get(0).size());
                    assertEquals(1, rows.get(1).size());
                    assertTrue(rows.subList(2, rows.size()).stream().allMatch(List::isEmpty));
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"tohop", "tohop.count", "byhop", "byhop.count", "athop", "athop.count"})
    void nullDistanceIsRejectedByName(String procedure) {
        String query = "MATCH (n:First) CALL apoc.neighbors." + procedure + "(n, 'KNOWS>', null) YIELD "
                + column(procedure) + " RETURN *";
        assertArgumentError(query, "'distance' must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tohop", "tohop.count", "byhop", "byhop.count", "athop", "athop.count"})
    void nullNodeIsRejectedByName(String procedure) {
        String query =
                "CALL apoc.neighbors." + procedure + "(null, 'KNOWS>', 2) YIELD " + column(procedure) + " RETURN *";
        assertArgumentError(query, "'node' must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = {"tohop", "tohop.count", "byhop", "byhop.count", "athop", "athop.count"})
    void earlyReturnsStillReturnEmpty(String procedure) {
        // These never reached the graph and returned nothing before the argument checks existed, so they still do
        String yield = " YIELD " + column(procedure) + " RETURN count(*) AS rows";
        for (String call : List.of(
                "CALL apoc.neighbors." + procedure + "(null, '', 2)",
                "CALL apoc.neighbors." + procedure + "(null, 'KNOWS>', 0)",
                "MATCH (n:First) CALL apoc.neighbors." + procedure + "(n, '', 9223372036854775807)")) {
            TestUtil.testCall(db, call + yield, row -> assertEquals(0L, row.get("rows")));
        }
    }

    private void assertArgumentError(String query, String expectedMessage) {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> db.executeTransactionally(query, Map.of(), Result::resultAsString),
                query);
        Throwable rootCause = ExceptionUtils.getRootCause(e);
        assertInstanceOf(IllegalArgumentException.class, rootCause);
        assertEquals(expectedMessage, rootCause.getMessage());
    }

    private static String column(String procedure) {
        return switch (procedure) {
            case "tohop", "athop" -> "node";
            case "byhop" -> "nodes";
            default -> "value";
        };
    }
}
