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
package apoc.cypher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import apoc.util.collection.Iterables;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

public class CypherTestUtil {
    public static final String CREATE_RETURNQUERY_NODES =
            "UNWIND range(0,3) as id \n" + "CREATE (n:ReturnQuery {id:id})-[:REL {idRel: id}]->(:Other {idOther: id})";

    public static final String CREATE_RESULT_NODES =
            "UNWIND range(0,3) as id \n" + "CREATE (n:Result {id:id})-[:REL {idRel: id}]->(:Other {idOther: id})";

    // placed in test-utils because is used by extended as well
    public static String SET_NODE =
            """
            MATCH (n:Result)-[:REL]->(:Other)
            SET n.updated = true
            RETURN n
            ORDER BY n.id;
            """;

    public static String SET_AND_RETURN_QUERIES =
            """
            MATCH (n:Result)-[:REL]->(:Other)
            SET n.updated = true
            RETURN n;

            MATCH (n:Result)-[rel:REL]->(o:Other)
            SET rel.updated = 1
            RETURN n, o, collect(rel) AS rels;

            MATCH (n:Result)-[rel:REL]->(o:Other)
            SET o.updated = 'true'
            RETURN collect(n) as nodes, collect(rel) as rels, collect(o) as others;
            """;

    public static String SIMPLE_RETURN_QUERIES = "MATCH (n:ReturnQuery) RETURN n ORDER BY n.id";

    public static void testRunProcedureWithSimpleReturnResults(
            Session session, String query, Map<String, Object> params) {
        session.executeWrite(tx -> tx.run(CREATE_RETURNQUERY_NODES).consume());
        assertThat(session.run(query, params).list())
                .satisfiesExactly(
                        row -> assertReturnQueryNode(row.asMap(), 0L),
                        row -> assertReturnQueryNode(row.asMap(), 1L),
                        row -> assertReturnQueryNode(row.asMap(), 2L),
                        row -> assertReturnQueryNode(row.asMap(), 3L),
                        row -> assertReadOnlyResult(row.asMap()));
    }

    public static void assertReadOnlyResult(Map<String, Object> row) {
        Map<?, ?> result = (Map<?, ?>) row.get("result");
        assertEquals(-1L, row.get("row"));
        assertEquals(0L, (long) result.get("nodesCreated"));
        assertEquals(0L, (long) result.get("propertiesSet"));
    }

    private static void assertReturnQueryNode(Map<String, Object> row, long id) {
        assertEquals(id, row.get("row"));

        Map<String, Node> result = (Map<String, Node>) row.get("result");
        assertEquals(1, result.size());
        assertReturnQueryNode(id, result);
    }

    public static void assertReturnQueryNode(long id, Map<String, Node> result) {
        Node n = result.get("n");
        assertEquals(List.of("ReturnQuery"), Iterables.asList(n.labels()));
        assertEquals(Map.of("id", id), n.asMap());
    }

    // placed in test-utils because is used by extended as well
    public static void testRunProcedureWithSetAndReturnResults(
            Session session, String query, Map<String, Object> params) {
        session.executeWrite(tx -> tx.run(CREATE_RESULT_NODES).consume());

        assertThat(session.run(query, params).list())
                .satisfiesExactly(
                        // check that all results from the 1st statement are correctly returned
                        row -> assertRunProcNode(row.asMap(), 0L),
                        row -> assertRunProcNode(row.asMap(), 1L),
                        row -> assertRunProcNode(row.asMap(), 2L),
                        row -> assertRunProcNode(row.asMap(), 3L),
                        // check `queryStatistics` row
                        row -> assertRunProcStatistics(row.asMap()),
                        // check that all results from the 2nd statement are correctly returned
                        row -> assertRunProcRel(row.asMap(), 0L),
                        row -> assertRunProcRel(row.asMap(), 1L),
                        row -> assertRunProcRel(row.asMap(), 2L),
                        row -> assertRunProcRel(row.asMap(), 3L),
                        // check `queryStatistics` row
                        row -> assertRunProcStatistics(row.asMap()),
                        // check that all results from the 3rd statement are correctly returned
                        row -> {
                            assertEquals(0L, row.get("row").asInt());
                            assertThat(row.get("result").asMap(Value::asList))
                                    .containsOnlyKeys("rels", "nodes", "others")
                                    .allSatisfy((k, v) -> assertThat(v).hasSize(4));
                        },
                        // check `queryStatistics` row
                        row -> assertRunProcStatistics(row.asMap()));

        // check that the procedure's SET operations work properly
        final var sq = "MATCH p=(:Result {updated:true})-[:REL {updated: 1}]->(:Other {updated: 'true'}) RETURN *";
        assertThat(session.run(sq).list()).hasSize(4);
    }

    private static void assertRunProcStatistics(Map<String, Object> row) {
        Map<?, ?> result = (Map<?, ?>) row.get("result");
        assertEquals(-1L, row.get("row"));
        assertEquals(0L, (long) result.get("nodesCreated"));
        assertEquals(4L, (long) result.get("propertiesSet"));
    }

    private static void assertRunProcNode(Map<String, Object> row, long id) {
        assertEquals(id, row.get("row"));

        Map<String, Node> result = (Map<String, Node>) row.get("result");
        assertEquals(1, result.size());
        assertResultNode(id, result);
    }

    public static void assertResultNode(long id, Map<String, Node> result) {
        Node n = result.get("n");
        assertEquals(List.of("Result"), Iterables.asList(n.labels()));
        assertEquals(Map.of("id", id, "updated", true), n.asMap());
    }

    private static void assertRunProcRel(Map<String, Object> row, long id) {
        assertEquals(id, row.get("row"));

        Map<String, Object> result = (Map<String, Object>) row.get("result");
        assertEquals(3, result.size());
        List<Relationship> n = (List<Relationship>) result.get("rels");
        assertEquals(1L, n.size());
        assertEquals("REL", n.get(0).type());
        assertEquals(Map.of("idRel", id, "updated", 1L), n.get(0).asMap());
    }
}
