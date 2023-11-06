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

import static apoc.util.TestContainerUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import java.util.List;
import java.util.Map;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
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
            RETURN n;
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
        session.writeTransaction(tx -> tx.run(CREATE_RETURNQUERY_NODES));
        // Due to flaky tests, this is a debug section to see if we can
        // figure out a reason why the results aren't always returning the
        // entire result set.
        session.readTransaction(tx -> {
            List<Record> result = tx.run("MATCH (n:ReturnQuery) RETURN n.id").list();
            System.out.println("DEBUG: testRunProcedureWithSimpleReturnResults");
            System.out.println("DEBUG: " + result.size());
            for (Record r : result) {
                System.out.println("DEBUG: " + r);
            }
            tx.commit();
            return null;
        });
        testResult(session, query, params, r -> {
            // check that all results from the 1st statement are correctly returned
            Map<String, Object> row = r.next();
            assertReturnQueryNode(row, 0L);
            row = r.next();
            assertReturnQueryNode(row, 1L);
            row = r.next();
            assertReturnQueryNode(row, 2L);
            row = r.next();
            assertReturnQueryNode(row, 3L);

            // check `queryStatistics` row
            row = r.next();
            assertReadOnlyResult(row);

            assertFalse(r.hasNext());
        });
    }

    public static void assertReadOnlyResult(Map<String, Object> row) {
        Map result = (Map) row.get("result");
        assertEquals(-1L, row.get("row"));
        assertEquals(0L, (long) result.get("nodesCreated"));
        assertEquals(0L, (long) result.get("propertiesSet"));
    }

    private static void assertReturnQueryNode(Map<String, Object> row, long id) {
        System.out.println("DEBUG: " + row);
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
        session.writeTransaction(tx -> tx.run(CREATE_RESULT_NODES));

        testResult(session, query, params, r -> {
            // check that all results from the 1st statement are correctly returned
            Map<String, Object> row = r.next();
            assertRunProcNode(row, 0L);
            row = r.next();
            assertRunProcNode(row, 1L);
            row = r.next();
            assertRunProcNode(row, 2L);
            row = r.next();
            assertRunProcNode(row, 3L);

            // check `queryStatistics` row
            row = r.next();
            assertRunProcStatistics(row);

            // check that all results from the 2nd statement are correctly returned
            row = r.next();
            assertRunProcRel(row, 0L);
            row = r.next();
            assertRunProcRel(row, 1L);
            row = r.next();
            assertRunProcRel(row, 2L);
            row = r.next();
            assertRunProcRel(row, 3L);

            // check `queryStatistics` row
            row = r.next();
            assertRunProcStatistics(row);

            // check that all results from the 3rd statement are correctly returned
            row = r.next();
            assertEquals(0L, row.get("row"));
            Map<String, Object> result = (Map<String, Object>) row.get("result");
            assertEquals(3, result.size());
            List<Relationship> rels = (List<Relationship>) result.get("rels");
            List<Node> nodes = (List<Node>) result.get("nodes");
            List<Node> others = (List<Node>) result.get("others");
            assertEquals(4L, rels.size());
            assertEquals(4L, nodes.size());
            assertEquals(4L, others.size());
            row = r.next();

            // check `queryStatistics` row
            assertRunProcStatistics(row);
            assertFalse(r.hasNext());
        });

        // check that the procedure's SET operations work properly
        testResult(
                session,
                "MATCH p=(:Result {updated:true})-[:REL {updated: 1}]->(:Other {updated: 'true'}) RETURN *",
                r -> assertEquals(4L, Iterators.count(r)));
    }

    private static void assertRunProcStatistics(Map<String, Object> row) {
        Map result = (Map) row.get("result");
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
