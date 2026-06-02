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

import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension
class CoverTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, Cover.class);
    }

    @BeforeEach
    void setUp() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }

    @Test
    void testCover() {
        db.executeTransactionally("CREATE (a)-[:X]->(b)-[:X]->(c)-[:X]->(d)");
        List<String> nodeRepresentations = List.of("n", "id(n)", "elementId(n)");
        for (String nodeRep : nodeRepresentations) {
            TestUtil.testCall(
                    db,
                    String.format(
                            """
                            MATCH (n)
                            WITH collect(%s) AS nodes
                            CALL apoc.algo.cover(nodes)
                            YIELD rel
                            RETURN count(*) AS c
                        """,
                            nodeRep),
                    (r) -> assertEquals(3L, r.get("c")));
        }
    }

    @Test
    void testCoverEmpty() {
        TestUtil.testCall(
                db, "CALL apoc.algo.cover([]) YIELD rel RETURN count(*) AS c", r -> assertEquals(0L, r.get("c")));
    }

    @Test
    void testCoverSingleNode() {
        db.executeTransactionally("CREATE (:Single)");
        TestUtil.testCall(
                db,
                """
                    MATCH (n:Single)
                    WITH collect(n) AS nodes
                    CALL apoc.algo.cover(nodes)
                    YIELD rel
                    RETURN count(*) AS c
                """,
                r -> assertEquals(0L, r.get("c")));
    }

    @Test
    void testCoverDisconnectedNodes() {
        db.executeTransactionally("CREATE (:Disco), (:Disco)");
        TestUtil.testCall(
                db,
                """
                        MATCH (n:Disco)
                        WITH collect(n) AS nodes
                        CALL apoc.algo.cover(nodes)
                        YIELD rel
                        RETURN count(*) AS c
                    """,
                r -> assertEquals(0L, r.get("c")));
    }

    @Test
    void testCoverSelfLoop() {
        db.executeTransactionally("CREATE (a:SelfLoop)-[:X]->(a)");
        TestUtil.testCall(
                db,
                """
                        MATCH (n:SelfLoop)
                        WITH collect(n) AS nodes
                        CALL apoc.algo.cover(nodes)
                        YIELD rel RETURN count(*) AS c
                    """,
                r -> assertEquals(1L, r.get("c")));
    }

    @Test
    void testCoverMultipleRelTypes() {
        db.executeTransactionally("CREATE (a:Multi)-[:X]->(b:Multi)-[:Y]->(c:Multi)");
        TestUtil.testCall(
                db,
                """
                        MATCH (n:Multi)
                        WITH collect(n) AS nodes
                        CALL apoc.algo.cover(nodes)
                        YIELD rel
                        RETURN count(*) AS c
                    """,
                r -> assertEquals(2L, r.get("c")));
    }

    @Test
    void testCoverIncomingRelationship() {
        // relationship points a<-b; cover finds it via b's outgoing rels when both nodes are in the set
        db.executeTransactionally("CREATE (a:Incoming)<-[:X]-(b:Incoming)");
        TestUtil.testCall(
                db,
                """
                    MATCH (n:Incoming)
                    WITH collect(n) AS nodes
                    CALL apoc.algo.cover(nodes)
                    YIELD rel
                    RETURN count(*) AS c
                   """,
                r -> assertEquals(1L, r.get("c")));
    }

    @Test
    void testCoverSubset() {
        // chain of 3; passing only the first two nodes should return only the relationship between them
        db.executeTransactionally("CREATE (:Sub {id: 1})-[:X]->(:Sub {id: 2})-[:X]->(:Sub {id: 3})");
        TestUtil.testCall(
                db,
                """
                    MATCH (n:Sub)
                    WHERE n.id IN [1, 2]
                    WITH collect(n) AS nodes
                    CALL apoc.algo.cover(nodes)
                    YIELD rel
                    RETURN count(*) AS c
                """,
                r -> assertEquals(1L, r.get("c")));
    }
}
