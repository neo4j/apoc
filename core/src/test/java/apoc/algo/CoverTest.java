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
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension()
public class CoverTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, Cover.class);
    }

    @Test
    public void testCover() {
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
}
