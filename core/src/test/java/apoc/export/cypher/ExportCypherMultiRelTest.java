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
package apoc.export.cypher;

import static apoc.export.cypher.ExportCypherTestUtils.CLEANUP_EMPTY;
import static apoc.export.cypher.ExportCypherTestUtils.CLEANUP_SMALL_BATCH;
import static apoc.export.cypher.ExportCypherTestUtils.CLEANUP_SMALL_BATCH_NODES;
import static apoc.export.cypher.ExportCypherTestUtils.CLEANUP_SMALL_BATCH_ONLY_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_MULTI_RELS_ADD_STRUCTURE;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_MULTI_REL_CREATE;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_UNWIND;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_UNWIND_ADD_STRUCTURE;
import static apoc.export.cypher.ExportCypherTestUtils.NODES_UNWIND_UPDATE_STRUCTURE;
import static apoc.export.cypher.ExportCypherTestUtils.RELSUPDATE_STRUCTURE_2;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_ADD_STRUCTURE_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_UNWIND_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.RELS_UNWIND_UPDATE_ALL_MULTI_RELS;
import static apoc.export.cypher.ExportCypherTestUtils.SCHEMA_UPDATE_STRUCTURE_MULTI_REL;
import static apoc.export.cypher.ExportCypherTestUtils.SCHEMA_WITH_UNIQUE_IMPORT_ID;
import static apoc.export.cypher.formatter.CypherFormatterUtils.UNIQUE_ID_REL;
import static apoc.export.util.ExportConfig.RELS_WITH_TYPE_KEY;
import static apoc.util.Util.map;
import static org.assertj.core.api.Assertions.assertThat;

import apoc.cypher.Cypher;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
public class ExportCypherMultiRelTest {
    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfigRaw(Map.of(
                "internal.dbms.debug.track_cursor_close", "true",
                "db.format", "aligned" // Test assertions depends on sequential ids
                ));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, ExportCypher.class, Cypher.class);
    }

    @BeforeEach
    void beforeEach() {
        db.executeTransactionally(
                """
                create
                  (pers:Person {name: 'MyName'})-[:WORKS_FOR {id: 1}]->(proj:Project {a: 1}),
                  (pers)-[:WORKS_FOR {id: 2}]->(proj),
                  (pers)-[:WORKS_FOR {id: 2}]->(proj),
                  (pers)-[:WORKS_FOR {id: 3}]->(proj),
                  (pers)-[:WORKS_FOR {id: 4}]->(proj),
                  (pers)-[:WORKS_FOR {id: 5}]->(proj),
                  (pers)-[:IS_TEAM_MEMBER_OF {name: 'aaa'}]->(:Team {name: 'one'}),
                  (pers)-[:IS_TEAM_MEMBER_OF {name: 'eee'}]->(:Team {name: 'two'})
                """);
    }

    @Test
    public void updateAllOptimizationNone() {
        testsCommon(
                NODES_MULTI_RELS + SCHEMA_WITH_UNIQUE_IMPORT_ID + RELS_MULTI_RELS + CLEANUP_SMALL_BATCH,
                withoutOptimization(map("cypherFormat", "updateAll")));
    }

    @Test
    public void createOptimizationNone() {
        testsCommon(
                NODES_MULTI_REL_CREATE
                        + SCHEMA_WITH_UNIQUE_IMPORT_ID
                        + RELS_ADD_STRUCTURE_MULTI_RELS
                        + CLEANUP_SMALL_BATCH_NODES,
                withoutOptimization(map("cypherFormat", "create")));
    }

    @Test
    public void addStructureOptimizationNone() {
        testsCommon(
                NODES_MULTI_RELS_ADD_STRUCTURE
                        + SCHEMA_UPDATE_STRUCTURE_MULTI_REL
                        + RELS_ADD_STRUCTURE_MULTI_RELS
                        + CLEANUP_EMPTY,
                withoutOptimization(map("cypherFormat", "addStructure")));
    }

    @Test
    public void updateStructureOptimizationNone() {
        testsCommon(
                ":begin\n:commit\n" + SCHEMA_UPDATE_STRUCTURE_MULTI_REL + RELSUPDATE_STRUCTURE_2
                        + CLEANUP_SMALL_BATCH_ONLY_RELS,
                withoutOptimization(map("cypherFormat", "updateStructure")),
                true);
    }

    @Test
    public void updateAllWithOptimization() {
        testsCommon(
                SCHEMA_WITH_UNIQUE_IMPORT_ID + NODES_UNWIND_UPDATE_STRUCTURE + RELS_UNWIND_UPDATE_ALL_MULTI_RELS + "\n"
                        + CLEANUP_SMALL_BATCH,
                withOptimizationSmallBatch(map("cypherFormat", "updateAll")));
    }

    @Test
    public void createWithOptimization() {
        testsCommon(
                SCHEMA_WITH_UNIQUE_IMPORT_ID + NODES_UNWIND + RELS_UNWIND_MULTI_RELS + CLEANUP_SMALL_BATCH,
                withOptimizationSmallBatch(map("cypherFormat", "create")));
    }

    @Test
    public void addStructureWithOptimization() {
        testsCommon(
                SCHEMA_UPDATE_STRUCTURE_MULTI_REL
                        + NODES_UNWIND_ADD_STRUCTURE
                        + RELS_UNWIND_MULTI_RELS
                        + CLEANUP_SMALL_BATCH_ONLY_RELS,
                withOptimizationSmallBatch(map("cypherFormat", "addStructure")));
    }

    @Test
    public void addStructureWithOptimizationAndWithoutNodeCleanup() {
        testsCommon(
                SCHEMA_UPDATE_STRUCTURE_MULTI_REL
                        + NODES_UNWIND_ADD_STRUCTURE
                        + RELS_UNWIND_MULTI_RELS
                        + CLEANUP_SMALL_BATCH_ONLY_RELS,
                withOptimizationSmallBatch(map("cypherFormat", "addStructure")),
                false);
    }

    @Test
    public void updateStructureWithOptimization() {
        testsCommon(
                SCHEMA_UPDATE_STRUCTURE_MULTI_REL + RELS_UNWIND_UPDATE_ALL_MULTI_RELS + CLEANUP_SMALL_BATCH_ONLY_RELS,
                withOptimizationSmallBatch(map("cypherFormat", "updateStructure")),
                true);
    }

    private Map<String, Object> withoutOptimization(Map<String, Object> map) {
        map.put("useOptimizations", map("type", "none"));
        return map;
    }

    private Map<String, Object> withOptimizationSmallBatch(Map<String, Object> map) {
        map.put("useOptimizations", map("type", "unwind_batch", "unwindBatchSize", 5L));
        return map;
    }

    private void testsCommon(String expectedCypherStatement, Map<String, Object> otherConfigs) {
        testsCommon(expectedCypherStatement, otherConfigs, false);
    }

    private void testsCommon(String expectedCypherStatement, Map<String, Object> otherConfigs, boolean recreateNodes) {
        consistencyCheck();

        // all test with batch size, to ensure it works correctly and with multipleRelationshipsWithType: true
        final Map<String, Object> config = map("stream", true, RELS_WITH_TYPE_KEY, true, "batchSize", 5);
        config.putAll(otherConfigs);
        final String cypherStatements = db.executeTransactionally(
                "CALL apoc.export.cypher.all(null, $config)",
                map("config", config),
                r -> Iterators.stream(r.<String>columnAs("cypherStatements")).collect(Collectors.joining("\n")));

        // check cypherStatements result
        assertThat(cypherStatements.replace("  ", " ")).isEqualTo(expectedCypherStatement);

        // delete and recreate using export nodeStatements, relationshipStatements
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // re-create all
        if (recreateNodes) {
            // for 'cypherFormat: updateStructure', because doesn't create nodes, only match them
            db.executeTransactionally("CREATE (:Person:`UNIQUE IMPORT LABEL`{name: 'MyName', `UNIQUE IMPORT ID`:0}), "
                    + "(:Project:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}),"
                    + "(:Team:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2, name: 'one'}),"
                    + "(:Team:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3, name: 'two'})");
        }
        db.executeTransactionally("call apoc.cypher.runMany($statements, {})", map("statements", cypherStatements));

        // check that before and after import the results are equivalents
        consistencyCheck();
    }

    private void consistencyCheck() {
        final var worksForQuery =
                "match p=(start:Person {name: 'MyName'})-[rel:WORKS_FOR]->(end:Project) return rel ORDER BY rel.id";
        try (final var tx = db.beginTx();
                final var result = tx.execute(worksForQuery)) {
            final var resultList = result.<Relationship>columnAs("rel").stream().toList();
            final var firstRel = resultList.getFirst();
            assertThat(resultList)
                    .satisfiesExactly(
                            r -> assertThat(r.getProperty("id")).isEqualTo(1L),
                            r -> assertThat(r.getProperty("id")).isEqualTo(2L),
                            r -> assertThat(r.getProperty("id")).isEqualTo(2L),
                            r -> assertThat(r.getProperty("id")).isEqualTo(3L),
                            r -> assertThat(r.getProperty("id")).isEqualTo(4L),
                            r -> assertThat(r.getProperty("id")).isEqualTo(5L))
                    .allSatisfy(r -> assertThat(r.hasProperty(UNIQUE_ID_REL)).isFalse())
                    .allSatisfy(r -> assertThat(r.getStartNode()).isEqualTo(firstRel.getStartNode()))
                    .allSatisfy(r -> assertThat(r.getEndNode()).isEqualTo(firstRel.getEndNode()));
        }
        final var teamsQuery =
                "match p=(start {name: 'MyName'})-[rel:IS_TEAM_MEMBER_OF]->(end:Team) return end.name order by end.name";
        try (final var tx = db.beginTx();
                final var result = tx.execute(teamsQuery)) {
            assertThat(result.<String>columnAs("end.name").stream()).containsExactly("one", "two");
        }
    }
}
