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

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_BEGIN_AND_FOO;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CLEAN_UP;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CLEAN_UP_EMPTY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_DATE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_DURATION;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_LABELS_ASCENDEND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_POINT;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_SHELL_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_CYPHER_TIME;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_MERGE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NEO4J_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES_EMPTY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES_MERGE_ON_CREATE_SET;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_NODES_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_ONLY_SCHEMA_CYPHER_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_ONLY_SCHEMA_NEO4J_SHELL;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_ONLY_SCHEMA_NEO4J_SHELL_WITH_NAMES;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED3;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED4;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_QUERY_PARAMS_ODD;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_RELATIONSHIPS_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_REL_ONLY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_EMPTY;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_ONLY_START;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_OPTIMIZED_WITH_IF_NOT_EXISTS;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_OPTIMIZED_WITH_RELS_IF_NOT_EXISTS_AND_NAME;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_WITH_NAMES;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_WITH_RELS_AND_IF_NOT_EXISTS;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_WITH_RELS_AND_NAME_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_SCHEMA_WITH_RELS_OPTIMIZED;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_UPDATE_ALL_UNWIND;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.EXPECTED_WITHOUT_END_NODE;
import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.convertToCypherShellFormat;
import static apoc.export.util.ExportFormat.CYPHER_SHELL;
import static apoc.export.util.ExportFormat.NEO4J_SHELL;
import static apoc.export.util.ExportFormat.PLAIN_FORMAT;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.TestUtil.assertError;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static apoc.util.Util.map;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

import apoc.export.util.ExportConfig;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {
    private static final String queryWithRelOnly = "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) RETURN rel";
    private static final String queryWithStartAndRel = "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) RETURN start, rel";
    private static final String exportQuery = "CALL apoc.export.cypher.query($query, $file, $config)";

    private static final String exportDataWithStartAndRel =
            """
                MATCH (start:Foo)-[rel:KNOWS]->(end:Bar)
                CALL apoc.export.cypher.data([start], [rel], $file, $config)
                YIELD nodes, relationships, properties RETURN *""";
    private static final String exportDataWithRelOnly =
            """
                MATCH (start:Foo)-[rel:KNOWS]->(end:Bar)
                CALL apoc.export.cypher.data([], [rel], $file, $config)
                YIELD nodes, relationships, properties RETURN *""";

    private static final Map<String, Object> exportConfig =
            map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");

    private static final File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    directory.toPath().toAbsolutePath())
            .withSetting(
                    newBuilder("internal.dbms.debug.track_cursor_close", BOOL, false)
                            .build(),
                    false)
            .withSetting(
                    newBuilder("internal.dbms.debug.trace_cursors", BOOL, false).build(), false);

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() {
        ExportCypherTestUtils.setUp(db, testName);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testRoundTripCypherExportAll() {
        final String cypherStatements = Util.readResourceFile("exportAll.cypher");
        db.executeTransactionally("CALL apoc.cypher.runMany($statements, {})", map("statements", cypherStatements));

        String fileName = "all.cypher";
        final String query =
                "CALL apoc.export.cypher.all($fileName, {batchSize: 100, format: 'cypher-shell', saveIndexNames: true, saveConstraintNames: true, multipleRelationshipsWithType: true})";

        TestUtil.testCall(db, query, map("fileName", fileName), (r) -> assertEquals(fileName, r.get("file")));

        assertEquals(cypherStatements, readFile("all.cypher").strip());
    }

    @Test
    public void testUniqueNodeLabels() {
        // Check the unique node test doesn't fail if the node has extra non-unique constrained labels
        String createExtraNodes = "MATCH (n) SET n:EXTRA_LABEL";
        db.executeTransactionally(createExtraNodes);

        try (Transaction tx = db.beginTx()) {
            MultiStatementCypherSubGraphExporter exporter =
                    new MultiStatementCypherSubGraphExporter(new DatabaseSubGraph(tx), new ExportConfig(null), db);

            assertEquals(exporter.countArtificialUniques(tx.getAllNodes()), 2);
        }
    }

    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                (r) -> {
                    assertResults(null, r, "database");
                    assertEquals(EXPECTED_NEO4J_SHELL, r.get("cypherStatements"));
                });
    }

    @Test
    public void testExportAllCypherStreaming() {
        final String query =
                "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, streamStatements:true,batchSize:3, format: 'neo4j-shell'})";
        assertExportAllCypherStreaming(CompressionAlgo.NONE, query);
    }

    @Test
    public void testExportAllWithCompressionCypherStreaming() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        final String query = "CALL apoc.export.cypher.all(null,{compression: '" + algo.name()
                + "', useOptimizations: { type: 'none'}, streamStatements:true,batchSize:3, format: 'neo4j-shell'})";
        assertExportAllCypherStreaming(algo, query);
    }

    @Test
    public void testExportAllWithStreamingSeparatedAndCompressedFile() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        final Map<String, Object> config = map(
                "compression",
                algo.name(),
                "useOptimizations",
                map("type", "none"),
                "stream",
                true,
                "separateFiles",
                true,
                "format",
                "neo4j-admin");

        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null, $config)", map("config", config), r -> {
            assertEquals(EXPECTED_NODES, getDecompressedData(algo, r.get("nodeStatements")));
            assertEquals(EXPECTED_RELATIONSHIPS, getDecompressedData(algo, r.get("relationshipStatements")));
            assertEquals(EXPECTED_SCHEMA, getDecompressedData(algo, r.get("schemaStatements")));
            assertEquals(EXPECTED_CLEAN_UP, getDecompressedData(algo, r.get("cleanupStatements")));
        });
    }

    private void assertExportAllCypherStreaming(CompressionAlgo algo, String query) {
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, query, (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(5L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("cypher", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("cypherStatements")));
            r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("cypherStatements")));
        });
        assertEquals(EXPECTED_NEO4J_SHELL.replace("LIMIT 20000", "LIMIT 3"), sb.toString());
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() {
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($fileName,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                map("fileName", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportAllCypherForCypherShell() {
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,$config)",
                map("file", fileName, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherForNeo4j() {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportQueryOnlyRel() {
        final Map<String, Object> config = withoutOptimization(map("format", "neo4j-shell"));
        Map<String, Object> params = Map.of("query", queryWithRelOnly, "config", config);
        assertExportRelOnly(params, exportQuery);

        // check that apoc.export.cypher.data returns consistent results
        assertExportRelOnly(params, exportDataWithRelOnly);
    }

    @Test
    public void testExportQueryOnlyRelWithNodeOfRelsTrue() {
        // check that `nodesOfRelationships: true` doesn't change the result
        final Map<String, Object> config =
                withoutOptimization(map("format", "neo4j-shell", "nodesOfRelationships", true));
        Map<String, Object> params = Map.of("query", queryWithRelOnly, "config", config);
        assertExportRelOnly(params, exportQuery);

        // check that apoc.export.cypher.data returns consistent results
        assertExportRelOnly(params, exportDataWithRelOnly);
    }

    @Test
    public void testExportQueryOnlyRelAndStart() {
        final Map<String, Object> config = withoutOptimization(map("format", "neo4j-shell"));
        Map<String, Object> params = Map.of("query", queryWithStartAndRel, "config", config);
        assertExportWithoutEndNode(params, exportQuery);

        // check that apoc.export.cypher.data returns consistent results
        assertExportWithoutEndNode(params, exportDataWithStartAndRel);
    }

    @Test
    public void testExportQueryOnlyRelAndStartWithNodeOfRelsTrue() {
        // check that with {nodesOfRelationships: true} the end node is returned as well
        final Map<String, Object> config =
                withoutOptimization(map("format", "neo4j-shell", "nodesOfRelationships", true));
        Map<String, Object> params = Map.of("query", queryWithStartAndRel, "config", config);

        String expectedNodesWithEndNode = String.format(EXPECTED_BEGIN_AND_FOO
                + "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:42, name:\"bar\", `UNIQUE IMPORT ID`:1});%n"
                + "COMMIT%n");
        String expectedWithEndNode =
                expectedNodesWithEndNode + EXPECTED_SCHEMA_ONLY_START + EXPECTED_REL_ONLY + EXPECTED_CLEAN_UP;

        commonDataAndQueryAssertions(exportQuery, params, expectedWithEndNode, 2L, 5L);

        // apoc.export.cypher.data doesn't accept `nodesOfRelationships` config,
        // so it returns the same result as the above one
        assertExportWithoutEndNode(params, exportDataWithStartAndRel);
    }

    private void assertExportWithoutEndNode(Map<String, Object> params, String exportData) {
        commonDataAndQueryAssertions(exportData, params, EXPECTED_WITHOUT_END_NODE, 1L, 3L);
    }

    private void assertExportRelOnly(Map<String, Object> params, String exportData) {
        commonDataAndQueryAssertions(exportData, params, EXPECTED_REL_ONLY, 0L, 1L);
    }

    private void commonDataAndQueryAssertions(
            String query, Map<String, Object> config, String expectedOutput, long nodes, long properties) {
        String fileName = "testFile.cypher";

        Map<String, Object> params = map("file", fileName);
        params.putAll(config);

        TestUtil.testCall(db, query, params, r -> {
            assertEquals(nodes, r.get("nodes"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(properties, r.get("properties"));
        });
        assertEquals(expectedOutput, readFile(fileName));
    }

    @Test
    public void testExportCypherAdminOperationErrorMessage() {
        String filename = "test.cypher";
        List<String> invalidQueries =
                List.of("SHOW CONSTRAINTS YIELD id, name, type RETURN *", "SHOW INDEXES YIELD id, name, type RETURN *");

        for (String query : invalidQueries) {
            QueryExecutionException e = Assert.assertThrows(
                    QueryExecutionException.class,
                    () -> TestUtil.testCall(
                            db,
                            exportQuery,
                            MapUtil.map(
                                    "query", query,
                                    "file", filename,
                                    "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                            (r) -> assertResults(filename, r, "database")));

            assertError(e, INVALID_QUERY_MODE_ERROR, RuntimeException.class, "apoc.export.cypher.query");
        }
    }

    private static String readFile(String fileName) {
        return readFile(fileName, CompressionAlgo.NONE);
    }

    private static String readFile(String fileName, CompressionAlgo algo) {
        return BinaryTestUtil.readFileToString(new File(directory, fileName), UTF_8, algo);
    }

    @Test
    public void testExportGraphCypher() {
        String fileName = "graph.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map(
                        "file",
                        fileName,
                        "exportConfig",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() {
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,$exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NODES, readFile("all.nodes.cypher"));
    }

    @Test
    public void testExportAllCypherRelationships() {
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,$exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("all.relationships.cypher"));
    }

    @Test
    public void testExportAllCypherSchema() {
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,$exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherSchemaWithSaveIdxNames() {
        final Map<String, Object> config = new HashMap<>(ExportCypherTest.exportConfig);
        config.put("saveIndexNames", true);
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,$config)",
                map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_SCHEMA_WITH_NAMES, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherCleanUp() {
        String fileName = "all.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,$exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("all.cleanup.cypher"));
    }

    @Test
    public void testExportGraphCypherNodes() {
        String fileName = "graph.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile("graph.nodes.cypher"));
    }

    @Test
    public void testExportGraphCypherRelationships() {
        String fileName = "graph.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("graph.relationships.cypher"));
    }

    @Test
    public void testExportGraphCypherSchema() {
        String fileName = "graph.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, readFile("graph.schema.cypher"));
    }

    @Test
    public void testExportGraphCypherCleanUp() {
        String fileName = "graph.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("graph.cleanup.cypher"));
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "plain")),
                (r) -> {});
        assertEquals(EXPECTED_PLAIN, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map(
                                "useOptimizations",
                                map("type", "none"),
                                "format",
                                "neo4j-shell",
                                "cypherFormat",
                                "updateAll")),
                (r) -> {});
        assertEquals(EXPECTED_NEO4J_MERGE, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map(
                                "useOptimizations",
                                map("type", "none"),
                                "format",
                                "neo4j-shell",
                                "cypherFormat",
                                "addStructure")),
                (r) -> {});
        assertEquals(
                EXPECTED_NODES_MERGE_ON_CREATE_SET
                        + EXPECTED_SCHEMA_EMPTY
                        + EXPECTED_RELATIONSHIPS
                        + EXPECTED_CLEAN_UP_EMPTY,
                readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map(
                                "useOptimizations",
                                map("type", "none"),
                                "format",
                                "neo4j-shell",
                                "cypherFormat",
                                "updateStructure")),
                (r) -> {});
        assertEquals(
                EXPECTED_NODES_EMPTY
                        + EXPECTED_SCHEMA_EMPTY
                        + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET
                        + EXPECTED_CLEAN_UP_EMPTY,
                readFile(fileName));
    }

    @Test
    public void testExportSchemaCypher() {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.schema($file,$exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypherWithStreamTrue() {
        Consumer<Map<String, Object>> resultAssertion = (r) -> {
            Object actual = r.get("cypherStatements");
            assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, actual);
        };

        // with {stream: true}
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema(null, {stream: true})", resultAssertion);

        // with {streamStatements: true}
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema(null, {streamStatements: true})", resultAssertion);

        // assert `schemaStatements` of `apoc.export.cypher.all`
        // is equal to `cypher.schema` output plus the `CREATE CONSTRAINT UNIQUE_IMPORT_NAME ...` statement
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null, { separateFiles: true, stream: true})", r -> {
            Object actual = r.get("schemaStatements");
            assertEquals(convertToCypherShellFormat(EXPECTED_SCHEMA), actual);
        });
    }

    @Test
    public void testExportSchemaCypherWithIdxNames() {
        final Map<String, Object> config = new HashMap<>(ExportCypherTest.exportConfig);
        config.putAll(map("saveIndexNames", true));
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.schema($file,$config)",
                map("file", fileName, "config", config),
                (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL_WITH_NAMES, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypherShell() {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.schema($file,$exportConfig)",
                map(
                        "file",
                        fileName,
                        "exportConfig",
                        map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportCypherNodePoint() {
        db.executeTransactionally("CREATE (f:Test {name:'foo'," + "place2d:point({ x: 2.3, y: 4.5 }),"
                + "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})"
                + "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->"
                + "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})");
        String fileName = "temporalPoint.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_POINT, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDate() {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " + "date:date('2018-10-30'), "
                + "datetime:datetime('2018-10-30T12:50:35.556+0100'), "
                + "localTime:localdatetime('20181030T19:32:24')})"
                + "-[:FRIEND_OF {date:date('2018-10-30')}]->"
                + "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})");
        String fileName = "temporalDate.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DATE, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeTime() {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " + "local:localtime('12:50:35.556'),"
                + "t:time('125035.556+0100')})"
                + "-[:FRIEND_OF {t:time('125035.556+0100')}]->"
                + "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})");
        String fileName = "temporalTime.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_TIME, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDuration() {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " + "duration:duration('P5M1.5D')})"
                + "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->"
                + "(:Bar {duration:duration('P5M1.5D')})");
        String fileName = "temporalDuration.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DURATION, readFile(fileName));
    }

    @Test
    public void testExportWithAscendingLabels() {
        db.executeTransactionally("CREATE (f:User:User1:User0:User12 {name:'Alan'})");
        String fileName = "ascendingLabels.cypher";
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(
                db,
                exportQuery,
                map(
                        "file",
                        fileName,
                        "query",
                        query,
                        "config",
                        map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_LABELS_ASCENDEND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, format: 'neo4j-shell'})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultOptimized() {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file, $exportConfig)",
                map("file", fileName, "exportConfig", map("format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file, $exportConfig)",
                map("file", fileName, "exportConfig", map("separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("allDefaultOptimized.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("allDefaultOptimized.relationships.cypher"));
        assertEquals(EXPECTED_SCHEMA, readFile("allDefaultOptimized.schema.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("allDefaultOptimized.cleanup.cypher"));
    }

    @Test
    public void testExportAllCypherWithIfNotExistsFalseOptimized() {
        String fileName = "ifNotExists.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file, $config)",
                map(
                        "file",
                        fileName,
                        "config",
                        map("ifNotExists", true, "separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_SCHEMA_OPTIMIZED_WITH_IF_NOT_EXISTS, readFile("ifNotExists.schema.cypher"));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("ifNotExists.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("ifNotExists.relationships.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("ifNotExists.cleanup.cypher"));
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'cypher-shell'})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() {
        String fileName = "allPlainAddStructureOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
                });
        assertEquals(EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_UPDATE_ALL_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithCompressionWithUnwindBatchSizeWithBatchSizeOptimized() {
        final CompressionAlgo algo = CompressionAlgo.DEFLATE;
        String fileName = "allPlainOptimized.cypher.ZZ";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{compression: '" + algo.name()
                        + "', format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, readFile(fileName, algo));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOddDataset() {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddDataset() {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    public void testExportWithCompressionQueryCypherShellUnwindBatchParamsWithOddDataset() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        String fileName = "allPlainOdd.cypher.bz2";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{compression: '" + algo.name()
                        + "', format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, readFile(fileName, algo));
    }

    @Test
    public void testExportAllCypherPlainOptimized() {
        String fileName = "queryPlainOptimized.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.query('MATCH (f:Foo)-[r:KNOWS]->(b:Bar) return f,r,b', $file,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(10L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("statement: nodes(4), rels(2)", r.get("source"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
                });
        String actual = readFile(fileName);
        assertTrue(
                "expected generated output ",
                EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED.equals(actual)
                        || EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2.equals(actual)
                        || EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED3.equals(actual)
                        || EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED4.equals(actual));
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddBatchSizeOddDataset() {
        db.executeTransactionally("CREATE (:Bar {name:'bar3',age:35}), (:Bar {name:'bar4',age:36})");
        String fileName = "allPlainOddNew.cypher";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:3})",
                map("file", fileName),
                (r) -> {});
        db.executeTransactionally("MATCH (n:Bar {name:'bar3',age:35}), (n1:Bar {name:'bar4',age:36}) DELETE n, n1");
        assertEquals(EXPECTED_QUERY_PARAMS_ODD, readFile(fileName));
    }

    @Test
    public void exportMultiTokenIndex() {
        // given
        db.executeTransactionally("CREATE (n:TempNode {value:'value', value2:'value'})");
        db.executeTransactionally("CREATE (n:TempNode2 {value:'value', value:'value2'})");
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX MyCoolNodeFulltextIndex FOR (n:TempNode|TempNode2) ON EACH [n.value, n.value2]");

        String query = "MATCH (t:TempNode) return t";
        Map<String, Object> config = map("awaitForIndexes", 3000);
        String expected = String.format(":begin%n"
                + "CREATE FULLTEXT INDEX MyCoolNodeFulltextIndex FOR (n:TempNode|TempNode2) ON EACH [n.`value`,n.`value2`];%n"
                + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                + ":commit%n"
                + "CALL db.awaitIndexes(3000);%n"
                + ":begin%n"
                + "UNWIND [{_id:3, properties:{value2:\"value\", value:\"value\"}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:TempNode;%n"
                + ":commit%n"
                + ":begin%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + ":commit%n"
                + ":begin%n"
                + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n"
                + ":commit%n");

        // when
        TestUtil.testCall(db, exportQuery, map("query", query, "file", null, "config", config), (r) -> {
            // then
            assertEquals(expected, r.get("cypherStatements"));
        });
    }

    @Test
    public void shouldExportFulltextIndexForRelationship() {
        // given
        db.executeTransactionally("CREATE (s:TempNode)-[:REL{rel_value: 'the rel value'}]->(e:TempNode2)");
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX MyCoolRelFulltextIndex FOR ()-[rel:REL]-() ON EACH [rel.rel_value]");
        String query = "MATCH (t:TempNode)-[r:REL{rel_value: 'the rel value'}]->(e:TempNode2) return t,r";
        Map<String, Object> config = map("awaitForIndexes", 3000);
        String expected = String.format(":begin%n"
                + "CREATE FULLTEXT INDEX MyCoolRelFulltextIndex FOR ()-[rel:REL]-() ON EACH [rel.`rel_value`];%n"
                + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                + ":commit%n"
                + "CALL db.awaitIndexes(3000);%n"
                + ":begin%n"
                + "UNWIND [{_id:3, properties:{}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:TempNode;%n"
                + ":commit%n"
                + ":begin%n"
                + "UNWIND [{start: {_id:3}, end: {_id:4}, properties:{rel_value:\"the rel value\"}}] AS row%n"
                + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                + "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})%n"
                + "CREATE (start)-[r:REL]->(end) SET r += row.properties;%n"
                + ":commit%n"
                + ":begin%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + ":commit%n"
                + ":begin%n"
                + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n"
                + ":commit%n");

        // when
        TestUtil.testCall(db, exportQuery, map("query", query, "file", null, "config", config), (r) -> {
            // then
            assertEquals(expected, r.get("cypherStatements"));
        });
    }

    @Test
    public void shouldNotCreateUniqueImportIdForUniqueConstraint() {
        db.executeTransactionally("CREATE (n:Bar:Baz{name: 'A'})");
        String query = "MATCH (n:Baz) RETURN n";
        /* The bug was:
        UNWIND [{name:"A", _id:20, properties:{}}] AS row
        CREATE (n:Bar{name: row.name, `UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Baz;
        But should be the expected variable
         */
        final String expected = "UNWIND [{name:\"A\", properties:{}}] AS row\n"
                + "CREATE (n:Bar{name: row.name}) SET n += row.properties SET n:Baz";
        TestUtil.testCall(
                db,
                exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)),
                (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldManageBigNumbersCorrectly() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        db.executeTransactionally("CREATE (:Bar{name:'Foo', var1:1.416785E32}), (:Bar{name:'Bar', var1:12E4});");
        final String expected =
                "UNWIND [{name:\"Foo\", properties:{var1:1.416785E32}}, {name:\"Bar\", properties:{var1:120000.0}}] AS row\n"
                        + "CREATE (n:Bar{name: row.name}) SET n += row.properties";
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file, $config)",
                map("file", null, "config", map("format", "plain", "stream", true)),
                (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldQuotePropertyNameStartingWithDollarCharacter() {
        db.executeTransactionally("CREATE (n:Bar:Baz{name: 'A', `$lock`: true})");
        String query = "MATCH (n:Baz) RETURN n";
        final String expected = "UNWIND [{name:\"A\", properties:{`$lock`:true}}] AS row\n"
                + "CREATE (n:Bar{name: row.name}) SET n += row.properties SET n:Baz";
        TestUtil.testCall(
                db,
                exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)),
                (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldHandleTwoLabelsWithOneUniqueConstraintEach() {
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint1 FOR (b:Base) REQUIRE b.id IS UNIQUE");
        db.executeTransactionally("CREATE (b:Bar:Base {id:'waBfk3z', name:'barista',age:42})"
                + "-[:KNOWS]->(f:Foo {name:'foosha', born:date('2018-10-31')})");
        String query = "MATCH (b:Bar:Base)-[r:KNOWS]-(f:Foo) RETURN b, f, r";
        /* The bug was:
        UNWIND [{start: {name:"barista"}, end: {_id:4}, properties:{}}] AS row
        MATCH (start:Bar{name: row.start.name, id: row.start.id})    // <-- Notice id here but not above in UNWIND!
        MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
        CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
        But should be the expected variable
         */
        final String expected =
                """
                UNWIND [{start: {name:"barista"}, end: {_id:4}, properties:{}}] AS row
                MATCH (start:Bar{name: row.start.name})
                MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
                CREATE (start)-[r:KNOWS]->(end) SET r += row.properties""";
        TestUtil.testCall(
                db,
                exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)),
                (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .filter(s -> s.contains("barista"))
                            .skip(1)
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldHandleTwoLabelsWithTwoUniqueConstraintsEach() {
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint1 FOR (b:Base) REQUIRE b.id IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint2 FOR (b:Base) REQUIRE b.oid IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint3 FOR (b:Bar) REQUIRE b.oname IS UNIQUE");
        db.executeTransactionally("CREATE (b:Bar:Base {id:'waBfk3z',oid:42,name:'barista',oname:'bar',age:42})"
                + "-[:KNOWS]->(f:Foo {name:'foosha', born:date('2018-10-31')})");
        String query = "MATCH (b:Bar:Base)-[r:KNOWS]-(f:Foo) RETURN b, f, r";
        final String expected =
                """
                UNWIND [{start: {name:"barista"}, end: {_id:4}, properties:{}}] AS row
                MATCH (start:Bar{name: row.start.name})
                MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
                CREATE (start)-[r:KNOWS]->(end) SET r += row.properties""";
        TestUtil.testCall(
                db,
                exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)),
                (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .filter(s -> s.contains("barista"))
                            .skip(1)
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldSaveCorrectlyRelIndexesOptimized() {
        String fileName = "relIndex.cypher";
        db.executeTransactionally("CREATE RANGE INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since, r.foo)");

        relIndexTestCommon(fileName, EXPECTED_SCHEMA_WITH_RELS_OPTIMIZED, map("ifNotExists", false));

        // with ifNotExists: true
        relIndexTestCommon(fileName, EXPECTED_SCHEMA_WITH_RELS_AND_IF_NOT_EXISTS, map("ifNotExists", true));

        db.executeTransactionally("DROP INDEX rel_index_name");
    }

    @Test
    public void shouldSaveCorrectlyRelIndexesWithNameOptimized() {
        String fileName = "relIndex.cypher";
        db.executeTransactionally("CREATE RANGE INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since, r.foo)");

        relIndexTestCommon(
                fileName,
                EXPECTED_SCHEMA_WITH_RELS_AND_NAME_OPTIMIZED,
                map("ifNotExists", false, "saveIndexNames", true));

        // with ifNotExists: true
        relIndexTestCommon(
                fileName,
                EXPECTED_SCHEMA_OPTIMIZED_WITH_RELS_IF_NOT_EXISTS_AND_NAME,
                map("ifNotExists", true, "saveIndexNames", true));

        db.executeTransactionally("DROP INDEX rel_index_name");
    }

    @Test
    public void testIssue2886OptimizationsNoneAndCypherFormatCreate() {
        final Map<String, Object> config = map("cypherFormat", "create");
        issue2886Common(
                actual -> check2886FullStructureNonOptimized(actual, "create"), withoutOptimization(config), true);
    }

    @Test
    public void testIssue2886OptimizationsNoneAndCypherFormatAddStructure() {
        final Map<String, Object> config = map("cypherFormat", "addStructure");
        issue2886Common(
                actual -> {
                    String[] lines = actual.split("[\r\n]+");
                    assertEquals(6, lines.length);
                    check2886UpdateNodeStructureNoOptimization(lines[0], lines[1], lines[2], lines[3], "addStructure");
                    check2886UpdateRelStructureNoOptimization(lines[4], lines[5], "addStructure");
                    check2886IdsNoOptimization(actual);
                },
                withoutOptimization(config),
                true);
    }

    @Test
    public void testIssue2886OptimizationsNoneAndCypherFormatUpdateStructure() {
        final Map<String, Object> config = map("cypherFormat", "updateStructure");
        issue2886Common(
                actual -> {
                    String[] lines = actual.split("[\r\n]+");
                    assertEquals(2, lines.length);
                    check2886UpdateRelStructureNoOptimization(lines[0], lines[1], "updateStructure");
                },
                withoutOptimization(config),
                false);
    }

    @Test
    public void testIssue2886OptimizationsNoneAndCypherFormatUpdateAll() {
        final Map<String, Object> config = map("cypherFormat", "updateAll");
        issue2886Common(
                actual -> check2886FullStructureNonOptimized(actual, "updateAll"), withoutOptimization(config), true);
    }

    @Test
    public void testIssue2886CypherFormatCreate() {
        final Map<String, Object> config = map("cypherFormat", "create");
        issue2886Common(actual -> check2886FullStructure(actual, "create"), config, true);
    }

    @Test
    public void testIssue2886CypherFormatAddStructure() {
        final Map<String, Object> config = map("cypherFormat", "addStructure");
        issue2886Common(
                actual -> {
                    String[] lines = actual.split("[\r\n]+");
                    assertEquals(8, lines.length);
                    check2886UpdateNodeStructure(lines[0], lines[1], lines[2], lines[3], "MERGE", "ON CREATE SET");
                    check2886UpdateRelStructure(lines[4], lines[5], lines[6], lines[7], "CREATE");
                    check2886Ids(actual);
                },
                config,
                true);
    }

    @Test
    public void testIssue2886CypherFormatUpdateStructure() {
        final Map<String, Object> config = map("cypherFormat", "updateStructure");
        issue2886Common(
                actual -> {
                    String[] lines = actual.split("[\r\n]+");
                    assertEquals(4, lines.length);
                    check2886UpdateRelStructure(lines[0], lines[1], lines[2], lines[3], "MERGE");
                },
                config,
                false);
    }

    @Test
    public void testIssue2886CypherFormatUpdateAll() {
        final Map<String, Object> config = map("cypherFormat", "updateAll");
        issue2886Common(actual -> check2886FullStructure(actual, "updateAll"), config, true);
    }

    private Map<String, Object> withoutOptimization(Map<String, Object> map) {
        map.put("useOptimizations", map("type", ExportConfig.OptimizationType.NONE.name()));
        return map;
    }

    private void issue2886Common(Consumer<String> checkMethod, Map<String, Object> config, boolean isRountrip) {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        final String startOne = "First";
        final long relOne = 1L;
        final String startTwo = "Second";
        final long relTwo = 2L;
        db.executeTransactionally(
                "CREATE (:Person {name: $name})-[:WORKS_FOR {id: $id}]->(:Project)",
                map("name", startOne, "id", relOne));
        db.executeTransactionally(
                "CREATE (:Person {name: $name})-[:WORKS_FOR {id: $id}]->(:Project)",
                map("name", startTwo, "id", relTwo));

        final Map<String, Object> conf = map("format", "plain");
        conf.putAll(config);
        final String cypherStatements =
                db.executeTransactionally("CALL apoc.export.cypher.all(null, $config)", map("config", conf), r ->
                        (String) r.next().get("cypherStatements"));

        beforeAfterIssue2886();

        checkMethod.accept(cypherStatements);

        if (!isRountrip) {
            return;
        }
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        db.executeTransactionally("CALL apoc.schema.assert({}, {})");

        for (String i : cypherStatements.split(";\n")) {
            db.executeTransactionally(i);
        }

        beforeAfterIssue2886();
    }

    private void beforeAfterIssue2886() {
        TestUtil.testResult(db, "MATCH (start:Person)-[rel:WORKS_FOR]->(end:Project) RETURN rel ORDER BY rel.id", r -> {
            final ResourceIterator<Relationship> rels = r.columnAs("rel");
            Relationship rel = rels.next();
            assertEquals("First", rel.getStartNode().getProperty("name"));
            assertEquals(1L, rel.getProperty("id"));
            rel = rels.next();
            assertEquals("Second", rel.getStartNode().getProperty("name"));
            assertEquals(2L, rel.getProperty("id"));
            assertFalse(rels.hasNext());
        });
    }

    private void check2886Cleanup(String line1, String line2) {
        assertEquals(
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;",
                line1);
        assertEquals("DROP CONSTRAINT UNIQUE_IMPORT_NAME;", line2);
    }

    private void check2886Schema(List<String> lines) {
        assertEquals(5, lines.size());
        assertTrue(lines.contains("CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);"));
        assertTrue(lines.contains("CREATE RANGE INDEX FOR (n:Foo) ON (n.name);"));
        assertTrue(lines.contains("CREATE CONSTRAINT uniqueConstraint FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;"));
        assertTrue(lines.contains(
                "CREATE CONSTRAINT uniqueConstraintComposite FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;"));
        assertTrue(
                lines.contains(
                        "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;"));
    }

    private List<String> extractIds(String s, String idName) {
        List<String> ids = new ArrayList<>();
        Pattern idPattern = Pattern.compile(String.format("%s:\\d+", idName));
        Matcher idMatcher = idPattern.matcher(s);
        while (idMatcher.find()) {
            ids.add(String.valueOf(idMatcher.group()));
        }
        return ids;
    }

    private void check2886Ids(String actual) {
        // Check that the non-deterministic ids are sensible
        List<String> ids = extractIds(actual, "_id");
        assertEquals(8, ids.size());
        List<String> endIds = ids.subList(0, 2);
        List<String> startIds = ids.subList(2, 4);
        assertTrue(startIds.contains(ids.get(4))
                && startIds.contains(ids.get(6))
                && !Objects.equals(ids.get(4), ids.get(6)));
        assertTrue(
                endIds.contains(ids.get(5)) && endIds.contains(ids.get(7)) && !Objects.equals(ids.get(5), ids.get(7)));
    }

    private void check2886IdsNoOptimization(String actual) {
        // Check that the non-deterministic ids are sensible
        List<String> ids = extractIds(actual, "`UNIQUE IMPORT ID`");
        assertEquals(8, ids.size());
        List<String> startIds = new ArrayList<>();
        startIds.add(ids.get(0));
        startIds.add(ids.get(2));
        List<String> endIds = new ArrayList<>();
        endIds.add(ids.get(1));
        endIds.add(ids.get(3));
        assertTrue(startIds.contains(ids.get(4))
                && startIds.contains(ids.get(6))
                && !Objects.equals(ids.get(4), ids.get(6)));
        assertTrue(
                endIds.contains(ids.get(5)) && endIds.contains(ids.get(7)) && !Objects.equals(ids.get(5), ids.get(7)));
    }

    private void check2886UpdateNodeStructure(
            String line1, String line2, String line3, String line4, String createOrMerge, String setCommand) {
        assertTrue(regexMatch("UNWIND [{_id:\\d, properties:{}}, {_id:\\d, properties:{}}] AS row", line1));
        assertEquals(
                String.format(
                        "%s (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) %s n += row.properties SET n:Project;",
                        createOrMerge, setCommand),
                line2);

        assertTrue(regexMatch(
                        "UNWIND [{_id:\\d, properties:{name:\"First\"}}, {_id:\\d, properties:{name:\"Second\"}}] AS row",
                        line3)
                || regexMatch(
                        "UNWIND [{_id:\\d, properties:{name:\"Second\"}}, {_id:\\d, properties:{name:\"First\"}}] AS row",
                        line3));
        assertEquals(
                String.format(
                        "%s (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) %s n += row.properties SET n:Person;",
                        createOrMerge, setCommand),
                line4);
    }

    private void check2886UpdateNodeStructureNoOptimization(
            String line1, String line2, String line3, String line4, String cypherFormat) {
        switch (cypherFormat) {
            case "create" -> {
                assertTrue(regexMatch(
                        "CREATE (:Person:`UNIQUE IMPORT LABEL` {name:\"First\", `UNIQUE IMPORT ID`:\\d});", line1));
                assertTrue(regexMatch("CREATE (:Project:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:\\d});", line2));
                assertTrue(regexMatch(
                        "CREATE (:Person:`UNIQUE IMPORT LABEL` {name:\"Second\", `UNIQUE IMPORT ID`:\\d});", line3));
                assertTrue(regexMatch("CREATE (:Project:`UNIQUE IMPORT LABEL` {`UNIQUE IMPORT ID`:\\d});", line4));
            }
            case "updateAll" -> {
                assertTrue(regexMatch(
                        "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) SET n.name=\"First\", n:Person;",
                        line1));
                assertTrue(regexMatch("MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) SET n:Project;", line2));
                assertTrue(regexMatch(
                        "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) SET n.name=\"Second\", n:Person;",
                        line3));
                assertTrue(regexMatch("MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) SET n:Project;", line4));
            }
            case "addStructure" -> {
                assertTrue(regexMatch(
                        "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) ON CREATE SET n.name=\"First\", n:Person;",
                        line1));
                assertTrue(regexMatch(
                        "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) ON CREATE SET n:Project;", line2));
                assertTrue(regexMatch(
                        "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) ON CREATE SET n.name=\"Second\", n:Person;",
                        line3));
                assertTrue(regexMatch(
                        "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) ON CREATE SET n:Project;", line4));
            }
            default -> Assert.fail(String.format("Unexpected cypher format %s", cypherFormat));
        }
    }

    private void check2886UpdateRelStructure(
            String line1, String line2, String line3, String line4, String createOrMerge) {
        assertTrue(regexMatch(
                        "UNWIND [{start: {_id:\\d}, end: {_id:\\d}, properties:{id:1}}, {start: {_id:\\d}, end: {_id:\\d}, properties:{id:2}}] AS row",
                        line1)
                || regexMatch(
                        "UNWIND [{start: {_id:\\d}, end: {_id:\\d}, properties:{id:2}}, {start: {_id:\\d}, end: {_id:\\d}, properties:{id:1}}] AS row",
                        line1));

        assertEquals("MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})", line2);
        assertEquals("MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})", line3);
        assertTrue(regexMatch(
                        String.format("%s (start)-[r:WORKS_FOR]->(end) SET r += row.properties;", createOrMerge), line4)
                || regexMatch(
                        String.format("%s (start)-[r:WORKS_FOR]->(end)  SET r += row.properties;", createOrMerge),
                        line4));
    }

    private void check2886UpdateRelStructureNoOptimization(String line1, String line2, String cypherFormat) {
        switch (cypherFormat) {
            case "create", "addStructure" -> {
                assertTrue(regexMatch(
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) CREATE (n1)-[r:WORKS_FOR {id:1}]->(n2);",
                        line1));
                assertTrue(regexMatch(
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) CREATE (n1)-[r:WORKS_FOR {id:2}]->(n2);",
                        line2));
            }
            case "updateAll" -> {
                assertTrue(regexMatch(
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) MERGE (n1)-[r:WORKS_FOR]->(n2) SET r.id=1;",
                        line1));
                assertTrue(regexMatch(
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) MERGE (n1)-[r:WORKS_FOR]->(n2) SET r.id=2;",
                        line2));
            }
            case "updateStructure" -> {
                assertTrue(regexMatch(
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) MERGE (n1)-[r:WORKS_FOR]->(n2) ON CREATE SET r.id=1;",
                        line1));
                assertTrue(regexMatch(
                        "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:\\d}) MERGE (n1)-[r:WORKS_FOR]->(n2) ON CREATE SET r.id=2;",
                        line2));
            }
            default -> Assert.fail(String.format("Unexpected cypher format %s", cypherFormat));
        }
    }

    private void check2886FullStructure(String actual, String cypherFormat) {
        String createOrMerge = "";
        switch (cypherFormat) {
            case "updateAll" -> {
                createOrMerge = "MERGE";
            }
            case "create" -> {
                createOrMerge = "CREATE";
            }
            default -> Assert.fail(String.format("Unexpected cypher format %s", cypherFormat));
        }

        String[] lines = actual.split("[\r\n]+");
        assertEquals(15, lines.length);
        check2886Schema(Arrays.stream(lines).toList().subList(0, 5));
        check2886UpdateNodeStructure(lines[5], lines[6], lines[7], lines[8], createOrMerge, "SET");
        check2886UpdateRelStructure(lines[9], lines[10], lines[11], lines[12], createOrMerge);
        check2886Cleanup(lines[13], lines[14]);
        check2886Ids(actual);
    }

    private void check2886FullStructureNonOptimized(String actual, String cypherFormat) {

        String[] lines = actual.split("[\r\n]+");
        assertEquals(13, lines.length);

        check2886UpdateNodeStructureNoOptimization(lines[0], lines[1], lines[2], lines[3], cypherFormat);
        check2886Schema(Arrays.stream(lines).toList().subList(4, 9));
        check2886UpdateRelStructureNoOptimization(lines[9], lines[10], cypherFormat);
        check2886Cleanup(lines[11], lines[12]);
        check2886IdsNoOptimization(actual);
    }

    private boolean regexMatch(String regex, String actual) {
        Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[^A-Za-z0-9\\\\d\\n\\r\\s]");
        String escapedRegex = SPECIAL_REGEX_CHARS.matcher(regex).replaceAll("\\\\$0");
        Pattern pattern = Pattern.compile(escapedRegex);
        Matcher matcher = pattern.matcher(actual);
        return matcher.matches();
    }

    private void relIndexTestCommon(String fileName, String expectedSchema, Map<String, Object> config) {
        Map<String, Object> exportConfig = map("separateFiles", true, "format", "neo4j-shell");
        exportConfig.putAll(config);
        TestUtil.testCall(
                db,
                "CALL apoc.export.cypher.all($file, $exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("relIndex.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("relIndex.relationships.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("relIndex.cleanup.cypher"));
        assertEquals(expectedSchema, readFile("relIndex.schema.cypher"));
    }

    private void assertResultsOptimized(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    private void assertResultsOdd(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    public static class ExportCypherResults {
        static final String EXPECTED_ISOLATED_NODE =
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:12, `UNIQUE IMPORT ID`:2});\n";
        static final String EXPECTED_BAR_END_NODE = "CREATE (:Bar {age:42, name:\"bar\"});%n";
        static final String EXPECTED_BEGIN_AND_FOO = "BEGIN%n"
                + "CREATE (:Foo:`UNIQUE IMPORT LABEL` {born:date('2018-10-31'), name:\"foo\", `UNIQUE IMPORT ID`:0});%n";

        public static final String EXPECTED_NODES =
                String.format(EXPECTED_BEGIN_AND_FOO + EXPECTED_BAR_END_NODE + EXPECTED_ISOLATED_NODE + "COMMIT%n");

        private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n"
                + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.name=\"foo\", n.born=date('2018-10-31'), n:Foo;%n"
                + "MERGE (n:Bar{name:\"bar\"}) SET n.age=42;%n"
                + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.age=12, n:Bar;%n"
                + "COMMIT%n");

        public static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
                EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        public static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" + "COMMIT%n");

        static final String EXPECTED_REL_ONLY =
                """
                BEGIN
                MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:KNOWS {since:2016}]->(n2);
                COMMIT
                """;

        private static final String EXPECTED_UNIQUE_IMPORT_CONSTRAINT_AND_AWAIT =
                "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                        + "COMMIT%n"
                        + "SCHEMA AWAIT%n";

        private static final String EXPECTED_CONSTRAINTS_AND_AWAIT =
                "CREATE CONSTRAINT uniqueConstraint FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT uniqueConstraintComposite FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;%n"
                        + EXPECTED_UNIQUE_IMPORT_CONSTRAINT_AND_AWAIT;

        private static final String EXPECTED_IDX_FOO = "CREATE RANGE INDEX FOR (n:Foo) ON (n.name);%n";

        public static final String EXPECTED_SCHEMA =
                String.format("BEGIN%n" + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + EXPECTED_IDX_FOO
                        + EXPECTED_CONSTRAINTS_AND_AWAIT);

        static final String EXPECTED_SCHEMA_ONLY_START =
                String.format("BEGIN%n" + EXPECTED_IDX_FOO + EXPECTED_UNIQUE_IMPORT_CONSTRAINT_AND_AWAIT);

        static final String EXPECTED_SCHEMA_WITH_NAMES =
                String.format("BEGIN%n" + "CREATE RANGE INDEX barIndex FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX fooIndex FOR (n:Foo) ON (n.name);%n"
                        + EXPECTED_CONSTRAINTS_AND_AWAIT);

        public static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" + "COMMIT%n" + "SCHEMA AWAIT%n");

        public static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndexes(300);%n");

        public static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n"
                + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) CREATE (n1)-[r:KNOWS {since:2016}]->(n2);%n"
                + "COMMIT%n");

        private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n"
                + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) MERGE (n1)-[r:KNOWS]->(n2) SET r.since=2016;%n"
                + "COMMIT%n");

        public static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
                EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        public static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n"
                + "COMMIT%n");

        public static final String EXPECTED_CLEAN_UP_EMPTY =
                String.format("BEGIN%n" + "COMMIT%n" + "BEGIN%n" + "COMMIT%n");

        public static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL =
                String.format("BEGIN%n" + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX FOR (n:Foo) ON (n.name);%n"
                        + "CREATE CONSTRAINT uniqueConstraint FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT uniqueConstraintComposite FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;%n"
                        + "COMMIT%n"
                        + "SCHEMA AWAIT%n");

        static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL_WITH_NAMES =
                """
                BEGIN
                CREATE RANGE INDEX barIndex FOR (n:Bar) ON (n.first_name, n.last_name);
                CREATE RANGE INDEX fooIndex FOR (n:Foo) ON (n.name);
                CREATE CONSTRAINT uniqueConstraint FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;
                CREATE CONSTRAINT uniqueConstraintComposite FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;
                COMMIT
                SCHEMA AWAIT
                """;

        public static final String EXPECTED_CYPHER_POINT = String.format("BEGIN%n"
                + "CREATE (:Test:`UNIQUE IMPORT LABEL` {name:\"foo\", place2d:point({x: 2.3, y: 4.5, crs: 'cartesian'}), place3d1:point({x: 2.3, y: 4.5, z: 1.2, crs: 'cartesian-3d'}), `UNIQUE IMPORT ID`:3});%n"
                + "CREATE (:Bar:`UNIQUE IMPORT LABEL` {place3d:point({x: 12.78, y: 56.7, z: 100.0, crs: 'wgs-84-3d'}), `UNIQUE IMPORT ID`:4});%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                + EXPECTED_CONSTRAINTS_AND_AWAIT
                + "BEGIN%n"
                + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {place2d:point({x: 56.7, y: 12.78, crs: 'wgs-84'})}]->(n2);%n"
                + "COMMIT%n"
                + EXPECTED_CLEAN_UP);

        public static final String EXPECTED_CYPHER_DATE = String.format("BEGIN%n"
                + "CREATE (:Test:`UNIQUE IMPORT LABEL` {date:date('2018-10-30'), datetime:datetime('2018-10-30T12:50:35.556+01:00'), localTime:localdatetime('2018-10-30T19:32:24'), name:\"foo\", `UNIQUE IMPORT ID`:3});%n"
                + "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556Z'), `UNIQUE IMPORT ID`:4});%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                + EXPECTED_CONSTRAINTS_AND_AWAIT
                + "BEGIN%n"
                + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {date:date('2018-10-30')}]->(n2);%n"
                + "COMMIT%n"
                + EXPECTED_CLEAN_UP);

        public static final String EXPECTED_CYPHER_TIME = String.format("BEGIN%n"
                + "CREATE (:Test:`UNIQUE IMPORT LABEL` {local:localtime('12:50:35.556'), name:\"foo\", t:time('12:50:35.556+01:00'), `UNIQUE IMPORT ID`:3});%n"
                + "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556+01:00'), `UNIQUE IMPORT ID`:4});%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                + EXPECTED_CONSTRAINTS_AND_AWAIT
                + "BEGIN%n"
                + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {t:time('12:50:35.556+01:00')}]->(n2);%n"
                + "COMMIT%n"
                + EXPECTED_CLEAN_UP);

        public static final String EXPECTED_CYPHER_DURATION = String.format("BEGIN%n"
                + "CREATE (:Test:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), name:\"foo\", `UNIQUE IMPORT ID`:3});%n"
                + "CREATE (:Bar:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), `UNIQUE IMPORT ID`:4});%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                + EXPECTED_CONSTRAINTS_AND_AWAIT
                + "BEGIN%n"
                + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {duration:duration('P5M1DT12H')}]->(n2);%n"
                + "COMMIT%n"
                + EXPECTED_CLEAN_UP);

        public static final String EXPECTED_CYPHER_LABELS_ASCENDEND = String.format("BEGIN%n"
                + "CREATE (:User:User0:User1:User12:`UNIQUE IMPORT LABEL` {name:\"Alan\", `UNIQUE IMPORT ID`:3});%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                + "COMMIT%n"
                + "SCHEMA AWAIT%n"
                + EXPECTED_CLEAN_UP);

        static final String EXPECTED_SCHEMA_WITH_RELS_OPTIMIZED =
                String.format("BEGIN%n" + "CREATE RANGE INDEX FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n"
                        + "CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX FOR (n:Foo) ON (n.name);%n"
                        + EXPECTED_CONSTRAINTS_AND_AWAIT);

        static final String EXPECTED_SCHEMA_WITH_RELS_AND_NAME_OPTIMIZED =
                String.format("BEGIN%n" + "CREATE RANGE INDEX barIndex FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX fooIndex FOR (n:Foo) ON (n.name);%n"
                        + "CREATE RANGE INDEX rel_index_name FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n"
                        + EXPECTED_CONSTRAINTS_AND_AWAIT);

        static final String EXPECTED_SCHEMA_WITH_RELS_AND_IF_NOT_EXISTS = String.format(
                "BEGIN%n" + "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n"
                        + "CREATE RANGE INDEX IF NOT EXISTS FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX IF NOT EXISTS FOR (n:Foo) ON (n.name);%n"
                        + "CREATE CONSTRAINT uniqueConstraint IF NOT EXISTS FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT uniqueConstraintComposite IF NOT EXISTS FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME IF NOT EXISTS FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                        + "COMMIT%n"
                        + "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED_WITH_IF_NOT_EXISTS = String.format(
                "BEGIN%n" + "CREATE RANGE INDEX IF NOT EXISTS FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX IF NOT EXISTS FOR (n:Foo) ON (n.name);%n"
                        + "CREATE CONSTRAINT uniqueConstraint IF NOT EXISTS FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT uniqueConstraintComposite IF NOT EXISTS FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME IF NOT EXISTS FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                        + "COMMIT%n"
                        + "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED_WITH_RELS_IF_NOT_EXISTS_AND_NAME = String.format(
                "BEGIN%n" + "CREATE RANGE INDEX barIndex IF NOT EXISTS FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX fooIndex IF NOT EXISTS FOR (n:Foo) ON (n.name);%n"
                        + "CREATE RANGE INDEX rel_index_name IF NOT EXISTS FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n"
                        + "CREATE CONSTRAINT uniqueConstraint IF NOT EXISTS FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT uniqueConstraintComposite IF NOT EXISTS FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME IF NOT EXISTS FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                        + "COMMIT%n"
                        + "SCHEMA AWAIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE =
                String.format("BEGIN%n" + "UNWIND [{_id:3, properties:{age:17}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n"
                        + "UNWIND [{_id:2, properties:{age:12}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n"
                        + "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                        + "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n"
                        + "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n"
                        + "UNWIND [{_id:6, properties:{age:99}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n"
                        + "COMMIT%n");

        public static final String EXPECTED_NODES_OPTIMIZED =
                String.format("BEGIN%n" + "UNWIND [{_id:3, properties:{age:17}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n"
                        + "UNWIND [{_id:2, properties:{age:12}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n"
                        + "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                        + "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n"
                        + "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n"
                        + "UNWIND [{_id:6, properties:{age:99}}] AS row%n"
                        + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n"
                        + "COMMIT%n");

        // The order in UNWIND is non-deterministic so we need to check for all 4 combinations of orders
        static final String EXPECTED_QUERY_NODES_OPTIMIZED = String.format("BEGIN%n"
                + "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                + "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n"
                + "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n"
                + "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED2 = String.format("BEGIN%n"
                + "UNWIND [{_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                + "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n"
                + "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n"
                + "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED3 = String.format("BEGIN%n"
                + "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                + "UNWIND [{name:\"bar2\", properties:{age:44}}, {name:\"bar\", properties:{age:42}}] AS row%n"
                + "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n"
                + "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED4 = String.format("BEGIN%n"
                + "UNWIND [{_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                + "UNWIND [{name:\"bar2\", properties:{age:44}}, {name:\"bar\", properties:{age:42}}] AS row%n"
                + "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n"
                + "COMMIT%n");

        public static final String EXPECTED_RELATIONSHIPS_OPTIMIZED = String.format("BEGIN%n"
                + "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n"
                + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                + "MATCH (end:Bar{name: row.end.name})%n"
                + "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n"
                + "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_ODD = String.format(
                "BEGIN%n" + "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}] AS row%n"
                        + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                        + "MATCH (end:Bar{name: row.end.name})%n"
                        + "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n"
                        + "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_PARAMS_ODD = String.format(
                "BEGIN%n" + ":param rows => [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}]%n"
                        + "UNWIND $rows AS row%n"
                        + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                        + "MATCH (end:Bar{name: row.end.name})%n"
                        + "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n"
                        + "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED_BATCH = String.format("BEGIN%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + "COMMIT%n"
                + "BEGIN%n"
                + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n"
                + "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND =
                """
                BEGIN
                UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:"foo"}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                UNWIND [{name:"bar", properties:{age:42}}] AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                COMMIT
                BEGIN
                UNWIND [{_id:3, properties:{age:17}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;
                UNWIND [{_id:2, properties:{age:12}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;
                COMMIT
                BEGIN
                UNWIND [{_id:4, properties:{born:date('2017-09-29'), name:"foo2"}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                UNWIND [{name:"bar2", properties:{age:44}}] AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                COMMIT
                BEGIN
                UNWIND [{_id:6, properties:{age:99}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;
                COMMIT
                """;

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD =
                """
                :begin
                UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:"foo"}}, {_id:1, properties:{born:date('2017-09-29'), name:"foo2"}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                :commit
                :begin
                UNWIND [{_id:2, properties:{born:date('2016-03-12'), name:"foo3"}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                UNWIND [{name:"bar", properties:{age:42}}] AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :commit
                :begin
                UNWIND [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}] AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;
                :commit
                :begin
                UNWIND [{name:"bar2", properties:{age:44}}] AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :commit
                """;

        static final String EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD =
                """
                :begin
                :param rows => [{_id:0, properties:{born:date('2018-10-31'), name:"foo"}}, {_id:1, properties:{born:date('2017-09-29'), name:"foo2"}}]
                UNWIND $rows AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                :commit
                :begin
                :param rows => [{_id:2, properties:{born:date('2016-03-12'), name:"foo3"}}]
                UNWIND $rows AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                :param rows => [{name:"bar", properties:{age:42}}]
                UNWIND $rows AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :commit
                :begin
                :param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]
                UNWIND $rows AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;
                :commit
                :begin
                :param rows => [{name:"bar2", properties:{age:44}}]
                UNWIND $rows AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :commit
                """;

        public static final String EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND =
                String.format("UNWIND [{_id:3, properties:{age:17}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar;%n"
                        + "UNWIND [{_id:2, properties:{age:12}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar:Person;%n"
                        + "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Foo;%n"
                        + "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n"
                        + "MERGE (n:Bar{name: row.name}) ON CREATE SET n += row.properties;%n"
                        + "UNWIND [{_id:6, properties:{age:99}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties;%n"
                        + "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n"
                        + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                        + "MATCH (end:Bar{name: row.end.name})%n"
                        + "CREATE (start)-[r:KNOWS]->(end)  SET r += row.properties;%n");

        public static final String EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND = String.format(
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n"
                        + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                        + "MATCH (end:Bar{name: row.end.name})%n"
                        + "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");

        public static final String EXPECTED_UPDATE_ALL_UNWIND =
                String.format("CREATE RANGE INDEX FOR (n:Bar) ON (n.first_name, n.last_name);%n"
                        + "CREATE RANGE INDEX FOR (n:Foo) ON (n.name);%n"
                        + "CREATE CONSTRAINT uniqueConstraint FOR (node:Bar) REQUIRE (node.name) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT uniqueConstraintComposite FOR (node:Bar) REQUIRE (node.name, node.age) IS UNIQUE;%n"
                        + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                        + "UNWIND [{_id:3, properties:{age:17}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n"
                        + "UNWIND [{_id:2, properties:{age:12}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n"
                        + "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n"
                        + "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n"
                        + "MERGE (n:Bar{name: row.name}) SET n += row.properties;%n"
                        + "UNWIND [{_id:6, properties:{age:99}}] AS row%n"
                        + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n"
                        + EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND
                        + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                        + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n");

        public static final String EXPECTED_NEO4J_OPTIMIZED =
                EXPECTED_SCHEMA + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + EXPECTED_CLEAN_UP;

        public static final String EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA
                + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE
                + EXPECTED_RELATIONSHIPS_OPTIMIZED
                + EXPECTED_CLEAN_UP;

        static final String EXPECTED_QUERY_NODES =
                EXPECTED_SCHEMA + EXPECTED_QUERY_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + EXPECTED_CLEAN_UP;
        static final String EXPECTED_QUERY_NODES2 = EXPECTED_SCHEMA
                + EXPECTED_QUERY_NODES_OPTIMIZED2
                + EXPECTED_RELATIONSHIPS_OPTIMIZED
                + EXPECTED_CLEAN_UP;
        static final String EXPECTED_QUERY_NODES3 = EXPECTED_SCHEMA
                + EXPECTED_QUERY_NODES_OPTIMIZED3
                + EXPECTED_RELATIONSHIPS_OPTIMIZED
                + EXPECTED_CLEAN_UP;
        static final String EXPECTED_QUERY_NODES4 = EXPECTED_SCHEMA
                + EXPECTED_QUERY_NODES_OPTIMIZED4
                + EXPECTED_RELATIONSHIPS_OPTIMIZED
                + EXPECTED_CLEAN_UP;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND = EXPECTED_SCHEMA
                + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND
                + EXPECTED_RELATIONSHIPS_OPTIMIZED
                + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD = EXPECTED_SCHEMA
                + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD
                + EXPECTED_RELATIONSHIPS_ODD
                + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_SCHEMA
                + EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD
                + EXPECTED_RELATIONSHIPS_PARAMS_ODD
                + DROP_UNIQUE_OPTIMIZED_BATCH;

        public static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND =
                convertToCypherShellFormat(EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND);

        public static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD =
                convertToCypherShellFormat(EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD);

        public static final String EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD =
                convertToCypherShellFormat(EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD);

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED = convertToCypherShellFormat(EXPECTED_QUERY_NODES);

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2 = convertToCypherShellFormat(EXPECTED_QUERY_NODES2);

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED3 = convertToCypherShellFormat(EXPECTED_QUERY_NODES3);

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED4 = convertToCypherShellFormat(EXPECTED_QUERY_NODES4);

        public static final String EXPECTED_CYPHER_SHELL_OPTIMIZED =
                convertToCypherShellFormat(EXPECTED_NEO4J_OPTIMIZED);

        public static final String EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE =
                convertToCypherShellFormat(EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE);

        public static final String EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE = EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE
                .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin())
                .replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        public static final String EXPECTED_NEO4J_SHELL =
                EXPECTED_NODES + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

        public static final String EXPECTED_CYPHER_SHELL = convertToCypherShellFormat(EXPECTED_NEO4J_SHELL);

        public static final String EXPECTED_PLAIN = EXPECTED_NEO4J_SHELL
                .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin())
                .replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        public static final String EXPECTED_NEO4J_MERGE =
                EXPECTED_NODES_MERGE + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS_MERGE + EXPECTED_CLEAN_UP;

        public static final String EXPECTED_ONLY_SCHEMA_CYPHER_SHELL =
                convertToCypherShellFormat(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL);

        public static final List<String> EXPECTED_CONSTRAINTS = List.of(
                "CREATE CONSTRAINT SingleUnique FOR (node:Label) REQUIRE (node.prop) IS UNIQUE;",
                "CREATE CONSTRAINT SingleUniqueRel FOR ()-[rel:TYPE]-() REQUIRE (rel.prop) IS UNIQUE;",
                "CREATE CONSTRAINT CompositeUnique FOR (node:Label) REQUIRE (node.prop1, node.prop2) IS UNIQUE;",
                "CREATE CONSTRAINT CompositeUniqueRel FOR ()-[rel:TYPE]-() REQUIRE (rel.prop1, rel.prop2) IS UNIQUE;",
                "CREATE CONSTRAINT SingleExists FOR (node:Label2) REQUIRE (node.prop) IS NOT NULL;",
                "CREATE CONSTRAINT SingleExistsRel FOR ()-[rel:TYPE2]-() REQUIRE (rel.prop) IS NOT NULL;",
                "CREATE CONSTRAINT SingleNodeKey FOR (node:Label3) REQUIRE (node.prop) IS NODE KEY;",
                "CREATE CONSTRAINT PersonRequiresNamesConstraint FOR (node:Person) REQUIRE (node.name, node.surname) IS NODE KEY;");

        public static final String EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT =
                String.format("COMMIT%n" + "SCHEMA AWAIT%n"
                        + "BEGIN%n"
                        + "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n"
                        + "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n"
                        + "COMMIT%n"
                        + "BEGIN%n"
                        + "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n"
                        + "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n"
                        + "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n"
                        + "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n"
                        + "COMMIT%n");

        public static final String EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT =
                convertToCypherShellFormat(EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT);

        public static final String EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT = String.format(
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n"
                        + "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n"
                        + "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n"
                        + "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n"
                        + "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n"
                        + "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");

        static final String EXPECTED_NODES_PARAMS_ODD =
                """
                :begin
                :param rows => [{_id:0, properties:{born:date('2018-10-31'), name:"foo"}}, {_id:1, properties:{born:date('2017-09-29'), name:"foo2"}}]
                UNWIND $rows AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                :param rows => [{_id:2, properties:{born:date('2016-03-12'), name:"foo3"}}]
                UNWIND $rows AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;
                :commit
                :begin
                :param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]
                UNWIND $rows AS row
                CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;
                :param rows => [{name:"bar", properties:{age:42}}]
                UNWIND $rows AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :commit
                :begin
                :param rows => [{name:"bar2", properties:{age:44}}, {name:"bar3", properties:{age:35}}]
                UNWIND $rows AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :param rows => [{name:"bar4", properties:{age:36}}]
                UNWIND $rows AS row
                CREATE (n:Bar{name: row.name}) SET n += row.properties;
                :commit
                """;

        static final String EXPECTED_DROP_PARAMS_ODD = String.format(
                ":begin%n"
                        + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %1$d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                        + ":commit%n"
                        + ":begin%n"
                        + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %1$d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                        + ":commit%n"
                        + ":begin%n"
                        + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n"
                        + ":commit%n",
                3);

        static final String EXPECTED_WITHOUT_END_NODE = String.format(EXPECTED_BEGIN_AND_FOO + "COMMIT%n")
                + EXPECTED_SCHEMA_ONLY_START
                + EXPECTED_REL_ONLY
                + EXPECTED_CLEAN_UP;

        public static final String EXPECTED_QUERY_PARAMS_ODD = (EXPECTED_SCHEMA
                        + EXPECTED_NODES_PARAMS_ODD
                        + EXPECTED_RELATIONSHIPS_PARAMS_ODD
                        + EXPECTED_DROP_PARAMS_ODD)
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        public static String convertToCypherShellFormat(String input) {
            return input.replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                    .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                    .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                    .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());
        }
    }
}
