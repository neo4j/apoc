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
package apoc.export.json;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.json.ImportJsonConfig.WILDCARD_PROPS;
import static apoc.export.json.JsonImporter.MISSING_CONSTRAINT_ERROR_MSG;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.TransactionTestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import apoc.util.collection.Iterables;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import junit.framework.TestCase;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

@EnterpriseDbmsExtension(configurationCallback = "configure")
public class ImportJsonTest {

    private static final long NODES_BIG_JSON = 16L;
    private static final long RELS_BIG_JSON = 4L;
    private static File directory = new File("src/test/resources/exportJSON");

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) throws IOException {
        builder.setConfig(
                GraphDatabaseSettings.load_csv_file_url_root,
                directory.getCanonicalFile().toPath());
    }

    public ImportJsonTest() throws IOException {}

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, ImportJson.class, Utils.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
    }

    @Test
    public void shouldImportAllJsonWithoutImportId() {
        shouldImportAllCommon(map("cleanup", true), 8, 0L);
    }

    @Test
    public void shouldImportAllJson() {
        shouldImportAllCommon(Collections.emptyMap(), 9, 1L);
    }

    private void shouldImportAllCommon(Map<String, Object> config, int expectedPropSize, long relCount) {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE n.neo4jImportId IS UNIQUE");

        // given
        String filename = "all.json";

        // when
        TestUtil.testCall(
                db,
                "CALL apoc.import.json($file, $config)",
                map("file", filename, "config", config),
                (r) -> assertionsAllJsonProgressInfo(r, false));

        assertionsAllJsonDbResult(expectedPropSize, relCount);
    }

    @Test
    public void testImportOfPointValues() {
        db.executeTransactionally(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Earthquake) REQUIRE n.neo4jImportId IS UNIQUE");

        String filename = "importPointValues.json";

        TestUtil.testCall(
                db,
                "CALL apoc.import.json($file, {nodePropertyMappings: { Earthquake: { coordinates: 'POINT' }}})",
                map("file", filename),
                (r) -> {
                    // then
                    assertEquals(filename, r.get("file"));
                    assertEquals("file", r.get("source"));
                    assertEquals("json", r.get("format"));
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                    assertEquals(12L, r.get("properties"));
                    assertEquals(4L, r.get("rows"));
                    assertEquals(true, r.get("done"));
                });

        try (Transaction tx = db.beginTx()) {
            var result = tx.execute("MATCH (n:Earthquake) RETURN n ORDER BY n.orderID")
                    .<Node>columnAs("n");
            Node node = result.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -122.7955, 38.8232, 3),
                    node.getProperty("coordinates"));
            node = result.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, -122.7955, 38.8232),
                    node.getProperty("coordinates"));
            node = result.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.CARTESIAN, -122.7955, 38.8232),
                    node.getProperty("coordinates"));
            node = result.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, -122.7955, 38.8232, 3),
                    node.getProperty("coordinates"));
            assertFalse(result.hasNext());
        }
    }

    @Test
    public void testInvalidPointValues() {
        db.executeTransactionally(
                "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Earthquake) REQUIRE n.neo4jImportId IS UNIQUE");

        String filename = "invalidPointValues.json";
        var e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.testCall(
                        db,
                        "CALL apoc.import.json($file, {nodePropertyMappings: { Earthquake: { coordinates: 'POINT' }}})",
                        map("file", filename),
                        row -> {}));

        String expectedMsg = "Cannot convert the map with keys: [lat, long, height] to a POINT value.";
        Assertions.assertThat(e.getMessage()).contains(expectedMsg);
    }

    @Test
    public void shouldImportAllJsonWithPropertyMappings() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE n.neo4jImportId IS UNIQUE");
        // given
        String filename = "all.json";

        // when
        TestUtil.testCall(
                db,
                "CALL apoc.import.json($file, $config)",
                map(
                        "file",
                        filename,
                        "config",
                        map(
                                "nodePropertyMappings",
                                map("User", map("place", "Point", "born", "LocalDateTime")),
                                "relPropertyMappings",
                                map("KNOWS", map("bffSince", "Duration")),
                                "unwindBatchSize",
                                1,
                                "txBatchSize",
                                1)),
                (r) -> {
                    // then
                    assertEquals("all.json", r.get("file"));
                    assertEquals("file", r.get("source"));
                    assertEquals("json", r.get("format"));
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(15L, r.get("properties"));
                    assertEquals(4L, r.get("rows"));
                    assertEquals(true, r.get("done"));
                });

        try (Transaction tx = db.beginTx()) {
            final long countNodes = tx.execute("MATCH (n:User) RETURN count(n) AS count")
                    .<Long>columnAs("count")
                    .next();
            assertEquals(3L, countNodes);

            final long countRels = tx.execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count")
                    .<Long>columnAs("count")
                    .next();
            assertEquals(1L, countRels);

            final Map<String, Object> props = tx.execute("MATCH (n:User {name: 'Adam'}) RETURN n")
                    .<Node>columnAs("n")
                    .next()
                    .getAllProperties();
            assertEquals(7, props.size());
            assertInstanceOf(PointValue.class, props.get("place"));
            PointValue point = (PointValue) props.get("place");
            final PointValue pointValue = Values.pointValue(CoordinateReferenceSystem.WGS_84, 33.46789D, 13.1D);
            assertTrue(point.equals((Point) pointValue));
            assertInstanceOf(LocalDateTime.class, props.get("born"));

            Relationship rel = tx.execute("MATCH ()-[r:KNOWS]->() RETURN r")
                    .<Relationship>columnAs("r")
                    .next();
            assertInstanceOf(DurationValue.class, rel.getProperty("bffSince"));
            assertEquals("P5M1DT12H", rel.getProperty("bffSince").toString());
        }
    }

    @Test
    public void shouldImportNodesWithoutLabels() throws Exception {
        // given
        String filename = "nodes_without_labels.json";
        Map<String, Object> jsonMap = JsonUtil.OBJECT_MAPPER.readValue(new File(directory, filename), Map.class);
        Map<String, Object> properties = (Map<String, Object>) jsonMap.get("properties");
        List<Double> bbox = (List<Double>) properties.get("bbox");
        final double[] expected = bbox.stream().mapToDouble(Double::doubleValue).toArray();

        // when
        TestUtil.testCall(db, "CALL apoc.import.json($file)", map("file", filename), (r) -> {
            // then
            assertEquals(filename, r.get("file"));
            assertEquals("file", r.get("source"));
            assertEquals("json", r.get("format"));
            assertEquals(1L, r.get("nodes"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(2L, r.get("properties"));
            assertEquals(1L, r.get("rows"));
            assertEquals(true, r.get("done"));
        });

        try (Transaction tx = db.beginTx()) {
            Node node = tx.execute("MATCH (n) WHERE n.neo4jImportId = '5016999' RETURN n")
                    .<Node>columnAs("n")
                    .next();
            assertNotNull(node);
            final double[] actual = (double[]) node.getProperty("bbox");
            assertArrayEquals(expected, actual, 0.05D);
        }
    }

    @Test
    public void shouldTerminateImportWhenTransactionIsTimedOut() {

        createConstraints(List.of("Stream", "User", "Game", "Team", "Language"));

        String filename = "https://devrel-data-science.s3.us-east-2.amazonaws.com/twitch_all.json";

        final String query = "CALL apoc.import.json($file)";

        TransactionTestUtil.checkTerminationGuard(db, query, map("file", filename));
    }

    @Test
    public void shouldImportAllNodesAndRelsWithFilterAll() {
        createConstraints(List.of("FirstLabel", "Stream", "User", "Game", "Team", "Language", "$User", "$Stream"));
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";

        TestUtil.testCall(
                db,
                "CALL apoc.import.json($file, $config)",
                map(
                        "file",
                        filename,
                        "config",
                        map(
                                "nodePropFilter",
                                map(WILDCARD_PROPS, List.of("name")),
                                "relPropFilter",
                                map(WILDCARD_PROPS, List.of("bffSince")))),
                r -> {
                    assertEquals(NODES_BIG_JSON, r.get("nodes"));
                    assertEquals(RELS_BIG_JSON, r.get("relationships"));
                });

        final Consumer<Node> nodeConsumer = node -> assertFalse(node.hasProperty("name"));
        final Consumer<Relationship> relConsumer = rel -> assertFalse(rel.hasProperty("bffSince"));
        assertEntities(NODES_BIG_JSON, RELS_BIG_JSON, nodeConsumer, relConsumer);
    }

    @Test
    public void shouldImportAllNodesAndRelsWithLabelAndRelTypeFilter() {
        createConstraints(List.of("FirstLabel", "Stream", "User", "Game", "Team", "Language", "$User", "$Stream"));
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";

        TestUtil.testCall(
                db,
                "CALL apoc.import.json($file, $config)",
                map(
                        "file",
                        filename,
                        "config",
                        map(
                                "nodePropFilter",
                                map(
                                        "Stream",
                                        List.of("name"),
                                        "$User",
                                        List.of("total_view_count", "url"),
                                        "$Stream",
                                        List.of("name", "id")),
                                "relPropFilter",
                                map("REL_GAME_TO_LANG", List.of("since")))),
                r -> {
                    assertEquals(NODES_BIG_JSON, r.get("nodes"));
                    assertEquals(RELS_BIG_JSON, r.get("relationships"));
                });

        final Consumer<Node> nodeConsumer = node -> {
            if (node.hasLabel(Label.label("Stream"))) {
                final Set<String> actual = Iterables.asSet(node.getPropertyKeys());
                assertEquals(
                        Set.of(
                                "createdAt",
                                "followers",
                                "description",
                                "id",
                                "total_view_count",
                                "url",
                                "neo4jImportId"),
                        actual);
            }
            if (node.hasLabel(Label.label("$User"))) {
                final Set<String> actual = Iterables.asSet(node.getPropertyKeys());
                assertEquals(Set.of("createdAt", "followers", "description", "neo4jImportId"), actual);
            }
        };

        final Consumer<Relationship> relConsumer = rel -> {
            final boolean hasTypeRelGameToLang = rel.getType().name().equals("REL_GAME_TO_LANG");
            assertEquals(!hasTypeRelGameToLang, rel.hasProperty("since"));
            assertTrue(rel.hasProperty("bffSince"));
        };

        assertEntities(NODES_BIG_JSON, RELS_BIG_JSON, nodeConsumer, relConsumer);
    }

    @Test
    public void shouldImportAllNodesAndRels() {
        createConstraints(List.of("FirstLabel", "Stream", "User", "Game", "Team", "Language", "$User", "$Stream"));
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";

        TestUtil.testCall(db, "CALL apoc.import.json($file)", map("file", filename), (r) -> {
            assertEquals(NODES_BIG_JSON, r.get("nodes"));
            assertEquals(RELS_BIG_JSON, r.get("relationships"));
        });

        assertEntities(NODES_BIG_JSON, RELS_BIG_JSON);
    }

    @Test
    public void shouldFailBecauseOfMissingSecondConstraintException() {
        String customId = "customId";
        createConstraints(List.of("FirstLabel", "Stream", "Game", "$User"), customId);
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";

        var e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.testCall(
                        db,
                        "CALL apoc.import.json($file, {importIdName: $importIdName})",
                        map("file", filename, "importIdName", customId),
                        row -> {}));

        String expectedMsg = format(MISSING_CONSTRAINT_ERROR_MSG, "User", customId);
        Assertions.assertThat(e.getMessage()).contains(expectedMsg);

        // check that only 1st node created after constraint exception
        assertEntities(1L, 0L);
    }

    @Test
    public void shouldFailBecauseOfMissingUniquenessConstraintException() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE (n.neo4jImportId, n.name) IS UNIQUE;");
        assertEntities(0L, 0L);

        String filename = "all.json";
        var e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.testCall(db, "CALL apoc.import.json($file, {})", map("file", filename), row -> {}));

        String expectedMsg = format(MISSING_CONSTRAINT_ERROR_MSG, "User", "neo4jImportId");
        Assertions.assertThat(e.getMessage()).contains(expectedMsg);
    }

    @Test
    public void shouldImportAllJsonFromBinary() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE n.neo4jImportId IS UNIQUE");

        TestUtil.testCall(
                db,
                "CALL apoc.import.json($file, $config)",
                map(
                        "config",
                        map(COMPRESSION, CompressionAlgo.DEFLATE.name()),
                        "file",
                        fileToBinary(new File(directory, "all.json"), CompressionAlgo.DEFLATE.name())),
                (r) -> assertionsAllJsonProgressInfo(r, true));

        assertionsAllJsonDbResult();
    }

    @Test
    public void shouldTerminateImportJson() {
        createConstraints(List.of("Movie", "Other", "Person"));
        checkTerminationGuard(db, "CALL apoc.import.json('testTerminate.json',{})");
    }

    private void assertionsAllJsonProgressInfo(Map<String, Object> r, boolean isBinary) {
        // then
        assertEquals(isBinary ? null : "all.json", r.get("file"));
        assertEquals(isBinary ? "binary" : "file", r.get("source"));
        assertEquals("json", r.get("format"));
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(15L, r.get("properties"));
        assertEquals(4L, r.get("rows"));
        assertEquals(true, r.get("done"));
    }

    private void assertionsAllJsonDbResult() {
        assertionsAllJsonDbResult(9, 1L);
    }

    private void assertionsAllJsonDbResult(int expectedPropSize, long relCount) {
        try (Transaction tx = db.beginTx()) {
            final long countNodes = tx.execute("MATCH (n:User) RETURN count(n) AS count")
                    .<Long>columnAs("count")
                    .next();
            assertEquals(3L, countNodes);

            final long countRels = tx.execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count")
                    .<Long>columnAs("count")
                    .next();
            assertEquals(relCount, countRels);

            final Map<String, Object> props = tx.execute("MATCH (n:User {name: 'Adam'}) RETURN n")
                    .<Node>columnAs("n")
                    .next()
                    .getAllProperties();

            assertEquals(expectedPropSize, props.size());
            assertEquals("wgs-84", props.get("place.crs"));
            assertEquals(13.1D, props.get("place.latitude"));
            assertEquals(33.46789D, props.get("place.longitude"));
            assertFalse(props.containsKey("place"));
        }
    }

    private void assertRootMessage(String expectedMsg, Exception e) {
        Throwable except = ExceptionUtils.getRootCause(e);
        TestCase.assertTrue(except instanceof RuntimeException);
        assertEquals(expectedMsg, except.getMessage());
    }

    private void createConstraints(List<String> labels, String customId) {
        labels.forEach(label -> db.executeTransactionally(
                format("CREATE CONSTRAINT FOR (n:%s) REQUIRE n.%s IS UNIQUE;", Util.quote(label), customId)));
    }

    private void createConstraints(List<String> labels) {
        createConstraints(labels, "neo4jImportId");
    }

    private void assertEntities(long expectedNodes, long expectedRels) {
        assertEntities(expectedNodes, expectedRels, null, null);
    }

    private void assertEntities(
            long expectedNodes, long expectedRels, Consumer<Node> nodeConsumer, Consumer<Relationship> relConsumer) {
        try (Transaction tx = db.beginTx()) {
            final List<Node> nodeList = Iterables.asList(tx.getAllNodes());
            final List<Relationship> relList = Iterables.asList((tx.getAllRelationships()));
            assertEquals(expectedNodes, nodeList.size());
            assertEquals(expectedRels, relList.size());
            if (nodeConsumer != null) {
                nodeList.forEach(nodeConsumer);
            }
            if (relConsumer != null) {
                relList.forEach(relConsumer);
            }
        }
    }
}
