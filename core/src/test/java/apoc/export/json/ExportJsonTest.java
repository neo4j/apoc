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

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.json.JsonFormat.Format;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.CompressionAlgo.DEFLATE;
import static apoc.util.CompressionAlgo.FRAMED_SNAPPY;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.assertError;
import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import apoc.HelperProcedures;
import apoc.graph.Graphs;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.google.common.io.Resources;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ExportJsonTest {

    private static final String DEFLATE_EXT = ".zz";
    private static final File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    directory.toPath().toAbsolutePath())
            // Run with aligned format to get sequential ids (assertions depends on this)
            .withSetting(GraphDatabaseSettings.db_format, "aligned");

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, ExportJson.class, ImportJson.class, Graphs.class, HelperProcedures.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        db.executeTransactionally(
                """
                        CREATE (f:User {
                            name:'Adam',
                            age:42,
                            male:true,
                            kids:['Sam','Anna','Grace'],
                            born:localdatetime('2015185T19:32:24'),
                            place:point({latitude: 13.1, longitude: 33.46789})
                          }
                        )-[:KNOWS {
                            since: 1993,
                            bffSince: duration('P5M1.5D')
                          }
                        ]->(b:User {name:'Jim',age:42}),
                        (c:User {age:12})
                        """);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testExportAllJson() {
        String filename = "all.json";
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.all($file,null)",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportJsonAdminOperationErrorMessage() {
        String filename = "test.json";
        List<String> invalidQueries =
                List.of("SHOW CONSTRAINTS YIELD id, name, type RETURN *", "SHOW INDEXES YIELD id, name, type RETURN *");

        for (String query : invalidQueries) {
            QueryExecutionException e = Assert.assertThrows(
                    QueryExecutionException.class,
                    () -> TestUtil.testCall(
                            db,
                            """
                        CALL apoc.export.json.query(
                        $query,
                        $file
                        )""",
                            map("query", query, "file", filename),
                            (r) -> {}));

            assertError(e, INVALID_QUERY_MODE_ERROR, RuntimeException.class, "apoc.export.json.query");
        }
    }

    @Test
    public void testJsonRoundtrip() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE n.neo4jImportId IS UNIQUE;");
        String filename = "all.json.gzip";
        final Map<String, Object> params =
                map("file", filename, "config", map(COMPRESSION, CompressionAlgo.GZIP.name()));
        TestUtil.testCall(
                db, "CALL apoc.export.json.all($file, $config)", params, (r) -> assertResults(filename, r, "database"));

        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        TestUtil.testCall(db, "CALL apoc.import.json($file, $config) ", params, r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n) RETURN n order by coalesce(n.name, '')", r -> {
            final ResourceIterator<Node> iterator = r.columnAs("n");
            final Node first = iterator.next();
            assertEquals(12L, first.getProperty("age"));
            assertFalse(first.hasProperty("name"));
            assertEquals(List.of(Label.label("User")), first.getLabels());

            final Node second = iterator.next();
            assertEquals(42L, second.getProperty("age"));
            assertEquals("Adam", second.getProperty("name"));
            assertEquals(List.of(Label.label("User")), second.getLabels());

            final Node third = iterator.next();
            assertEquals(42L, third.getProperty("age"));
            assertEquals("Jim", third.getProperty("name"));
            assertEquals(List.of(Label.label("User")), third.getLabels());

            assertFalse(iterator.hasNext());
        });
    }

    @Test
    public void testExportAllJsonArray() {
        String filename = "all_array.json";
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.all($file, {jsonFormat: 'ARRAY_JSON'})",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllJsonFields() {
        String filename = "all_fields.json";
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.all($file, {jsonFormat: 'JSON'})",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllJsonIdAsKeys() {
        String filename = "all_id_as_keys.json";
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.all($file, {jsonFormat: 'JSON_ID_AS_KEYS'})",
                map("file", filename),
                (r) -> assertResults(filename, r, "database"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportAllJsonStream() {
        TestUtil.testCall(db, "CALL apoc.export.json.all(null, {stream: true})", (r) -> {
            assertStreamResults(r, "database");
            assertThat(r.get("data").toString().lines())
                    .zipSatisfy(readResourceLines("exportJSON/all.json"), jsonEquals);
        });
    }

    @Test
    public void testExportAllJsonStreamWithFormatConfig() {
        Map.of(
                        Format.JSON_LINES.name(),
                        "all.json",
                        Format.JSON.name(),
                        "all_fields.json",
                        Format.ARRAY_JSON.name(),
                        "all_array.json",
                        Format.JSON_ID_AS_KEYS.name(),
                        "all_id_as_keys.json")
                .forEach((jsonFormat, resource) -> {
                    TestUtil.testCall(
                            db,
                            "CALL apoc.export.json.all(null, $config)",
                            map("config", map("jsonFormat", jsonFormat, "stream", true)),
                            (r) -> {
                                assertStreamResults(r, "database");
                                assertThat(r.get("data").toString().lines())
                                        .zipSatisfy(readResourceLines("exportJSON/" + resource), jsonEquals);
                            });
                });
    }

    @Test
    public void testExportAllJsonStreamWithCompression() {
        final CompressionAlgo algo = FRAMED_SNAPPY;
        String expectedFile = "all.json";
        String filename = expectedFile + ".sz";
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.all(null, $config)",
                map("file", filename, "config", map("stream", true, "compression", algo.name())),
                (r) -> {
                    assertStreamResults(r, "database");
                    assertThat(getDecompressedData(algo, r.get("data")).lines())
                            .zipSatisfy(readResourceLines("exportJSON/all.json"), jsonEquals);
                });
    }

    @Test
    public void testExportPointMapDatetimeJson() {
        String filename = "mapPointDatetime.json";
        String query =
                """
                RETURN {
                    data: 1,
                    value: {
                        age: 12,
                        name:'Mike',
                        data: {
                            number: [1,3,5],
                            born: date('2018-10-29'),
                            place: point({latitude: 13.1, longitude: 33.46789})
                        }
                    }
                } AS map,
                datetime('2015-06-24T12:50:35.556+0100') AS theDateTime,
                localdatetime('2015185T19:32:24') AS theLocalDateTime,
                point({latitude: 13.1, longitude: 33.46789}) AS point,
                date('+2015-W13-4') AS date,
                time('125035.556+0100') AS time,
                localTime('12:50:35.556') AS localTime
                """;
        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(7)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                    assertFileEquals(filename);
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportPointMapDatetimeStreamJson() {
        String filename = "mapPointDatetime.json";
        String query =
                """
                RETURN {
                    data: 1,
                    value: {
                        age: 12,
                        name:'Mike',
                        data: {
                            number: [1,3,5],
                            born: date('2018-10-29'),
                            place: point({latitude: 13.1, longitude: 33.46789})
                        }
                    }
                } AS map,
                datetime('2015-06-24T12:50:35.556+0100') AS theDateTime,
                localdatetime('2015185T19:32:24') AS theLocalDateTime,
                point({latitude: 13.1, longitude: 33.46789}) AS point,
                date('+2015-W13-4') AS date,
                time('125035.556+0100') AS time,
                localTime('12:50:35.556') AS localTime
                """;
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.query($query, null, {stream: true})",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(7)"));
                    assertThat(r.get("data").toString().lines())
                            .zipSatisfy(readResourceLines("exportJSON/mapPointDatetime.json"), jsonEquals);
                });
    }

    @Test
    public void testExportListNode() {
        String filename = "listNode.json";

        String query = "MATCH (u:User) RETURN COLLECT(u) AS list";

        TestUtil.testCall(
                db,
                "CALL apoc.export.json.query($query,$file)",
                map("file", filename, "query", query),
                (r) -> assertionsListNode(filename, r));
        assertFileEquals(filename);
    }

    @Test
    public void testExportListNodeWithCompression() {
        String query = "MATCH (u:User) RETURN COLLECT(u) AS list";
        final CompressionAlgo algo = DEFLATE;
        String expectedFile = "listNode.json";
        String filename = expectedFile + DEFLATE_EXT;

        TestUtil.testCall(
                db,
                "CALL apoc.export.json.query($query, $file, $config)",
                map("file", filename, "query", query, "config", map("compression", algo.name())),
                (r) -> assertionsListNode(filename, r));
        assertFileEquals(expectedFile, algo);
    }

    private void assertionsListNode(String filename, Map<String, Object> r) {
        assertTrue("Should get statement", r.get("source").toString().contains("statement: cols(1)"));
        assertEquals(filename, r.get("file"));
        assertEquals("json", r.get("format"));
    }

    @Test
    public void testExportListRel() {
        String filename = "listRel.json";

        String query = "MATCH (u:User)-[rel:KNOWS]->(u2:User) RETURN COLLECT(rel) AS list";

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportListPath() {
        String filename = "listPath.json";

        String query = "MATCH p = (u:User)-[rel]->(u2:User) RETURN COLLECT(p) AS list";

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMap() {
        String filename = "MapNode.json";

        String query = "MATCH (u:User)-[r:KNOWS]->(d:User) RETURN u {.*}, d {.*}, r {.*}";

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(3)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMapPath() {
        db.executeTransactionally(
                """
                        CREATE (
                            f:User {name:'Mike',age:78,male:true}
                        )-[:KNOWS {since: 1850}]->(
                            b:User {name:'John',age:18}),(c:User {age:39}
                        )
                        """);
        String filename = "MapPath.json";

        String query = "MATCH path = (u:User)-[rel:KNOWS]->(u2:User) RETURN {key:path} AS map, 'Kate' AS name";

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(2)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMapRel() {
        String filename = "MapRel.json";
        String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel {.*}";

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportMapComplex() {
        String filename = "MapComplex.json";

        String query =
                """
                    RETURN {
                        value:1,
                        data:[
                            10,
                            'car',
                            null,
                            point({ longitude: 56.7, latitude: 12.78 }),
                            point({ longitude: 56.7, latitude: 12.78, height: 8 }),
                            point({ x: 2.3, y: 4.5 }),
                            point({ x: 2.3, y: 4.5, z: 2 }),
                            date('2018-10-10'),
                            datetime('2018-10-18T14:21:40.004Z'),
                            localdatetime({ year:1984, week:10, dayOfWeek:3, hour:12, minute:31, second:14, millisecond: 645 }),
                            {x:1, y:[1,2,3,{ age:10 }]}
                        ]
                    } AS key
                """;

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportGraphJson() {
        String filename = "graph.json";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) YIELD graph " + "CALL apoc.export.json.graph(graph, $file) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("file", filename),
                (r) -> assertResults(filename, r, "graph"));
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryJson() {
        String filename = "query.json";
        String query = "MATCH (u:User) RETURN u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryNodesJson() {
        String filename = "query_nodes.json";
        String query = "MATCH (u:User) RETURN u";
        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryTwoNodesJson() {
        String filename = "query_two_nodes.json";
        String query = "MATCH (u:User{name:'Adam'}), (l:User{name:'Jim'}) RETURN u, l";
        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(2)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });

        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryNodesJsonParams() {
        String filename = "query_nodes_param.json";
        String query = "MATCH (u:User) WHERE u.age > $age RETURN u";
        TestUtil.testCall(
                db,
                "CALL apoc.export.json.query($query,$file,{params:{age:10}})",
                map("file", filename, "query", query),
                (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryNodesJsonCount() {
        String filename = "query_nodes_count.json";
        String query = "MATCH (n) RETURN count(n)";
        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportData() {
        String filename = "data.json";
        TestUtil.testCall(
                db,
                """
                        MATCH (nod:User) MATCH ()-[reels:KNOWS]->()
                        WITH collect(nod) AS node, collect(reels) AS rels
                        CALL apoc.export.json.data(node, rels, $file, null)
                        YIELD nodes, relationships, properties, file, source, format, time
                        RETURN *""",
                map("file", filename),
                (r) -> {
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportDataWithNodeAndRelProps() {
        var filenames = Map.of(
                "", "data_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:true", "data_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:false", "data_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:true", "data_withNodeProps_withRelProps.json",
                "writeNodeProperties:false", "data_withoutNodeProps_withoutRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:true", "data_withNodeProps_withRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:false",
                        "data_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:true",
                        "data_withoutNodeProps_withRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:false",
                        "data_withoutNodeProps_withoutRelProps.json");
        for (var entry : filenames.entrySet()) {
            var config = entry.getKey();
            var filename = entry.getValue();
            TestUtil.testCall(
                    db,
                    """
                            MATCH (nod:User) MATCH ()-[reels:KNOWS]->()
                            WITH collect(nod) AS node, collect(reels) AS rels
                            CALL apoc.export.json.data(node, rels, $file, {%s})
                            YIELD nodes, relationships, properties, file, source, format, time
                            RETURN *"""
                            .formatted(config),
                    map("file", filename),
                    (r) -> {
                        assertEquals(filename, r.get("file"));
                        assertEquals("json", r.get("format"));
                    });
            assertFileEquals(filename);
        }
    }

    @Test
    public void testExportDataPath() {
        String filename = "query_nodes_path.json";
        String query = "MATCH p = (u:User)-[rel]->(u2:User) RETURN u, rel, u2, p, u.name";
        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportQueryWithWriteNodePropertiesJson() {
        var filenames = Map.of(
                "", "query_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:true", "query_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:false", "query_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:true", "query_withNodeProps_withRelProps.json",
                "writeNodeProperties:false", "query_withoutNodeProps_withoutRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:true", "query_withNodeProps_withRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:false",
                        "query_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:true",
                        "query_withoutNodeProps_withRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:false",
                        "query_withoutNodeProps_withoutRelProps.json");

        for (var entry : filenames.entrySet()) {
            var config = entry.getKey();
            var filename = entry.getValue();

            String query = "MATCH p = (u:User)-[rel:KNOWS]->(u2:User) RETURN rel";

            TestUtil.testCall(
                    db,
                    "CALL apoc.export.json.query($query,$file,{%s})".formatted(config),
                    map("file", filename, "query", query),
                    (r) -> {
                        assertTrue(
                                "Should get statement",
                                r.get("source").toString().contains("statement: cols(1)"));
                        assertEquals(filename, r.get("file"));
                        assertEquals("json", r.get("format"));
                    });
            assertFileEquals(filename);
        }
    }

    @Test
    public void testExportAllWithWriteNodePropertiesJson() {
        var filenames = Map.of(
                "", "all_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:true", "all_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:false", "all_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:true", "all_withNodeProps_withRelProps.json",
                "writeNodeProperties:false", "all_withoutNodeProps_withoutRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:true", "all_withNodeProps_withRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:false", "all_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:true", "all_withoutNodeProps_withRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:false",
                        "all_withoutNodeProps_withoutRelProps.json");

        for (var entry : filenames.entrySet()) {
            var config = entry.getKey();
            var filename = entry.getValue();

            TestUtil.testCall(
                    db,
                    "CALL apoc.export.json.all($file,{%s})".formatted(config),
                    map("file", filename),
                    (r) -> assertResults(filename, r, "database"));
            assertFileEquals(filename);
        }
    }

    @Test
    public void testExportGraphWithWriteNodePropertiesJson() {
        var filenames = Map.of(
                "", "all_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:true", "all_withNodeProps_withRelProps.json",
                "writeRelationshipProperties:false", "all_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:true", "all_withNodeProps_withRelProps.json",
                "writeNodeProperties:false", "all_withoutNodeProps_withoutRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:true", "all_withNodeProps_withRelProps.json",
                "writeNodeProperties:true, writeRelationshipProperties:false", "all_withNodeProps_withoutRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:true", "all_withoutNodeProps_withRelProps.json",
                "writeNodeProperties:false, writeRelationshipProperties:false",
                        "all_withoutNodeProps_withoutRelProps.json");

        for (var entry : filenames.entrySet()) {
            var config = entry.getKey();
            var filename = entry.getValue();

            TestUtil.testCall(
                    db,
                    """
                            CALL apoc.graph.fromDB('test',{}) YIELD graph
                            CALL apoc.export.json.graph(graph, $file, {%s})
                            YIELD nodes, relationships, properties, file, source, format, time
                            RETURN *"""
                            .formatted(config),
                    map("file", filename),
                    (r) -> assertResults(filename, r, "graph"));
            assertFileEquals(filename);
        }
    }

    @Test
    public void testExportQueryOrderJson() {
        db.executeTransactionally("CREATE (f:User12:User1:User0:User {name:'Alan'})");
        String filename = "query_node_labels.json";
        String query = "MATCH (u:User) WHERE u.name='Alan' RETURN u";

        TestUtil.testCall(
                db, "CALL apoc.export.json.query($query,$file)", map("file", filename, "query", query), (r) -> {
                    assertTrue(
                            "Should get statement", r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(filename, r.get("file"));
                    assertEquals("json", r.get("format"));
                });
        assertFileEquals(filename);
    }

    @Test
    public void testExportWgsPoint() {
        db.executeTransactionally(
                "CREATE (p:Position {place: point({latitude: 12.78, longitude: 56.7, height: 1.1})})");

        TestUtil.testCall(
                db,
                "CALL apoc.export.json.query($query, null, {stream: true}) YIELD data RETURN data",
                map("query", "MATCH (p:Position) RETURN p.place as place"),
                (r) -> {
                    String data = (String) r.get("data");
                    Map<String, Object> map = Util.fromJson(data, Map.class);
                    Map<String, Object> place = (Map<String, Object>) map.get("place");
                    assertEquals(12.78D, (double) place.get("latitude"), 0);
                    assertEquals(56.7D, (double) place.get("longitude"), 0);
                    assertEquals(1.1D, (double) place.get("height"), 0);
                });

        db.executeTransactionally("MATCH (n:Position) DETACH DELETE n");
    }

    @Test
    public void testExportOfNodeIntArrays() {
        db.executeTransactionally(
                """
                CREATE (test:Test {
                    intArray: [1,2,3,4],
                    boolArray: [true,false],
                    floatArray: [1.0,2.0]
                })
                """);

        TestUtil.testCall(
                db,
                """
                   CALL apoc.export.json.query(
                        "MATCH (test:Test) RETURN test{.intArray, .boolArray, .floatArray} AS data",
                        null,
                        {stream:true}
                    )
                   YIELD data
                   RETURN data
                """,
                (r) -> {
                    String data = (String) r.get("data");
                    Map<String, Object> map = Util.fromJson(data, Map.class);
                    Map<String, Object> arrays = (Map<String, Object>) map.get("data");
                    assertEquals(new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L)), arrays.get("intArray"));
                    assertEquals(new ArrayList<>(Arrays.asList(true, false)), arrays.get("boolArray"));
                    assertEquals(new ArrayList<>(Arrays.asList(1.0, 2.0)), arrays.get("floatArray"));
                });

        db.executeTransactionally("MATCH (n:Test) DETACH DELETE n");
    }

    private void assertResults(String filename, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(11L, r.get("properties"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals(filename, r.get("file"));
        assertEquals("json", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    private void assertFileEquals(String fileName, CompressionAlgo algo) {
        String fileExt = algo.equals(DEFLATE) ? DEFLATE_EXT : "";
        String actualText = BinaryTestUtil.readFileToString(new File(directory, fileName + fileExt), UTF_8, algo);
        assertThat(actualText.lines()).zipSatisfy(readResourceLines("exportJSON/" + fileName), jsonEquals);
    }

    private void assertFileEquals(String fileName) {
        assertFileEquals(fileName, NONE);
    }

    private void assertStreamResults(Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(11L, r.get("properties"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertNull("file should be null", r.get("file"));
        assertNotNull("data should be not null", r.get("data"));
        assertEquals("json", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    @Test
    public void testDifferentCypherVersionsApocJsonQuery() {
        for (HelperProcedures.CypherVersionCombinations cypherVersion : HelperProcedures.cypherVersions) {
            var query = String.format(
                    "%s CALL apoc.export.json.query('%s RETURN apoc.cypherVersion() AS version', null, { stream:true }) YIELD data RETURN data",
                    cypherVersion.outerVersion, cypherVersion.innerVersion);
            testCall(
                    db, query, r -> TestCase.assertTrue(r.get("data").toString().contains(cypherVersion.result)));
        }
    }

    private static final BiConsumer<String, String> jsonEquals =
            (actual, expected) -> assertThatJson(actual).isEqualTo(expected);

    private List<String> readResourceLines(String resource) {
        try {
            return Resources.readLines(Resources.getResource(resource), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + resource, e);
        }
    }
}
