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
package apoc.export.csv;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.CompressionAlgo.GZIP;
import static apoc.util.MapUtil.map;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import apoc.graph.Graphs;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ExportCsvNeo4jAdminTest {

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_TYPES_NODE = String.format(
            "\":ID\";\"born_2D:point\";\"born_3D:point\";\"localtime:localtime\";\"time:time\";\"dateTime:datetime\";\"localDateTime:localdatetime\";\"date:date\";\"duration:duration\";\":LABEL\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_TYPES_NODE = String.format(
            "\"6\";\"{\"\"crs\"\":\"\"cartesian\"\",\"\"x\"\":2.3,\"\"y\"\":4.5,\"\"z\"\":null}\";\"{\"\"crs\"\":\"\"wgs-84-3d\"\",\"\"latitude\"\":12.78,\"\"longitude\"\":56.7,\"\"height\"\":100.0}\";\"12:50:35.556\";\"12:50:35.556+01:00\";\"2018-10-30T12:50:35.556+01:00\";\"2018-10-30T19:32:24\";\"2018-10-30\";\"P5M1DT12H\";\"Types\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS =
            String.format("\":ID\";\"name\";\"street\";\":LABEL\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS1 =
            String.format("\":ID\";\"street\";\"name\";\"city\";\":LABEL\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER =
            String.format("\":ID\";\"name\";\"age:long\";\":LABEL\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER1 =
            String.format("\":ID\";\"name\";\"age:long\";\"male:boolean\";\"kids\";\":LABEL\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_KNOWS =
            String.format("\":START_ID\";\":END_ID\";\":TYPE\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_NEXT_DELIVERY =
            String.format("\":START_ID\";\":END_ID\";\":TYPE\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS =
            String.format("\"4\";\"Bar Sport\";\"\";\"Address\"%n" + "\"5\";\"\";\"via Benni\";\"Address\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS1 =
            String.format("\"3\";\"Via Garibaldi, 7\";\"Andrea\";\"Milano\";\"Address1;Address\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER =
            String.format("\"1\";\"bar\";\"42\";\"User\"%n" + "\"2\";\"\";\"12\";\"User\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER1 = String.format(
            "\"0\";\"foo \"\"the\"\" bar\";\"42\";\"true\";\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\";\"User1;User\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_KNOWS =
            String.format("\"0\";\"1\";\"KNOWS\"%n");

    private static final String EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_NEXT_DELIVERY =
            String.format("\"3\";\"4\";\"NEXT_DELIVERY\"%n");

    private static final String GZIP_EXT = ".foo";

    private static final File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    directory.toPath().toAbsolutePath());

    @Before
    public void before() {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        db.executeTransactionally(
                "CREATE (f:User1:User {name:'foo \"the\" bar',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally(
                "CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
        db.executeTransactionally(
                "CREATE (a:Types {date: date('2018-10-30'), localDateTime: localdatetime('20181030T19:32:24'), dateTime: datetime('2018-10-30T12:50:35.556+0100'), localtime: localtime('12:50:35.556'), duration: duration('P5M1DT12H'), time: time('125035.556+0100'), born_2D: point({ x: 2.3, y: 4.5 }), born_3D:point({ longitude: 56.7, latitude: 12.78, height: 100 })})");
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithCompressionNone() {
        String fileBaseName = "query_nodes_no_compress_and_Ext";
        String fileExpectedExt = ".csv";
        assertionTestExportForAdminNeo4jImport(CompressionAlgo.NONE, fileBaseName, fileExpectedExt);
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithCompressionNoneWithoutExtension() {
        String fileBaseName = "query_nodes_no_compress";
        assertionTestExportForAdminNeo4jImport(CompressionAlgo.NONE, fileBaseName, "");
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithCompressionNoneAndMultiDotInName() {
        String fileBaseName = "query_nodes.dots.in";
        String fileExpectedExt = ".name";
        assertionTestExportForAdminNeo4jImport(CompressionAlgo.NONE, fileBaseName, fileExpectedExt);
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithConfigWithCompression() {
        String fileBaseName = "query_nodes_with_csvgz_ext.csv";
        assertionTestExportForAdminNeo4jImport(GZIP, fileBaseName, GZIP_EXT);
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithCompressionAndWithoutExtension() {
        String fileBaseName = "query_nodes_with_ext";
        assertionTestExportForAdminNeo4jImport(GZIP, fileBaseName, GZIP_EXT);
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithCompressionAndWithoutAnyExtension() {
        String fileBaseName = "query_nodes_with_no_ext";
        assertionTestExportForAdminNeo4jImport(GZIP, fileBaseName, "");
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportWithCompressionAndWithoutExtensionAndMultiDotInName() {
        String fileBaseName = "query_nodes_with_ext.dots.name";
        assertionTestExportForAdminNeo4jImport(GZIP, fileBaseName, GZIP_EXT);
    }

    private void assertionTestExportForAdminNeo4jImport(
            CompressionAlgo algo, String fileBaseName, String fileExpectedExt) {
        final String fileName = fileBaseName + fileExpectedExt;
        File dir = new File(directory, fileName);

        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($fileName,{compression: $compression, bulkImport: true, separateHeader: true, delim: ';'})",
                map("fileName", fileName, "compression", algo.name()),
                r -> {
                    assertEquals(20000L, r.get("batchSize"));
                    assertEquals(1L, r.get("batches"));
                    assertEquals(7L, r.get("nodes"));
                    assertEquals(9L, r.get("rows"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(20L, r.get("properties"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
                });

        String file = dir.getParent() + File.separator;
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS,
                fileBaseName + ".header.nodes.Address" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS1,
                fileBaseName + ".header.nodes.Address1.Address" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER,
                fileBaseName + ".header.nodes.User" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER1,
                fileBaseName + ".header.nodes.User1.User" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_TYPES_NODE,
                fileBaseName + ".header.nodes.Types" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_KNOWS,
                fileBaseName + ".header.relationships.KNOWS" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_NEXT_DELIVERY,
                fileBaseName + ".header.relationships.NEXT_DELIVERY" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS,
                fileBaseName + ".nodes.Address" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS1,
                fileBaseName + ".nodes.Address1.Address" + fileExpectedExt,
                algo);
        assertFileEquals(
                file, EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER, fileBaseName + ".nodes.User" + fileExpectedExt, algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER1,
                fileBaseName + ".nodes.User1.User" + fileExpectedExt,
                algo);
        assertFileEquals(
                file, EXPECTED_NEO4J_ADMIN_IMPORT_TYPES_NODE, fileBaseName + ".nodes.Types" + fileExpectedExt, algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_KNOWS,
                fileBaseName + ".relationships.KNOWS" + fileExpectedExt,
                algo);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_NEXT_DELIVERY,
                fileBaseName + ".relationships.NEXT_DELIVERY" + fileExpectedExt,
                algo);
    }

    @Test
    public void testExportGraphNeo4jAdminCsvWithoutFileExt() {
        testExportGraphNeo4jAdminCsvCommon("graph_with_no_ext", "");
    }

    @Test
    public void testExportGraphNeo4jAdminCsvWithFileExt() {
        testExportGraphNeo4jAdminCsvCommon("graph", ".csv");
    }

    @Test
    public void testExportGraphNeo4jAdminCsvWithFileExtMultiDotInName() {
        testExportGraphNeo4jAdminCsvCommon("graph.multi.dots.name.file", ".csv");
    }

    private void testExportGraphNeo4jAdminCsvCommon(String fileBaseName, String fileExpectedExt) {
        final String fileName = fileBaseName + fileExpectedExt;
        File output = new File(directory, fileName);
        String separator = ";";
        TestUtil.testCall(
                db,
                "CALL apoc.graph.fromDB('test',{}) yield graph "
                        + "CALL apoc.export.csv.graph(graph, $fileName,{bulkImport: true, delim: $separator}) "
                        + "YIELD nodes, relationships, properties, file, source,format, time "
                        + "RETURN *",
                map("fileName", fileName, "separator", separator),
                (r) -> assertResults(fileName, r));

        String file = output.getParent() + File.separator;
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS + EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS,
                fileBaseName + ".nodes.Address" + fileExpectedExt,
                separator);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_ADDRESS1 + EXPECTED_NEO4J_ADMIN_IMPORT_NODE_ADDRESS1,
                fileBaseName + ".nodes.Address1.Address" + fileExpectedExt,
                separator);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER + EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER,
                fileBaseName + ".nodes.User" + fileExpectedExt,
                separator);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_NODE_USER1 + EXPECTED_NEO4J_ADMIN_IMPORT_NODE_USER1,
                fileBaseName + ".nodes.User1.User" + fileExpectedExt,
                separator);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_TYPES_NODE + EXPECTED_NEO4J_ADMIN_IMPORT_TYPES_NODE,
                fileBaseName + ".nodes.Types" + fileExpectedExt,
                separator);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_KNOWS + EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_KNOWS,
                fileBaseName + ".relationships.KNOWS" + fileExpectedExt,
                separator);
        assertFileEquals(
                file,
                EXPECTED_NEO4J_ADMIN_IMPORT_HEADER_RELATIONSHIP_NEXT_DELIVERY
                        + EXPECTED_NEO4J_ADMIN_IMPORT_RELATIONSHIP_NEXT_DELIVERY,
                fileBaseName + ".relationships.NEXT_DELIVERY" + fileExpectedExt,
                separator);
    }

    private void assertFileEquals(String base, String expected, String file) {
        assertFileEquals(base, expected, file, ",", CompressionAlgo.NONE);
    }

    private void assertFileEquals(String base, String expected, String file, CompressionAlgo algo) {
        assertFileEquals(base, expected, file, ",", algo);
    }

    private void assertFileEquals(String base, String expected, String file, String separator) {
        assertFileEquals(base, expected, file, separator, CompressionAlgo.NONE);
    }

    private void assertFileEquals(String base, String expected, String file, String separator, CompressionAlgo algo) {
        final List<Map<String, Object>> expectedList = convertCSVString(expected, separator);
        final String actual = BinaryTestUtil.readFileToString(new File(base + file), StandardCharsets.UTF_8, algo);
        final List<Map<String, Object>> actualList = convertCSVString(actual, separator);
        assertEquals(expectedList, actualList);
    }

    private List<Map<String, Object>> convertCSVString(String csv, String separator) {
        List<String> lines = List.of(csv.split("\n"));
        if (lines.size() <= 1) return List.of();
        List<String> header = List.of(lines.get(0).split(separator));
        return lines.stream()
                .skip(1)
                .map(line -> List.of(line.split(separator)))
                .map(cols -> {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 0; i < header.size(); i++) {
                        row.put(header.get(i), cols.get(i));
                    }
                    return row;
                })
                .collect(Collectors.toList());
    }

    @Test
    public void testCypherExportCsvForAdminNeo4jImportExceptionBulk() {
        String fileName = "query_nodes.csv";
        RuntimeException e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.testCall(
                        db,
                        "CALL apoc.export.csv.query('MATCH (n) return (n)',$fileName,{bulkImport: true})",
                        Util.map("fileName", fileName),
                        (r) -> {}));
        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(
                "You can use the `bulkImport` only with apoc.export.csv.all and apoc.export.csv.graph",
                except.getMessage());
    }

    private void assertResults(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(20L, r.get("properties"));
        assertEquals("graph: nodes(7), rels(2)", r.get("source"));
        assertEquals(fileName, r.get("file"));
        assertEquals("csv", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportCypherWithIdField() {
        // given
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        final Map<String, Object> map = db.executeTransactionally(
                "CREATE (source:User:Larus{id: 1, name: 'Andrea'})-[:KNOWS{id: 10}]->(target:User:Neo4j{id: 2, name: 'Michael'})\n"
                        + "RETURN id(source) as sourceId, id(target) as targetId",
                Collections.emptyMap(),
                Iterators::single);
        final String fileName = "export_id_field";
        String fileNameWithExtension = fileName + ".csv";
        File dir = new File(directory, fileNameWithExtension);

        // when
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($fileNameWithExtension,{bulkImport: true})",
                map("fileNameWithExtension", fileNameWithExtension),
                r -> {
                    // then
                    assertEquals(20000L, r.get("batchSize"));
                    assertEquals(1L, r.get("batches"));
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(3L, r.get("rows"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(5L, r.get("properties"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);

                    String file = dir.getParent() + File.separator;
                    String expectedNodesLarus = String.format(
                            "\":ID\",\"id:long\",\"name\",\":LABEL\"%n" + "\"%s\",\"1\",\"Andrea\",\"User;Larus\"%n",
                            map.get("sourceId"));
                    String expectedNodesNeo4j = String.format(
                            "\":ID\",\"id:long\",\"name\",\":LABEL\"%n" + "\"%s\",\"2\",\"Michael\",\"User;Neo4j\"%n",
                            map.get("targetId"));
                    String expectedRelsNeo4j = String.format(
                            "\":START_ID\",\":END_ID\",\":TYPE\",\"id:long\"%n" + "\"%s\",\"%s\",\"KNOWS\",\"10\"%n",
                            map.get("sourceId"), map.get("targetId"));

                    assertFileEquals(file, expectedNodesLarus, fileName + ".nodes.User.Larus.csv");
                    assertFileEquals(file, expectedNodesNeo4j, fileName + ".nodes.User.Neo4j.csv");
                    assertFileEquals(file, expectedRelsNeo4j, fileName + ".relationships.KNOWS.csv");
                });
    }
}
