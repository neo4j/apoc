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

import apoc.csv.CsvTestUtil;
import apoc.graph.Graphs;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.meta.Meta;
import apoc.util.CompressionConfig;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.nio.charset.Charset;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.util.ExportConfig.IF_NEEDED_QUUOTES;
import static apoc.export.util.ExportConfig.NONE_QUOTES;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.CompressionAlgo.DEFLATE;
import static apoc.util.CompressionAlgo.GZIP;
import static apoc.util.CompressionAlgo.NONE;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.assertError;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCsvTest {
    public static final String CALL_ALL = "CALL apoc.export.csv.all($file, $config)";
    public static final String CALL_QUERY = "CALL apoc.export.csv.query($query,$file, $config)";
    public static final String CALL_DATA = """
            CALL apoc.graph.fromDB('test',{}) yield graph
            CALL apoc.export.csv.data(graph.nodes, graph.relationships, $file, $config)
            YIELD nodes, relationships, properties, file, source,format, time RETURN *""";
    public static final String CALL_GRAPH = """
            CALL apoc.graph.fromDB('test',{}) yield graph
            CALL apoc.export.csv.graph(graph, $file, $config)
            YIELD nodes, relationships, properties, file, source,format, time RETURN *""";

    private static final String EXPECTED_QUERY_NODES = String.format("\"u\"%n" +
            "\"{\"\"id\"\":0,\"\"labels\"\":[\"\"User\"\",\"\"User1\"\"],\"\"properties\"\":{\"\"name\"\":\"\"foo\"\",\"\"age\"\":42,\"\"male\"\":true,\"\"kids\"\":[\"\"a\"\",\"\"b\"\",\"\"c\"\"]}}\"%n" +
            "\"{\"\"id\"\":1,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"name\"\":\"\"bar\"\",\"\"age\"\":42}}\"%n" +
            "\"{\"\"id\"\":2,\"\"labels\"\":[\"\"User\"\"],\"\"properties\"\":{\"\"age\"\":12}}\"%n");
    private static final String EXPECTED_QUERY = String.format("\"u.age\",\"u.name\",\"u.male\",\"u.kids\",\"labels(u)\"%n" +
            "\"42\",\"foo\",\"true\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"[\"\"User1\"\",\"\"User\"\"]\"%n" +
            "\"42\",\"bar\",\"\",\"\",\"[\"\"User\"\"]\"%n" +
            "\"12\",\"\",\"\",\"\",\"[\"\"User\"\"]\"%n");
    private static final String EXPECTED_QUERY_WITHOUT_QUOTES = String.format("u.age,u.name,u.male,u.kids,labels(u)%n" +
            "42,foo,true,[\"a\",\"b\",\"c\"],[\"User1\",\"User\"]%n" +
            "42,bar,,,[\"User\"]%n" +
            "12,,,,[\"User\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_NONE = String.format("a.name,a.city,a.street,labels(a)%n" +
            "Andrea,Milano,Via Garibaldi, 7,[\"Address1\",\"Address\"]%n" +
            "Bar Sport,,,[\"Address\"]%n" +
            ",,via Benni,[\"Address\"]%n");
    private static final String EXPECTED_QUERY_QUOTES_ALWAYS = String.format("\"a.name\",\"a.city\",\"a.street\",\"labels(a)\"%n" +
            "\"Andrea\",\"Milano\",\"Via Garibaldi, 7\",\"[\"\"Address1\"\",\"\"Address\"\"]\"%n" +
            "\"Bar Sport\",\"\",\"\",\"[\"\"Address\"\"]\"%n" +
            "\"\",\"\",\"via Benni\",\"[\"\"Address\"\"]\"%n");
    private static final String EXPECTED_QUERY_QUOTES_NEEDED = String.format("a.name,a.city,a.street,labels(a)%n" +
            "Andrea,Milano,\"Via Garibaldi, 7\",\"[\"Address1\",\"Address\"]\"%n" +
            "Bar Sport,,,\"[\"Address\"]\"%n" +
            ",,via Benni,\"[\"Address\"]\"%n");
    private static final String EXPECTED_BODY = "\"0\",\":User:User1\",\"42\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"true\",\"foo\",\"\",,,%n" +
            "\"1\",\":User\",\"42\",\"\",\"\",\"\",\"bar\",\"\",,,%n" +
            "\"2\",\":User\",\"12\",\"\",\"\",\"\",\"\",\"\",,,%n" +
            "\"3\",\":Address:Address1\",\"\",\"Milano\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,%n" +
            "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,%n" +
            "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,%n" +
            ",,,,,,,,\"0\",\"1\",\"KNOWS\"%n" +
            ",,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\"%n";
    private static final String EXPECTED_WITH_USE_TYPES = String.format("\"_id:id\",\"_labels:label\",\"age:long\",\"city\",\"kids\",\"male:boolean\",\"name\",\"street\",\"_start:id\",\"_end:id\",\"_type:label\"%n" +
            EXPECTED_BODY);
    private static final String EXPECTED = String.format("\"_id\",\"_labels\",\"age\",\"city\",\"kids\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\"%n" +
            EXPECTED_BODY);

    private static final String EXP_SAMPLE_BODY = "\"0\",\":User:User1\",\"\",\"42\",\"\",\"\",\"\",\"[\"\"a\"\",\"\"b\"\",\"\"c\"\"]\",\"\",\"true\",\"foo\",\"\",,,,,\n" +
            "\"1\",\":User\",\"\",\"42\",\"\",\"\",\"\",\"\",\"\",\"\",\"bar\",\"\",,,,,\n" +
            "\"2\",\":User\",\"\",\"12\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",,,,,\n" +
            "\"3\",\":Address:Address1\",\"\",\"\",\"\",\"Milano\",\"\",\"\",\"\",\"\",\"Andrea\",\"Via Garibaldi, 7\",,,,,\n" +
            "\"4\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"Bar Sport\",\"\",,,,,\n" +
            "\"5\",\":Address\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"via Benni\",,,,,\n" +
            "\"6\",\":Sample:User\",\"\",\"\",\"\",\"\",\"\",\"\",\"Galilei\",\"\",\"\",\"\",,,,,\n" +
            "\"7\",\":Sample:User\",\"Universe\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",,,,,\n" +
            "\"8\",\":Sample:User\",\"\",\"\",\"\",\"\",\"bar\",\"\",\"\",\"\",\"\",\"\",,,,,\n" +
            "\"9\",\":Sample:User\",\"\",\"\",\"baa\",\"\",\"true\",\"\",\"\",\"\",\"\",\"\",,,,,\n" +
            ",,,,,,,,,,,,\"0\",\"1\",\"KNOWS\",\"\",\"\"\n" +
            ",,,,,,,,,,,,\"3\",\"4\",\"NEXT_DELIVERY\",\"\",\"\"\n" +
            ",,,,,,,,,,,,\"8\",\"9\",\"KNOWS\",\"two\",\"four\"\n";
    private static final String EXP_SAMPLE_WITH_USE_TYPES = "\"_id:id\",\"_labels:label\",\"address\",\"age:long\",\"baz\",\"city\",\"foo\",\"kids\",\"last:Name\",\"male:boolean\",\"name\",\"street\",\"_start:id\",\"_end:id\",\"_type:label\",\"one\",\"three\"\n" +
            EXP_SAMPLE_BODY;
    private static final String EXP_SAMPLE = "\"_id\",\"_labels\",\"address\",\"age\",\"baz\",\"city\",\"foo\",\"kids\",\"last:Name\",\"male\",\"name\",\"street\",\"_start\",\"_end\",\"_type\",\"one\",\"three\"\n" +
            EXP_SAMPLE_BODY;

    private static final String EXPECTED_NONE_QUOTES_BODY = "0,:User:User1,42,,[\"a\",\"b\",\"c\"],true,foo,,,,%n" +
            "1,:User,42,,,,bar,,,,%n" +
            "2,:User,12,,,,,,,,%n" +
            "3,:Address:Address1,,Milano,,,Andrea,Via Garibaldi, 7,,,%n" +
            "4,:Address,,,,,Bar Sport,,,,%n" +
            "5,:Address,,,,,,via Benni,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,3,4,NEXT_DELIVERY%n";
    private static final String EXPECTED_NONE_QUOTES = String.format("_id,_labels,age,city,kids,male,name,street,_start,_end,_type%n" +
            EXPECTED_NONE_QUOTES_BODY);
    private static final String EXPECTED_NONE_QUOTES_WITH_USE_TYPES = String.format("_id:id,_labels:label,age:long,city,kids,male:boolean,name,street,_start:id,_end:id,_type:label%n" +
            EXPECTED_NONE_QUOTES_BODY);
    public static final String EXPECTED_NEEDED_QUOTES_BODY = "0,:User:User1,42,,\"[\"a\",\"b\",\"c\"]\",true,foo,,,,%n" +
            "1,:User,42,,,,bar,,,,%n" +
            "2,:User,12,,,,,,,,%n" +
            "3,:Address:Address1,,Milano,,,Andrea,\"Via Garibaldi, 7\",,,%n" +
            "4,:Address,,,,,Bar Sport,,,,%n" +
            "5,:Address,,,,,,via Benni,,,%n" +
            ",,,,,,,,0,1,KNOWS%n" +
            ",,,,,,,,3,4,NEXT_DELIVERY%n";
    private static final String EXPECTED_NEEDED_QUOTES = String.format("_id,_labels,age,city,kids,male,name,street,_start,_end,_type%n" +
            EXPECTED_NEEDED_QUOTES_BODY);
    private static final String EXPECTED_NEEDED_QUOTES_WITH_USE_TYPES = String.format("_id:id,_labels:label,age:long,city,kids,male:boolean,name,street,_start:id,_end:id,_type:label%n" +
            EXPECTED_NEEDED_QUOTES_BODY);

    public static final File directory = new File("target/import");
    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class, Meta.class, ImportCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    public static String readFile(String fileName) {
        return readFile(fileName, UTF_8, CompressionAlgo.NONE);
    }
    
    public static String readFile(String fileName, Charset charset, CompressionAlgo compression) {
        return BinaryTestUtil.readFileToString(new File(directory, fileName), charset, compression);
    }

    @Test
    public void testExportInvalidQuoteValue() {
        try {
            String fileName = "all.csv";
            TestUtil.testCall(db, "CALL apoc.export.csv.all($file,{quotes: 'Invalid'})",
                    map("file", fileName),
                    (r) -> assertResults(fileName, r, "database"));
            fail();
        } catch (RuntimeException e) {
            final String expectedMessage = "Failed to invoke procedure `apoc.export.csv.all`: Caused by: java.lang.RuntimeException: The string value of the field quote is not valid";
            assertEquals(expectedMessage, e.getMessage());
        }
    }
    
    @Test
    public void testExportAllCsvCompressed() {
        final CompressionAlgo compressionAlgo = DEFLATE;
        String fileName = "all.csv.zz";
        TestUtil.testCall(db, CALL_ALL,
                map("file", fileName, "config", map("compression", compressionAlgo.name())),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED, readFile(fileName, UTF_8, compressionAlgo));
    }
    
    @Test
    public void testCsvRoundTrip() {
        db.executeTransactionally("CREATE (f:Roundtrip {name:'foo',age:42,male:true,kids:['a','b','c']}),(b:Roundtrip {name:'bar',age:42}),(c:Roundtrip {age:12})");
        
        String fileName = "separatedFiles.csv.gzip";
        final Map<String, Object> params = map("file", fileName, "query", "MATCH (u:Roundtrip) return u.name as name", 
                "config", map(CompressionConfig.COMPRESSION, GZIP.name()));
        TestUtil.testCall(db, CALL_QUERY, params,
                (r) -> assertEquals(fileName, r.get("file")));

        final String deleteQuery = "MATCH (n:Roundtrip) DETACH DELETE n";
        db.executeTransactionally(deleteQuery);

        TestUtil.testCall(db, "CALL apoc.import.csv([{fileName: $file, labels: ['Roundtrip']}], [], $config) ", params, 
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (n:Roundtrip) return n.name as name", r -> {
            final Set<String> actual = Iterators.asSet(r.columnAs("name"));
            assertEquals(Set.of("foo", "bar", ""), actual);
        });

        db.executeTransactionally(deleteQuery);
    }

    @Test
    public void testExportAllCsv() {
        String fileName = "all.csv";
        testExportCsvAllCommon(fileName);
    }

    @Test
    public void testExportAllCsvWithDotInName() {
        String fileName = "all.with.dot.filename.csv";
        testExportCsvAllCommon(fileName);
    }

    @Test
    public void testExportAllCsvWithoutExtension() {
        String fileName = "all";
        testExportCsvAllCommon(fileName);
    }

    private void testExportCsvAllCommon(String fileName) {
        TestUtil.testCall(db, "CALL apoc.export.csv.all($file,null)", map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithSample() throws IOException {
        // quotes: 'none' to simplify header testing
        Map<String, Object> configWithSample = Map.of("sampling", true,
                "samplingConfig", Map.of("sample", 1L),
                "quotes", NONE_QUOTES
        );
        List<String> headers = List.of("_id", "_labels", "_start", "_end", "_type");

        testExportSampleCommon(Map.of(), EXP_SAMPLE,
                configWithSample, headers);
    }

    @Test
    public void testExportAllCsvWithSampleAndUseTypes() throws IOException {
        Map<String, Object> configWithoutSample = Map.of("useTypes", true);

        // quotes: 'none' to simplify header testing
        Map<String, Object> configWithSample = Map.of("sampling", true,
                "samplingConfig", Map.of("sample", 1L),
                "quotes", NONE_QUOTES,
                "useTypes", true);

        List<String> headers = List.of("_id:id", "_labels:label", "_start:id", "_end:id", "_type:label");

        testExportSampleCommon(configWithoutSample, EXP_SAMPLE_WITH_USE_TYPES,
                configWithSample, headers);
    }

    private static void testExportSampleCommon(Map<String, Object> configWithoutSample, String expSample, Map<String, Object> configWithSample, List<String> headers) throws IOException {
        db.executeTransactionally("CREATE (:User:Sample {`last:Name`:'Galilei'}), (:User:Sample {address:'Universe'}),\n" +
                "(:User:Sample {foo:'bar'})-[:KNOWS {one: 'two', three: 'four'}]->(:User:Sample {baz:'baa', foo: true})");
        String fileName = "all.csv";
        final long totalNodes = 10L;
        final long totalRels = 3L;
        final long totalProps = 19L;
        TestUtil.testCall(db, CALL_ALL, map("file", fileName, "config", configWithoutSample),
                (r) -> assertResults(fileName, r, "database", totalNodes, totalRels, totalProps, true));
        assertEquals(expSample, readFile(fileName));

        // check that totalNodes, totalRels and totalProps are consistent with non-sample export
        TestUtil.testCall(db, CALL_ALL, map("file", fileName, "config", configWithSample),
                (r) -> assertResults(fileName, r, "database", totalNodes, totalRels, totalProps, false));

        final String[] s = Files.lines(new File(directory, fileName).toPath()).findFirst().get().split(",");
        assertTrue(s.length < 17);
        assertTrue(Arrays.asList(s).containsAll(headers));

        db.executeTransactionally("MATCH (n:Sample) DETACH DELETE n");
    }

    @Test
    public void testExportAllCsvWithQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(db, "CALL apoc.export.csv.all($file,{quotes: true})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithQuotesAndUseTypes() {
        String fileName = "all.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", true);
        TestUtil.testCall(db, CALL_ALL,
                map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportAllCsvWithoutQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(db, "CALL apoc.export.csv.all($file,{quotes: 'none'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }


    @Test
    public void testExportAllCsvWithoutQuotesAndUseTypes() {
        String fileName = "all.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", NONE_QUOTES);
        TestUtil.testCall(db, CALL_ALL,
                map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NONE_QUOTES_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportAllCsvNeededQuotes() {
        String fileName = "all.csv";
        TestUtil.testCall(db, "CALL apoc.export.csv.all($file,{quotes: 'ifNeeded'})",
                map("file", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NEEDED_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportAllCsvNeededQuotesAndUseTypes() {
        String fileName = "all.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", IF_NEEDED_QUUOTES);
        TestUtil.testCall(db, CALL_ALL,
                map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NEEDED_QUOTES_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportGraphCsv() {
        String fileName = "graph.csv";
        Map<String, Object> config = Map.of("quotes", NONE_QUOTES);
        TestUtil.testCall(db, CALL_GRAPH, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportGraphCsvAndUseTypes() {
        String fileName = "graph.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", NONE_QUOTES);
        TestUtil.testCall(db, CALL_GRAPH, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NONE_QUOTES_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportGraphCsvWithoutQuotes() {
        String fileName = "graph.csv";
        TestUtil.testCall(db, CALL_GRAPH, map("file", fileName, "config", Map.of()),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED, readFile(fileName));
    }
    @Test
    public void testExportGraphCsvWithUseTypesAndWithoutQuotes() {
        String fileName = "graph.csv";
        Map<String, Object> config = Map.of("useTypes", true);
        TestUtil.testCall(db, CALL_GRAPH, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportCsvData() {
        String fileName = "data.csv";
        Map<String, Object> config = Map.of("quotes", NONE_QUOTES);
        TestUtil.testCall(db, CALL_DATA, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "data"));
        assertEquals(EXPECTED_NONE_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportCsvDataWithUseTypes() {
        String fileName = "data.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", NONE_QUOTES);
        TestUtil.testCall(db, CALL_DATA, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "data"));
        assertEquals(EXPECTED_NONE_QUOTES_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportCsvDataWithoutQuotes() {
        String fileName = "data.csv";
        TestUtil.testCall(db, CALL_DATA, map("file", fileName, "config", Map.of()),
                (r) -> assertResults(fileName, r, "data"));
        assertEquals(EXPECTED, readFile(fileName));
    }
    @Test
    public void testExportCsvDataWithUseTypesAndWithoutQuotes() {
        String fileName = "data.csv";
        Map<String, Object> config = Map.of("useTypes", true);
        TestUtil.testCall(db, CALL_DATA, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "data"));
        assertEquals(EXPECTED_WITH_USE_TYPES, readFile(fileName));
    }

    @Test
    public void testExportQueryCsv() {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName, "query", query, "config", Map.of()),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY, readFile(fileName));
    }

    @Test
    public void testExportQueryCsvWithUseTypes() {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        Map<String, Object> config = Map.of("useTypes", true);
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName, "query", query, "config", config),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY, readFile(fileName));
    }

    @Test
    public void testExportQueryCsvWithoutQuotes() {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        Map<String, Object> config = Map.of("quotes", false);
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName, "query", query, "config", config),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportQueryCsvWithUseTypesWithoutQuotes() {
        String fileName = "query.csv";
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", false);
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName, "query", query, "config", config),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(5)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, readFile(fileName));
    }

    @Test
    public void testExportCsvAdminOperationErrorMessage() {
        String filename = "test.csv";
        List<String> invalidQueries = List.of(
                "SHOW CONSTRAINTS YIELD id, name, type RETURN *",
                "SHOW INDEXES YIELD id, name, type RETURN *"
        );

        Map<String, Object> config = Map.of("quotes", false);
        for (String query : invalidQueries) {
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, CALL_QUERY,
                            map("query", query, "file", filename, "config", config),
                            (r) -> {}
                    )
            );

            assertError(e, INVALID_QUERY_MODE_ERROR, RuntimeException.class, "apoc.export.csv.query");
        }
    }

    @Test
    public void testExportQueryNodesCsv() {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) return u";
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName, "query", query, "config", Map.of()),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    @Test
    public void testExportQueryNodesCsvParams() {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        Map<String, Object> config = Map.of("params", Map.of("age", 10));
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName,"query",query, "config", config),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    @Test
    public void testExportQueryNodesCsvParamsWithUseTypes() {
        String fileName = "query_nodes.csv";
        String query = "MATCH (u:User) WHERE u.age > $age return u";
        Map<String, Object> config = Map.of("params", Map.of("age", 10),
                "useTypes", true);
        TestUtil.testCall(db, CALL_QUERY,
                map("file", fileName,"query",query, "config", config),
                (r) -> {
                    assertTrue("Should get statement",r.get("source").toString().contains("statement: cols(1)"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("csv", r.get("format"));

                });
        assertEquals(EXPECTED_QUERY_NODES, readFile(fileName));
    }

    public static void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertResults(fileName, r, source, 6L, 2L, 12L, true);
    }

    public static void assertResults(String fileName, Map<String, Object> r, final String source,
                               Long expectedNodes, Long expectedRelationships, Long expectedProperties, boolean assertPropEquality) {
        assertEquals(expectedNodes, r.get("nodes"));
        assertEquals(expectedRelationships, r.get("relationships"));
        if (assertPropEquality) {
            assertEquals(expectedProperties, r.get("properties"));
        } else {
            assertTrue((Long) r.get("properties") < expectedProperties);
        }
        final String expectedSource = source + ": nodes(" + expectedNodes + "), rels(" + expectedRelationships + ")";
        assertEquals(expectedSource, r.get("source"));
        assertCsvCommon(fileName, r);
    }

    private static void assertCsvCommon(String fileName, Map<String, Object> r) {
        assertEquals(fileName, r.get("file"));
        assertEquals("csv", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test public void testExportAllCsvStreaming() {
        String statement = "CALL apoc.export.csv.all(null,{stream:true,batchSize:2,useOptimizations:{unwindBatchSize:2}})";
        assertExportStreaming(statement, NONE, EXPECTED);
    }

    @Test public void testExportAllCsvStreamingWithUseTypes() {
        String statement = "CALL apoc.export.csv.all(null,{useTypes: true, stream:true, batchSize:2,useOptimizations:{unwindBatchSize:2}})";
        assertExportStreaming(statement, NONE, EXPECTED_WITH_USE_TYPES);
    }
    
    @Test
    public void testExportAllCsvStreamingCompressed() {
        final CompressionAlgo algo = GZIP;
        String statement = "CALL apoc.export.csv.all(null, {compression: '" + algo.name() + "',stream:true,batchSize:2,useOptimizations:{unwindBatchSize:2}})";
        assertExportStreaming(statement, algo, EXPECTED);
    }

    @Test
    public void testExportAllCsvStreamingCompressedWithUseTypes() {
        final CompressionAlgo algo = GZIP;
        String statement = "CALL apoc.export.csv.all(null, {useTypes: true, compression: '" + algo.name() + "', stream:true, batchSize:2, useOptimizations:{unwindBatchSize:2}})";
        assertExportStreaming(statement, GZIP, EXPECTED_WITH_USE_TYPES);
    }

    private void assertExportStreaming(String statement, CompressionAlgo algo, String expected) {
        StringBuilder sb=new StringBuilder();
        testResult(db, statement, (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(2L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(4L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(3L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(6L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(4L, r.get("batches"));
            assertEquals(6L, r.get("nodes"));
            assertEquals(8L, r.get("rows"));
            assertEquals(2L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("data")));
            res.close();
        });
        assertEquals(expected, sb.toString());
    }

    @Test public void testCypherCsvStreaming() {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        testResult(db, "CALL apoc.export.csv.query($query,null,{stream:true,batchSize:2, useOptimizations:{unwindBatchSize:2}})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));
        assertEquals(EXPECTED_QUERY, sb.toString());
    }

    @Test public void testCypherCsvStreamingWithoutQuotes() {
        String query = "MATCH (u:User) return u.age, u.name, u.male, u.kids, labels(u)";
        StringBuilder sb = new StringBuilder();
        testResult(db, "CALL apoc.export.csv.query($query,null,{quotes: false, stream:true,batchSize:2, useOptimizations:{unwindBatchSize:2}})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchUsers(sb));

        assertEquals(EXPECTED_QUERY_WITHOUT_QUOTES, sb.toString());
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchUsers(StringBuilder sb)
    {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(10L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data")); r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(15L, r.get("properties"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }

    @Test public void testCypherCsvStreamingWithAlwaysQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(db, "CALL apoc.export.csv.query($query,null,{quotes: 'always', stream:true,batchSize:2, useOptimizations:{unwindBatchSize:2}})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_ALWAYS, sb.toString());
    }

    @Test public void testCypherCsvStreamingWithNeededQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(db, "CALL apoc.export.csv.query($query,null,{quotes: 'ifNeeded', stream:true,batchSize:2, useOptimizations:{unwindBatchSize:2}})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NEEDED, sb.toString());
    }

    @Test public void testCypherCsvStreamingWithNoneQuotes() {
        String query = "MATCH (a:Address) return a.name, a.city, a.street, labels(a)";
        StringBuilder sb = new StringBuilder();
        testResult(db, "CALL apoc.export.csv.query($query,null,{quotes: 'none', stream:true,batchSize:2, useOptimizations:{unwindBatchSize:2}})", map("query",query),
                getAndCheckStreamingMetadataQueryMatchAddress(sb));

        assertEquals(EXPECTED_QUERY_QUOTES_NONE, sb.toString());
    }

    @Test
    public void testExportQueryCsvIssue1188() {
        String copyright = "\n" +
                "(c) 2018 Hovsepian, Albanese, et al. \"\"ASCB(r),\"\" \"\"The American Society for Cell Biology(r),\"\" and \"\"Molecular Biology of the Cell(r)\"\" are registered trademarks of The American Society for Cell Biology.\n" +
                "2018\n" +
                "\n" +
                "This article is distributed by The American Society for Cell Biology under license from the author(s). Two months after publication it is available to the public under an Attribution-Noncommercial-Share Alike 3.0 Unported Creative Commons License.\n" +
                "\n";
        String pk = "5921569";
        db.executeTransactionally("CREATE (n:Document{pk:$pk, copyright: $copyright})", map("copyright", copyright, "pk", pk));
        String query = "MATCH (n:Document{pk:'5921569'}) return n.pk as pk, n.copyright as copyright";
        TestUtil.testCall(db, "CALL apoc.export.csv.query($query, null, $config)", map("query", query,
                "config", map("stream", true)),
                (r) -> {
                    List<String[]> csv = CsvTestUtil.toCollection(r.get("data").toString());
                    assertEquals(2, csv.size());
                    assertArrayEquals(new String[]{"pk","copyright"}, csv.get(0));
                    assertArrayEquals(new String[]{"5921569",copyright}, csv.get(1));
                });
        db.executeTransactionally("MATCH (d:Document) DETACH DELETE d");

    }

    @Test
    public void testExportWgsPoint() {
        db.executeTransactionally("CREATE (p:Position {place: point({latitude: 12.78, longitude: 56.7, height: 1.1})})");

        TestUtil.testCall(db, "CALL apoc.export.csv.query($query, null, {quotes: 'none', stream: true}) YIELD data RETURN data",
                map("query", "MATCH (p:Position) RETURN p.place as place"),
                (r) -> {
                    String data = (String) r.get("data");
                    Map<String, Object> place = Util.fromJson(data.split(System.lineSeparator())[1], Map.class);
                    assertEquals(12.78D, (double) place.get("latitude"), 0);
                    assertEquals(56.7D, (double) place.get("longitude"), 0);
                    assertEquals(1.1D, (double) place.get("height"), 0);
                });
        db.executeTransactionally("MATCH (n:Position) DETACH DELETE n");
    }

    private Consumer<Result> getAndCheckStreamingMetadataQueryMatchAddress(StringBuilder sb)
    {
        return (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(2L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(8L, r.get("properties"));
            assertNull("Should get file", r.get("file"));
            assertEquals("csv", r.get("format"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
            r = res.next();
            assertEquals(2L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(0L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(12L, r.get("properties"));
            assertTrue("Should get time greater than 0",
                    ((long) r.get("time")) >= 0);
            sb.append(r.get("data"));
        };
    }

}
