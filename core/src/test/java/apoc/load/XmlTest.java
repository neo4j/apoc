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
package apoc.load;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import apoc.util.CompressionAlgo;
import apoc.util.TestUtil;
import apoc.util.TransactionTestUtil;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import apoc.xml.XmlTestUtils;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class XmlTest {
    public static final String FILE_SHORTENED =
            "src/test/resources/xml/humboldt_soemmering01_1791.TEI-P5-shortened.xml";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    private HttpServer server;

    @Before
    public void setUp() throws IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        TestUtil.registerProcedure(db, Xml.class);

        server = HttpServer.create(new InetSocketAddress(6363), 0);
        HttpContext staticContext = server.createContext("/");
        staticContext.setHandler(new SimpleHttpHandler());
        server.start();
    }

    @After
    public void cleanup() {
        server.stop(0);
        db.shutdown();
    }

    @Test
    public void testLoadXml() {
        testCall(
                db,
                "CALL apoc.load.xml('file:databases.xml')",
                (row) -> assertEquals(XmlTestUtils.XML_AS_NESTED_MAP, row.get("value")));
    }

    @Test
    public void testTerminateLoadXml() {
        final String file = ClassLoader.getSystemResource("largeFile.graphml").toString();
        TransactionTestUtil.checkTerminationGuard(db, "CALL apoc.load.xml($file)", Map.of("file", file));
    }

    @Test
    public void testLoadXmlAsStream() {
        testResult(db, "CALL apoc.load.xml('file:databases.xml', '/parent/child')", (res) -> {
            final ResourceIterator<Map<String, Object>> value = res.columnAs("value");
            final Map<String, String> expectedFirstRow =
                    Map.of("_type", "child", "name", "Neo4j", "_text", "Neo4j is a graph database");
            final Map<String, Object> expectedSecondRow = Map.of(
                    "_type",
                    "child",
                    "name",
                    "relational",
                    "_children",
                    List.of(
                            Map.of("_type", "grandchild", "name", "MySQL", "_text", "MySQL is a database & relational"),
                            Map.of(
                                    "_type",
                                    "grandchild",
                                    "name",
                                    "Postgres",
                                    "_text",
                                    "Postgres is a relational database")));
            Map<String, Object> next = value.next();
            assertEquals(expectedFirstRow, next);
            next = value.next();
            assertEquals(expectedSecondRow, next);
            assertFalse(value.hasNext());
        });
    }

    @Test
    public void testMixedContent() {
        testCall(
                db,
                "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/mixedcontent.xml") + "')",
                this::commonAssertionsMixedContent);
    }

    @Test
    public void testMixedContentWithBinary() {
        testCall(
                db,
                "CALL apoc.load.xml($data, null, $config)",
                map(
                        "data",
                        fileToBinary(
                                new File("src/test/resources/xml/mixedcontent.xml"), CompressionAlgo.BLOCK_LZ4.name()),
                        "config",
                        map(COMPRESSION, CompressionAlgo.BLOCK_LZ4.name())),
                this::commonAssertionsMixedContent);
    }

    private void commonAssertionsMixedContent(Map<String, Object> row) {
        assertEquals(
                map(
                        "_type",
                        "root",
                        "_children",
                        asList(
                                map("_type", "text", "_children", asList(map("_type", "mixed"), "text0", "text1")),
                                map("_type", "text", "_text", "text as cdata"))),
                row.get("value"));
    }

    @Test
    public void testBookIds() {
        testResult(db, String.format("""
                        CALL apoc.load.xml('%s')
                        YIELD value AS catalog
                        UNWIND catalog._children AS book
                        RETURN book.id AS id
                        """, TestUtil.getUrlFileName("xml/books.xml")), result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testFilterIntoCollection() {
        testResult(
                db,
                String.format("""
                        CALL apoc.load.xml('%s')
                        YIELD value AS catalog
                        UNWIND catalog._children AS book
                        RETURN
                            book.id,
                            [attr IN book._children WHERE attr._type IN ['author','title'] | [attr._type, attr._text]] AS pairs
                        """, TestUtil.getUrlFileName("xml/books.xml")),
                result -> assertEquals("""
                        +----------------------------------------------------------------------------------------------------------------+
                        | book.id | pairs                                                                                                |
                        +----------------------------------------------------------------------------------------------------------------+
                        | "bk101" | [["author","Gambardella, Matthew"],["author","Arciniegas, Fabio"],["title","XML Developer's Guide"]] |
                        | "bk102" | [["author","Ralls, Kim"],["title","Midnight Rain"]]                                                  |
                        | "bk103" | [["author","Corets, Eva"],["title","Maeve Ascendant"]]                                               |
                        | "bk104" | [["author","Corets, Eva"],["title","Oberon's Legacy"]]                                               |
                        | "bk105" | [["author","Corets, Eva"],["title","The Sundered Grail"]]                                            |
                        | "bk106" | [["author","Randall, Cynthia"],["title","Lover Birds"]]                                              |
                        | "bk107" | [["author","Thurman, Paula"],["title","Splish Splash"]]                                              |
                        | "bk108" | [["author","Knorr, Stefan"],["title","Creepy Crawlies"]]                                             |
                        | "bk109" | [["author","Kress, Peter"],["title","Paradox Lost"]]                                                 |
                        | "bk110" | [["author","O'Brien, Tim"],["title","Microsoft .NET: The Programming Bible"]]                        |
                        | "bk111" | [["author","O'Brien, Tim"],["title","MSXML3: A Comprehensive Guide"]]                                |
                        | "bk112" | [["author","Galos, Mike"],["title","Visual Studio 7: A Comprehensive Guide"]]                        |
                        +----------------------------------------------------------------------------------------------------------------+
                        12 rows
                        """, result.resultAsString()));
    }

    @Test
    public void testReturnCollectionElements() {
        testResult(
                db,
                String.format("""
                CALL apoc.load.xml('%s')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN
                    book.id AS id,
                    [x IN book._children WHERE x._type = 'author' | x._text] AS authors,
                    [x IN book._children WHERE x._type = 'title' | x._text] AS title
                """, TestUtil.getUrlFileName("xml/books.xml")),
                result -> assertEquals("""
                        +-----------------------------------------------------------------------------------------------------+
                        | id      | authors                                      | title                                      |
                        +-----------------------------------------------------------------------------------------------------+
                        | "bk101" | ["Gambardella, Matthew","Arciniegas, Fabio"] | ["XML Developer's Guide"]                  |
                        | "bk102" | ["Ralls, Kim"]                               | ["Midnight Rain"]                          |
                        | "bk103" | ["Corets, Eva"]                              | ["Maeve Ascendant"]                        |
                        | "bk104" | ["Corets, Eva"]                              | ["Oberon's Legacy"]                        |
                        | "bk105" | ["Corets, Eva"]                              | ["The Sundered Grail"]                     |
                        | "bk106" | ["Randall, Cynthia"]                         | ["Lover Birds"]                            |
                        | "bk107" | ["Thurman, Paula"]                           | ["Splish Splash"]                          |
                        | "bk108" | ["Knorr, Stefan"]                            | ["Creepy Crawlies"]                        |
                        | "bk109" | ["Kress, Peter"]                             | ["Paradox Lost"]                           |
                        | "bk110" | ["O'Brien, Tim"]                             | ["Microsoft .NET: The Programming Bible"]  |
                        | "bk111" | ["O'Brien, Tim"]                             | ["MSXML3: A Comprehensive Guide"]          |
                        | "bk112" | ["Galos, Mike"]                              | ["Visual Studio 7: A Comprehensive Guide"] |
                        +-----------------------------------------------------------------------------------------------------+
                        12 rows
                        """, result.resultAsString()));
    }

    @Test
    public void testLoadXmlXpathAuthorFromBookId() {
        testCall(db, String.format("""
                        CALL apoc.load.xml('%s', '/catalog/book[@id="bk102"]/author')
                        YIELD value AS result
                        """, TestUtil.getUrlFileName("xml/books.xml")), (r) -> {
            assertEquals("author", ((Map) r.get("result")).get("_type"));
            assertEquals("Ralls, Kim", ((Map) r.get("result")).get("_text"));
        });
    }

    @Test
    public void testLoadXmlXpathGenreFromBookTitle() {
        testCall(db, String.format("""
                        CALL apoc.load.xml('%s', '/catalog/book[title="Maeve Ascendant"]/genre')
                        YIELD value AS result
                        """, TestUtil.getUrlFileName("xml/books.xml")), (r) -> {
            assertEquals("genre", ((Map) r.get("result")).get("_type"));
            assertEquals("Fantasy", ((Map) r.get("result")).get("_text"));
        });
    }

    @Test
    public void testLoadXmlXpathReturnBookFromBookTitle() {
        testCall(db, String.format("""
                        CALL apoc.load.xml('%s', '/catalog/book[title="Maeve Ascendant"]/.')
                        YIELD value AS result
                        """, TestUtil.getUrlFileName("xml/books.xml")), (r) -> {
            Object value = Iterables.single(r.values());
            assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }

    @Test
    public void testLoadXmlXpathBooKsFromGenre() {
        testResult(db, String.format("""
                        CALL apoc.load.xml('%s', '/catalog/book[genre="Computer"]')
                        YIELD value AS result
                        """, TestUtil.getUrlFileName("xml/books.xml")), (r) -> {
            Map<String, Object> next = r.next();
            Object result = next.get("result");
            Map resultMap = (Map) next.get("result");
            Object children = resultMap.get("_children");

            List<Object> childrenList = (List<Object>) children;
            assertEquals("bk101", ((Map) result).get("id"));
            assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
            assertEquals("Gambardella, Matthew", ((Map) childrenList.get(0)).get("_text"));
            assertEquals("author", ((Map) childrenList.get(1)).get("_type"));
            assertEquals("Arciniegas, Fabio", ((Map) childrenList.get(1)).get("_text"));
            next = r.next();
            result = next.get("result");
            resultMap = (Map) next.get("result");
            children = resultMap.get("_children");
            childrenList = (List<Object>) children;
            assertEquals("bk110", ((Map) result).get("id"));
            assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
            assertEquals("O'Brien, Tim", ((Map) childrenList.get(0)).get("_text"));
            assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
            assertEquals("Microsoft .NET: The Programming Bible", ((Map) childrenList.get(1)).get("_text"));
            next = r.next();
            result = next.get("result");
            resultMap = (Map) next.get("result");
            children = resultMap.get("_children");
            childrenList = (List<Object>) children;
            assertEquals("bk111", ((Map) result).get("id"));
            assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
            assertEquals("O'Brien, Tim", ((Map) childrenList.get(0)).get("_text"));
            assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
            assertEquals("MSXML3: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
            next = r.next();
            result = next.get("result");
            resultMap = (Map) next.get("result");
            children = resultMap.get("_children");
            childrenList = (List<Object>) children;
            assertEquals("bk112", ((Map) result).get("id"));
            assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
            assertEquals("Galos, Mike", ((Map) childrenList.get(0)).get("_text"));
            assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
            assertEquals("Visual Studio 7: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testLoadXmlNoFailOnError() {
        testCall(
                db,
                "CALL apoc.load.xml('file:src/test/resources/books.xm', '', {failOnError:false}) YIELD value AS result",
                (r) -> {
                    Map resultMap = (Map) r.get("result");
                    assertEquals(Collections.emptyMap(), resultMap);
                });
    }

    @Test
    public void testLoadXmlWithNextWordRels() {
        assertThrows(
                "usage of `createNextWordRelationships` is no longer allowed. Use `{relType:'NEXT_WORD', label:'XmlWord'}` instead.",
                QueryExecutionException.class,
                () -> db.executeTransactionally("call apoc.import.xml('file:" + FILE_SHORTENED + "', "
                        + "{createNextWordRelationships: true, filterLeadingWhitespace: true}) yield node"));
    }

    @Test
    public void testLoadXmlWithNextWordRelsWithBinaryFile() {
        final String query = "CALL apoc.import.xml($data, $config) YIELD node";
        final String compression = CompressionAlgo.GZIP.name();
        final Map<String, Object> config = Map.of(
                "data",
                fileToBinary(new File(FILE_SHORTENED), compression),
                "config",
                Map.of(
                        "compression",
                        compression,
                        "relType",
                        "NEXT_WORD",
                        "label",
                        "XmlWord",
                        "filterLeadingWhitespace",
                        true));
        commonAssertionsWithNextWordRels(query, config);
    }

    @Test
    public void testLoadXmlWithNextWordRelsWithNewConfigOptions() {
        final String query = "CALL apoc.import.xml('file:" + FILE_SHORTENED + "', "
                + "{relType: 'NEXT_WORD', label: 'XmlWord', filterLeadingWhitespace: true}) YIELD node";
        commonAssertionsWithNextWordRels(query, Collections.emptyMap());
    }

    private void commonAssertionsWithNextWordRels(String query, Map<String, Object> config) {
        testCall(db, query, config, row -> assertNotNull(row.get("node")));
        testResult(db, "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count", result -> {
            final Map<String, Long> resultMap =
                    result.stream().collect(Collectors.toMap(o -> (String) o.get("label"), o -> (Long) o.get("count")));
            assertEquals(2L, (long) resultMap.get("XmlProcessingInstruction"));
            assertEquals(1L, (long) resultMap.get("XmlDocument"));
            assertEquals(369L, (long) resultMap.get("XmlWord"));
            assertEquals(158L, (long) resultMap.get("XmlTag"));
        });

        // no node more than one NEXT/NEXT_SIBLING
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)-[:NEXT]->()  | p ]) > 1 RETURN n", null);
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)-[:NEXT_SIBLING]->()  | p ]) > 1 RETURN n", null);

        // no node more than one IS_FIRST_CHILD / IS_LAST_CHILD
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)<-[:FIRST_CHILD_OF]-()  | p ]) > 1 RETURN n", null);
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)<-[:LAST_CHILD_OF]-()  | p ]) > 1 RETURN n", null);

        // NEXT_WORD relationship do connect all word nodes
        testResult(
                db,
                "MATCH p=(:XmlDocument)-[:NEXT_WORD*]->(e:XmlWord) WHERE NOT (e)-[:NEXT_WORD]->() RETURN length(p) AS len",
                result -> {
                    Map<String, Object> r = Iterators.single(result);
                    assertEquals(369L, r.get("len"));
                });
    }

    @Test
    public void testLoadXmlWithNextEntityRels() {
        testCall(db, String.format("""
                        CALL apoc.import.xml('file:%s', {connectCharacters: true, filterLeadingWhitespace: true})
                        YIELD node
                        """, FILE_SHORTENED), row -> assertNotNull(row.get("node")));
        testResult(db, "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count", result -> {
            final Map<String, Long> resultMap =
                    result.stream().collect(Collectors.toMap(o -> (String) o.get("label"), o -> (Long) o.get("count")));
            assertEquals(2L, (long) resultMap.get("XmlProcessingInstruction"));
            assertEquals(1L, (long) resultMap.get("XmlDocument"));
            assertEquals(369L, (long) resultMap.get("XmlCharacters"));
            assertEquals(158L, (long) resultMap.get("XmlTag"));
        });

        // no node more than one NEXT/NEXT_SIBLING
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)-[:NEXT]->()  | p ]) > 1 RETURN n", null);
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)-[:NEXT_SIBLING]->()  | p ]) > 1 RETURN n", null);

        // no node more than one IS_FIRST_CHILD / IS_LAST_CHILD
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)<-[:FIRST_CHILD_OF]-()  | p ]) > 1 RETURN n", null);
        testCallEmpty(db, "MATCH (n) WHERE size([p =  (n)<-[:LAST_CHILD_OF]-()  | p ]) > 1 RETURN n", null);

        // NEXT_WORD relationship do connect all word nodes
        testResult(
                db,
                "match p=(:XmlDocument)-[:NE*]->(e:XmlCharacters) where not (e)-[:NE]->() return length(p) as len",
                result -> {
                    Map<String, Object> r = Iterators.single(result);
                    assertEquals(369L, r.get("len"));
                });
    }

    @Test
    public void testLoadXmlFromZip() {
        testResult(db, """
                CALL apoc.load.xml('file:src/test/resources/testload.zip!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromTar() {
        testResult(db, """
                CALL apoc.load.xml('file:src/test/resources/testload.tar!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromTarGz() {
        testResult(db, """
                CALL apoc.load.xml('file:src/test/resources/testload.tar.gz!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromTgz() {
        testResult(db, """
                CALL apoc.load.xml('file:src/test/resources/testload.tgz!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromZipByUrl() {
        testResult(db, """
                CALL apoc.load.xml('http://localhost:6363/testload.zip?raw=true!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromTarByUrl() {
        testResult(db, """
                CALL apoc.load.xml('http://localhost:6363/testload.tar.gz?raw=true!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromTarGzByUrl() {
        testResult(db, """
                CALL apoc.load.xml('http://localhost:6363/testload.tar.gz?raw=true!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlFromTgzByUrl() {
        testResult(db, """
                CALL apoc.load.xml('http://localhost:6363/testload.tgz?raw=true!xml/books.xml')
                YIELD value AS catalog
                UNWIND catalog._children AS book
                RETURN book.id AS id
                """, result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1, 12).allMatch(value -> ids.contains(String.format("bk1%02d", value))));
        });
    }

    @Test
    public void testLoadXmlSingleLineSimple() {
        testCall(
                db,
                "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/singleLine.xml") + "', '/', null, true)",
                (row) -> {
                    assertEquals(XmlTestUtils.XML_AS_SINGLE_LINE_SIMPLE, row.get("value"));
                });
    }

    @Test
    public void testLoadXmlSingleLine() {
        testCall(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/singleLine.xml") + "')", (row) -> {
            assertEquals(XmlTestUtils.XML_AS_SINGLE_LINE, row.get("value"));
        });
    }

    @Test
    public void testParse() {
        testCall(
                db,
                "WITH '<?xml version=\"1.0\"?><table><tr><td><img src=\"pix/logo-tl.gif\"></img></td></tr></table>' AS xmlString RETURN apoc.xml.parse(xmlString) AS value",
                (row) -> assertEquals(XmlTestUtils.XML_AS_SINGLE_LINE, row.get("value")));
    }

    @Test
    public void testParseWithXPath() throws Exception {
        String xmlString =
                FileUtils.readFileToString(new File("src/test/resources/xml/books.xml"), StandardCharsets.UTF_8);
        testCall(
                db,
                "RETURN apoc.xml.parse($xmlString, '/catalog/book[title=\"Maeve Ascendant\"]/.') AS result",
                map("xmlString", xmlString),
                (r) -> assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, r.get("result")));
    }

    @Test
    public void testLoadXmlPreventXXEVulnerabilityThrowsQueryExecutionException() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/xxe.xml") + "')", (r) -> {
                    r.next();
                    r.close();
                }));

        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
    }

    @Test
    public void testLoadXmlPreventBillionLaughVulnerabilityThrowsQueryExecutionException() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(
                        db, "CALL apoc.load.xml('" + TestUtil.getUrlFileName("xml/billion_laughs.xml") + "')", (r) -> {
                            r.next();
                            r.close();
                        }));

        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
    }

    @Test
    public void testXmlParsePreventXXEVulnerabilityThrowsQueryExecutionException() {
        final var xml =
                "<?xml version=\"1.0\"?><!DOCTYPE GVI [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><foo>&xxe;</foo>";

        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(db, "RETURN apoc.xml.parse('" + xml + "')", (r) -> {
                    r.next();
                    r.close();
                }));

        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
    }

    @Test
    public void testXmlParsePreventBillionLaughsVulnerabilityThrowsQueryExecutionException() {
        final var xml =
                "<?xml version=\"1.0\"?><!DOCTYPE lolz [<!ENTITY lol \"lol\"><!ENTITY lol1 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">]><foo>&lol1;</foo>";

        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(db, "RETURN apoc.xml.parse('" + xml + "')", (r) -> {
                    r.next();
                    r.close();
                }));

        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
    }

    @Test
    public void testImportXmlPreventXXEVulnerabilityThrowsQueryExecutionException() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(db, "CALL apoc.import.xml('" + TestUtil.getUrlFileName("xml/xxe.xml") + "')", (r) -> {
                    r.next();
                    r.close();
                }));

        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
    }

    @Test
    public void testImportXmlPreventBillionLaughsVulnerabilityThrowsQueryExecutionException() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(
                        db,
                        "CALL apoc.import.xml('" + TestUtil.getUrlFileName("xml/billion_laughs.xml") + "')",
                        (r) -> {
                            r.next();
                            r.close();
                        }));

        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof RuntimeException);
        assertEquals(except.getMessage(), "XML documents with a DOCTYPE are not allowed.");
    }

    @Test
    public void testTerminateImportXml() {
        final String file = ClassLoader.getSystemResource("largeFile.graphml").toString();
        TransactionTestUtil.checkTerminationGuard(db, "CALL apoc.import.xml($file)", Map.of("file", file));
    }
}
