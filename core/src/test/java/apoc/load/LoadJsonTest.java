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

import static apoc.ApocConfig.*;
import static apoc.convert.ConvertJsonTest.EXPECTED_AS_PATH_LIST;
import static apoc.convert.ConvertJsonTest.EXPECTED_PATH;
import static apoc.convert.ConvertJsonTest.EXPECTED_PATH_WITH_NULLS;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.*;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class LoadJsonTest {

    private static ClientAndServer mockServer;
    private HttpServer server;

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule().withSetting(GraphDatabaseSettings.memory_tracking, true);

    @Before
    public void setUp() throws IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        apocConfig().setProperty("apoc.json.zip.url", "http://localhost:5353/testload.zip?raw=true!person.json");
        apocConfig()
                .setProperty(
                        "apoc.json.simpleJson.url",
                        ClassLoader.getSystemResource("map.json").toString());
        TestUtil.registerProcedure(db, LoadJson.class);

        server = HttpServer.create(new InetSocketAddress(5353), 0);
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
    public void testLoadJson() {
        URL url = ClassLoader.getSystemResource("map.json");
        testCall(
                db,
                "CALL apoc.load.json($url)",
                map("url", url.toString()),
                (row) -> assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value")));
    }

    @Test
    public void testLoadMultiJsonWithBinary() {
        testResult(
                db,
                "CYPHER 5 CALL apoc.load.jsonParams($url, null, null, null, $config)",
                map(
                        "url",
                        fileToBinary(
                                new File(ClassLoader.getSystemResource("multi.json")
                                        .getPath()),
                                CompressionAlgo.FRAMED_SNAPPY.name()),
                        "config",
                        map(COMPRESSION, CompressionAlgo.FRAMED_SNAPPY.name())),
                this::commonAssertionsLoadJsonMulti);
    }

    @Test
    public void testLoadMultiJson() {
        URL url = ClassLoader.getSystemResource("multi.json");
        testResult(
                db,
                "CALL apoc.load.json($url)",
                map("url", url.toString()), // 'file:map.json' YIELD value RETURN value
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value"));
                    row = result.next();
                    assertEquals(map("bar", asList(4L, 5L, 6L)), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }

    private void commonAssertionsLoadJsonMulti(Result result) {
        Map<String, Object> row = result.next();
        assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value"));
        row = result.next();
        assertEquals(map("bar", asList(4L, 5L, 6L)), row.get("value"));
        assertFalse(result.hasNext());
    }

    @Test
    public void testLoadMultiJsonPaths() {
        URL url = ClassLoader.getSystemResource("multi.json");
        testResult(
                db,
                "CALL apoc.load.json($url,'$')",
                map("url", url.toString()), // 'file:map.json' YIELD value RETURN value
                this::commonAssertionsLoadJsonMulti);
    }

    @Test
    public void testLoadJsonPath() {
        URL url = ClassLoader.getSystemResource("map.json");
        testCall(
                db,
                "CALL apoc.load.json($url,'$.foo')",
                map("url", url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(map("result", asList(1L, 2L, 3L)), row.get("value")));
    }

    @Test
    public void testLoadJsonPathRoot() {
        URL url = ClassLoader.getSystemResource("map.json");
        testCall(
                db,
                "CALL apoc.load.json($url,'$')",
                map("url", url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value")));
    }

    @Test
    public void testLoadJsonWithPathOptions() {
        URL url = ClassLoader.getSystemResource("columns.json");

        // -- load.json
        testResult(
                db,
                "CALL apoc.load.json($url, '$..columns')",
                map("url", url.toString()),
                (res) -> assertEquals(EXPECTED_PATH_WITH_NULLS, Iterators.asList(res.columnAs("value"))));

        testResult(
                db,
                "CALL apoc.load.json($url, '$..columns', $config)",
                map("url", url.toString(), "config", map("pathOptions", Collections.emptyList())),
                (res) -> assertEquals(EXPECTED_PATH, Iterators.asList(res.columnAs("value"))));

        testResult(
                db,
                "CALL apoc.load.json($url, '$..columns', $config)",
                map("url", url.toString(), "config", map("pathOptions", List.of("AS_PATH_LIST"))),
                (res) -> assertEquals(
                        List.of(Map.of("result", EXPECTED_AS_PATH_LIST)), Iterators.asList(res.columnAs("value"))));

        // -- load.jsonArray
        testResult(
                db,
                "CALL apoc.load.jsonArray($url, '$..columns')",
                map("url", url.toString()),
                (res) -> assertEquals(EXPECTED_PATH_WITH_NULLS, Iterators.asList(res.columnAs("value"))));

        testResult(
                db,
                "CALL apoc.load.jsonArray($url, '$..columns', $config)",
                map("url", url.toString(), "config", map("pathOptions", Collections.emptyList())),
                (res) -> assertEquals(EXPECTED_PATH, Iterators.asList(res.columnAs("value"))));

        testResult(
                db,
                "CALL apoc.load.jsonArray($url, '$..columns', $config)",
                map("url", url.toString(), "config", map("pathOptions", List.of("AS_PATH_LIST"))),
                (res) -> assertEquals(List.of(EXPECTED_AS_PATH_LIST), Iterators.asList(res.columnAs("value"))));
    }

    @Test
    public void testLoadJsonArrayPath() {
        URL url = ClassLoader.getSystemResource("map.json");
        testCall(
                db,
                "CALL apoc.load.jsonArray($url,'$.foo')",
                map("url", url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(asList(1L, 2L, 3L), row.get("value")));
    }

    @Test
    public void testLoadJsonArrayPathRoot() {
        URL url = ClassLoader.getSystemResource("map.json");
        testCall(
                db,
                "CALL apoc.load.jsonArray($url,'$')",
                map("url", url.toString()), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value")));
    }

    @Test
    public void testLoadJsonStackOverflow() {
        String url =
                "https://api.stackexchange.com/2.2/questions?pagesize=10&order=desc&sort=creation&tagged=neo4j&site=stackoverflow&filter=!5-i6Zw8Y)4W7vpy91PMYsKM-k9yzEsSC1_Uxlf";
        testCall(
                db,
                "CALL apoc.load.json($url)",
                map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse(value.isEmpty());
                    List<Map<String, Object>> items = (List<Map<String, Object>>) value.get("items");
                    assertEquals(10, items.size());
                });
    }

    @Test
    public void testLoadJsonNoFailOnError() {
        String url = "file.json";
        testResult(
                db,
                "CALL apoc.load.json($url,null, {failOnError:false})",
                map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> assertFalse(row.hasNext()));
    }

    @Test
    public void testLoadJsonZip() {
        URL url = ClassLoader.getSystemResource("testload.zip");
        testCall(db, "CALL apoc.load.json($url)", map("url", url.toString() + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonTar() {
        URL url = ClassLoader.getSystemResource("testload.tar");
        testCall(db, "CALL apoc.load.json($url)", map("url", url.toString() + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonTarGz() {
        URL url = ClassLoader.getSystemResource("testload.tar.gz");
        testCall(db, "CALL apoc.load.json($url)", map("url", url.toString() + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonTgz() {
        URL url = ClassLoader.getSystemResource("testload.tgz");
        testCall(db, "CALL apoc.load.json($url)", map("url", url.toString() + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonZipByUrl() throws Exception {
        URL url = new URL("http://localhost:5353/testload.zip?raw=true");
        testCall(db, "CALL apoc.load.json($url)", map("url", url + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonTarByUrl() throws Exception {
        URL url = new URL("http://localhost:5353/testload.tar?raw=true");
        testCall(db, "CALL apoc.load.json($url)", map("url", url + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonTarGzByUrl() throws Exception {
        URL url = new URL("http://localhost:5353/testload.tar.gz?raw=true");
        testCall(db, "CALL apoc.load.json($url)", map("url", url + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonTgzByUrl() throws Exception {
        URL url = new URL("http://localhost:5353/testload.tgz?raw=true");
        testCall(db, "CALL apoc.load.json($url)", map("url", url + "!person.json"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonZipByUrlInConfigFile() {
        testCall(db, "CALL apoc.load.json($key)", map("key", "zip"), (row) -> {
            Map<String, Object> r = (Map<String, Object>) row.get("value");
            assertEquals("Michael", r.get("name"));
            assertEquals(41L, r.get("age"));
            assertEquals(asList("Selina", "Rana", "Selma"), r.get("children"));
        });
    }

    @Test
    public void testLoadJsonByUrlInConfigFile() {

        testCall(
                db,
                "CALL apoc.load.json($key)",
                map("key", "simpleJson"), // 'file:map.json' YIELD value RETURN value
                (row) -> assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value")));
    }

    @Test
    public void testLoadJsonByUrlInConfigFileWrongKey() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testResult(db, "CALL apoc.load.json($key)", map("key", "foo"), Result::hasNext));
        Throwable except = ExceptionUtils.getRootCause(e);
        assertTrue(except instanceof IOException);
        final String message = except.getMessage();
        assertTrue(message.startsWith("Cannot open file "));
        assertTrue(message.endsWith("foo for reading."));
    }

    @Test
    public void testLoadJsonWithAuth() throws Exception {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);
        Map<String, String> responseBody = Map.of("result", "message");

        new MockServerClient("localhost", 1080)
                .when(request().withPath("/docs/search").withHeader("Authorization", "Basic " + token), exactly(1))
                .respond(response()
                        .withStatusCode(200)
                        .withHeaders(new Header("Cache-Control", "private, max-age=1000"))
                        .withBody(JsonUtil.OBJECT_MAPPER.writeValueAsString(responseBody))
                        .withDelay(TimeUnit.SECONDS, 1));

        testCall(
                db,
                "call apoc.load.json($url)",
                map("url", "http://" + userPass + "@localhost:1080/docs/search"),
                (row) -> assertEquals(responseBody, row.get("value")));
    }

    @Test
    public void testLoadJsonParamsWithAuth() throws Exception {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);
        Map<String, String> responseBody = Map.of("result", "message");

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("Authorization", "Basic " + token)
                                .withHeader("Content-type", "application/json")
                                .withBody("{\"query\":\"pagecache\",\"version\":\"3.5\"}"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(200)
                        .withHeaders(
                                new Header("Content-Type", "application/json"),
                                new Header("Cache-Control", "public, max-age=86400"))
                        .withBody(JsonUtil.OBJECT_MAPPER.writeValueAsString(responseBody))
                        .withDelay(TimeUnit.SECONDS, 1));

        testCall(
                db,
                "CYPHER 5 CALL apoc.load.jsonParams($url, $config, $payload)",
                map(
                        "payload",
                        "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url",
                        "http://" + userPass + "@localhost:1080/docs/search",
                        "config",
                        map("method", "POST", "Content-Type", "application/json")),
                (row) -> assertEquals(responseBody, row.get("value")));
    }

    @Test
    public void testLoadJsonParams() {
        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(200)
                        .withHeaders(
                                new Header("Content-Type", "application/json"),
                                new Header("Cache-Control", "public, max-age=86400"))
                        .withBody("{ result: 'message' }")
                        .withDelay(TimeUnit.SECONDS, 1));

        testCall(
                db,
                "CYPHER 5 CALL apoc.load.jsonParams($url, $config, $json)",
                map(
                        "json",
                        "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url",
                        "http://localhost:1080/docs/search",
                        "config",
                        map("method", "POST", "Content-Type", "application/json")),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse("value should be not empty", value.isEmpty());
                });
    }

    @Test
    public void shouldTerminateLoadJson() {
        URL url = ClassLoader.getSystemResource("exportJSON/testTerminate.json");
        checkTerminationGuard(db, "CALL apoc.load.json($file)", Map.of("file", url.toString()));
    }
}
