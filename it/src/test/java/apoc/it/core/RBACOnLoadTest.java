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
package apoc.it.core;

import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testCallEmpty;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class RBACOnLoadTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Driver testUserDriver;
    private static Session session;
    private static Session testUserSession;
    private static final String user = "testUser";
    private static final String userP = "password1234";

    @BeforeClass
    public static void beforeClass() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig("dbms.memory.heap.max_size", "1GB");
        neo4jContainer.start();

        assertTrue(neo4jContainer.isRunning());
        session = neo4jContainer.getSession();

        setupUser();
        testUserDriver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.basic(user, userP));
        testUserDriver.verifyConnectivity();
        testUserSession = testUserDriver.session();
    }

    private static void setupUser() {
        List<String> queries = List.of(
                "CREATE ROLE test",
                "CREATE USER " + user + " SET PASSWORD '" + userP + "' SET PASSWORD CHANGE NOT REQUIRED",
                "GRANT ROLE test, reader, editor TO testUser");
        for (String query : queries) testCallEmpty(session, query, emptyMap());
    }

    private static void addRBACOnLoad(String urlString) throws MalformedURLException, UnknownHostException {
        InetAddress address = InetAddress.getByName(new URL(urlString).getHost());
        String ip = address.getHostAddress();
        String query = "DENY LOAD ON CIDR \"" + ip + "/32\" TO test";

        testCallEmpty(session, query, emptyMap());
    }

    @Test
    public void testRBACOnDeny() throws IOException {
        String url = "https://neo4j.com/docs/cypher-refcard/3.3/csv/artists.csv";
        addRBACOnLoad(url);

        List<String> loadableAPOCProcs = List.of(
                "apoc.load.json($url)",
                "apoc.load.jsonArray($url)",
                "apoc.load.jsonParams($url, null, null, null, {})",
                "apoc.load.xml($url)",
                "apoc.load.arrow($url)",
                "apoc.import.csv([{fileName: $url, labels: ['Person']}], [], {})",
                "apoc.import.graphml($url, {})",
                "apoc.import.json($url)",
                "apoc.import.xml($url)");

        for (String loadableAPOCProc : loadableAPOCProcs) {
            RuntimeException e = assertThrows(
                    RuntimeException.class,
                    () -> testCall(testUserSession, "CALL " + loadableAPOCProc, Map.of("url", url), r -> {}));

            Assertions.assertThat(e.getMessage()).contains("URLAccessValidationError");
            Assertions.assertThat(e.getMessage())
                    .contains(
                            "Cause: LOAD on URL '" + url
                                    + "' is denied for user 'testUser' with roles [PUBLIC, editor, reader, test] restricted to");
        }
    }

    @Test
    public void testRBACOnDenyGeocode() throws IOException {
        String url = "http://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY";
        String reverseUrl = "http://api.opencagedata.com/geocode/v1/json?q=LAT+LNG&key=KEY";
        addRBACOnLoad(url);

        final Map<String, Object> config =
                map("provider", "opencage", "url", url, "reverseUrl", reverseUrl, "key", "myOwnMockKey");

        List<String> loadableAPOCProcs = List.of(
                "CALL apoc.spatial.geocode('FRANCE', 1, true, $config)",
                "CALL apoc.spatial.reverseGeocode($lat, $long, false, $config)",
                "CALL apoc.spatial.geocodeOnce('FRANCE', $config)");

        for (String loadableAPOCProc : loadableAPOCProcs) {
            RuntimeException e = assertThrows(
                    RuntimeException.class,
                    () -> testCall(
                            testUserSession,
                            loadableAPOCProc,
                            Map.of("config", config, "lat", 48.8582532, "long", 2.294287),
                            r -> {}));

            String urlEnd = loadableAPOCProc.contains("reverseGeocode")
                    ? "q=48.8582532+2.294287&key=myOwnMockKey"
                    : "q=FRANCE&key=myOwnMockKey";
            String erroredUrl = "http://api.opencagedata.com/geocode/v1/json?" + urlEnd;
            Assertions.assertThat(e.getMessage()).contains("URLAccessValidationError");
            Assertions.assertThat(e.getMessage())
                    .contains(
                            "Cause: LOAD on URL '" + erroredUrl
                                    + "' is denied for user 'testUser' with roles [PUBLIC, editor, reader, test] restricted to");
        }
    }

    @AfterClass
    public static void afterClass() {
        testUserSession.close();
        testUserDriver.close();
        session.close();
        neo4jContainer.close();
    }
}
