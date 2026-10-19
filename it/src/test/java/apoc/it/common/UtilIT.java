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
package apoc.it.common;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import apoc.ApocConfig;
import apoc.util.Util;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.testcontainers.containers.GenericContainer;

class UtilIT {
    private GenericContainer httpServer;

    public UtilIT() throws Exception {
        googleUrl = new URL("https://www.google.com");
    }

    private GenericContainer setUpServer(String redirectURL) {
        new ApocConfig(null);
        GenericContainer httpServer = new GenericContainer("alpine")
                .withCommand(
                        "/bin/sh",
                        "-c",
                        String.format(
                                "while true; do { echo -e 'HTTP/1.1 301 Moved Permanently\\r\\nLocation: %s'; echo ; } | nc -l -p 8000; done",
                                redirectURL))
                .withExposedPorts(8000);
        httpServer.start();
        return httpServer;
    }

    @AfterEach
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop();
        }
    }

    private final URL googleUrl;

    @Test
    void redirectShouldWorkWhenProtocolNotChangesWithUrlLocation() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("https://www.google.com");

        // given
        URL url = getServerUrl(httpServer);
        when(mockChecker.checkURL(url)).thenReturn(url);
        when(mockChecker.checkURL(googleUrl)).thenReturn(googleUrl);

        // when
        String page = IOUtils.toString(
                Util.openInputStream(url.toString(), null, null, null, mockChecker), StandardCharsets.UTF_8);

        // then
        Assertions.assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    void redirectWithBlockedIPsWithUrlLocation() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);

        httpServer = setUpServer("http://127.168.0.1");
        URL url = getServerUrl(httpServer);
        when(mockChecker.checkURL(url)).thenReturn(url);
        when(mockChecker.checkURL(new URL("http://127.168.0.1"))).thenThrow(new URLAccessValidationError("no"));

        IOException e = assertThrows(
                IOException.class, () -> Util.openInputStream(url.toString(), null, null, null, mockChecker));
        Assertions.assertTrue(e.getMessage().contains("no"));
    }

    @Test
    void redirectWithProtocolUpgradeIsAllowed() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("https://www.google.com");
        URL url = getServerUrl(httpServer);
        when(mockChecker.checkURL(url)).thenReturn(url);
        when(mockChecker.checkURL(googleUrl)).thenReturn(googleUrl);

        // when
        String page = IOUtils.toString(
                Util.openInputStream(url.toString(), null, null, null, mockChecker), StandardCharsets.UTF_8);

        // then
        Assertions.assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    void redirectWithProtocolDowngradeIsNotAllowed() throws IOException {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("http://127.168.0.1");
        when(mockCon.getURL()).thenReturn(new URL("https://127.0.0.0"));

        RuntimeException e = assertThrows(RuntimeException.class, () -> Util.isRedirect(mockCon));

        Assertions.assertTrue(e.getMessage().contains("The redirect URI has a different protocol: http://127.168.0.1"));
    }

    @Test
    void isRedirectResolvesRelativeLocation() throws Exception {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("/relative/path");
        when(mockCon.getURL()).thenReturn(new URL("https://example.com/old/path"));

        Assertions.assertTrue(Util.isRedirect(mockCon));
    }

    @Test
    void isRedirectResolvesProtocolRelativeLocation() throws Exception {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("//other.example.com/path");
        when(mockCon.getURL()).thenReturn(new URL("https://example.com/old/path"));

        Assertions.assertTrue(Util.isRedirect(mockCon));
    }

    @Test
    void redirectWithProtocolRelativeLocationIsResolvedAndFollowed() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        GenericContainer targetServer = setUpServer("https://www.google.com");
        URL targetUrl = getServerUrl(targetServer);

        // protocol-relative Location (no scheme) must resolve against the first hop's scheme, not fail parsing
        httpServer = setUpServer("//" + targetUrl.getAuthority());
        URL url = getServerUrl(httpServer);
        when(mockChecker.checkURL(any()))
                .thenAnswer((Answer<URL>) invocation -> (URL) invocation.getArguments()[0]);

        try {
            String page = IOUtils.toString(
                    Util.openInputStream(url.toString(), null, null, null, mockChecker), StandardCharsets.UTF_8);

            Assertions.assertTrue(page.contains("<title>Google</title>"));
        } finally {
            targetServer.stop();
        }
    }

    @Test
    void shouldFailForExceedingRedirectLimit() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("https://127.0.0.0");
        URL url = getServerUrl(httpServer);
        when(mockChecker.checkURL(any()))
                .thenAnswer((Answer<URL>) invocation -> (URL) invocation.getArguments()[0]);

        ArrayList<GenericContainer> servers = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            GenericContainer server = setUpServer(url.toString());
            servers.add(server);
            url = getServerUrl(server);
        }

        URL finalUrl = url;
        IOException e = assertThrows(
                IOException.class, () -> Util.openInputStream(finalUrl.toString(), null, null, null, mockChecker));

        Assertions.assertTrue(e.getMessage().contains("Redirect limit exceeded"));

        for (GenericContainer server : servers) {
            server.stop();
        }
    }

    @Test
    void crossOriginRedirectDoesNotForwardCredentialsNorPayload() throws Exception {
        new ApocConfig(null);
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        when(mockChecker.checkURL(any(URL.class))).thenAnswer(invocation -> invocation.getArgument(0));

        AtomicReference<String> receivedAuthorization = new AtomicReference<>();
        AtomicReference<String> receivedCookie = new AtomicReference<>();
        AtomicReference<String> receivedBody = new AtomicReference<>();

        HttpServer target = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        target.createContext("/collect", exchange -> {
            receivedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            receivedCookie.set(exchange.getRequestHeaders().getFirst("Cookie"));
            receivedBody.set(IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8));
            byte[] response = "collected".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        target.start();

        HttpServer origin = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        origin.createContext("/report", exchange -> {
            exchange.getResponseHeaders()
                    .add("Location", "http://127.0.0.1:" + target.getAddress().getPort() + "/collect");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        origin.start();

        try {
            URL url = new URL("http://127.0.0.1:" + origin.getAddress().getPort() + "/report");
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put("Authorization", "Basic dXNlcjpzZWNyZXQ=");
            headers.put("Cookie", "session=abc");
            headers.put("Accept", "text/plain");

            String page = IOUtils.toString(
                    Util.openInputStream(url.toString(), headers, "{\"secret\":true}", null, mockChecker),
                    StandardCharsets.UTF_8);

            Assertions.assertEquals("collected", page);
            Assertions.assertNull(receivedAuthorization.get(), "Authorization must not be forwarded cross-origin");
            Assertions.assertNull(receivedCookie.get(), "Cookie must not be forwarded cross-origin");
            Assertions.assertEquals("", receivedBody.get(), "the payload must not be forwarded cross-origin");
        } finally {
            origin.stop(0);
            target.stop(0);
        }
    }

    @Test
    void sameOriginRedirectKeepsCredentialsAndPayload() throws Exception {
        new ApocConfig(null);
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        when(mockChecker.checkURL(any(URL.class))).thenAnswer(invocation -> invocation.getArgument(0));

        AtomicReference<String> receivedAuthorization = new AtomicReference<>();
        AtomicReference<String> receivedBody = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/final", exchange -> {
            receivedAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            receivedBody.set(IOUtils.toString(exchange.getRequestBody(), StandardCharsets.UTF_8));
            byte[] response = "done".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.createContext("/start", exchange -> {
            exchange.getResponseHeaders()
                    .add("Location", "http://127.0.0.1:" + server.getAddress().getPort() + "/final");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.start();

        try {
            URL url = new URL("http://127.0.0.1:" + server.getAddress().getPort() + "/start");
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put("Authorization", "Basic dXNlcjpzZWNyZXQ=");
            headers.put("method", "POST");

            String page = IOUtils.toString(
                    Util.openInputStream(url.toString(), headers, "{\"secret\":true}", null, mockChecker),
                    StandardCharsets.UTF_8);

            Assertions.assertEquals("done", page);
            Assertions.assertEquals("Basic dXNlcjpzZWNyZXQ=", receivedAuthorization.get());
            Assertions.assertEquals("{\"secret\":true}", receivedBody.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void redirectShouldThrowExceptionWhenProtocolChangesWithFileLocation() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("file:/etc/passwd");
        // given
        URL url = getServerUrl(httpServer);
        when(mockChecker.checkURL(url)).thenReturn(url);
        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.get(GraphDatabaseInternalSettings.cypher_ip_blocklist)).thenReturn(Collections.emptyList());

        // when
        RuntimeException e = assertThrows(
                RuntimeException.class, () -> Util.openInputStream(url.toString(), null, null, null, mockChecker));

        Assertions.assertEquals("The redirect URI has a different protocol: file:/etc/passwd", e.getMessage());
    }

    private URL getServerUrl(GenericContainer httpServer) throws MalformedURLException {
        return new URL(
                String.format("http://%s:%s", httpServer.getContainerIpAddress(), httpServer.getMappedPort(8000)));
    }
}
