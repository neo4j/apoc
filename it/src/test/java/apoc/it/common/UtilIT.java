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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import apoc.ApocConfig;
import apoc.util.Util;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.graphdb.security.URLAccessChecker;

import org.mockito.stubbing.Answer;
import org.testcontainers.containers.GenericContainer;

public class UtilIT {
    private GenericContainer httpServer;

    public UtilIT() throws Exception {
        googleUrl = new URL( "https://www.google.com" );
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
        httpServer.stop();
    }

    private final URL googleUrl;

    @Test
    public void redirectShouldWorkWhenProtocolNotChangesWithUrlLocation() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("https://www.google.com");

        // given
        URL url = getServerUrl(httpServer);
        when( mockChecker.checkURL( url ) ).thenReturn( url );
        when( mockChecker.checkURL( googleUrl ) ).thenReturn( googleUrl );

        // when
        String page = IOUtils.toString( Util.openInputStream(url.toString(), null, null, null, mockChecker ), StandardCharsets.UTF_8);

        // then
        assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    public void redirectWithBlockedIPsWithUrlLocation() throws Exception{
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);

        httpServer = setUpServer("http://127.168.0.1");
        URL url = getServerUrl(httpServer);
        when( mockChecker.checkURL( url ) ).thenReturn( url );
        when( mockChecker.checkURL( new URL("http://127.168.0.1") ) ).thenThrow( new URLAccessValidationError( "no" ) );

        IOException e = Assert.assertThrows(IOException.class, () -> Util.openInputStream(url.toString(), null, null, null, mockChecker));
        TestCase.assertTrue(e.getMessage().contains("no"));
    }

    @Test
    public void redirectWithProtocolUpgradeIsAllowed() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("https://www.google.com");
        URL url = getServerUrl(httpServer);
        when( mockChecker.checkURL( url ) ).thenReturn( url );
        when( mockChecker.checkURL( googleUrl ) ).thenReturn( googleUrl );

        // when
        String page = IOUtils.toString( Util.openInputStream(url.toString(), null, null, null, mockChecker), StandardCharsets.UTF_8 );

        // then
        assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    public void redirectWithProtocolDowngradeIsNotAllowed() throws IOException {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("http://127.168.0.1");
        when(mockCon.getURL()).thenReturn(new URL("https://127.0.0.0"));

        RuntimeException e = Assert.assertThrows(RuntimeException.class, () -> Util.isRedirect(mockCon));

        TestCase.assertTrue(e.getMessage().contains("The redirect URI has a different protocol: http://127.168.0.1"));
    }

    @Test
    public void shouldFailForExceedingRedirectLimit() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("https://127.0.0.0");
        URL url = getServerUrl(httpServer);
        when( mockChecker.checkURL( any() ) ).thenAnswer( (Answer<URL>) invocation -> (URL) invocation.getArguments()[0] );

        ArrayList<GenericContainer> servers = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            GenericContainer server = setUpServer(url.toString());
            servers.add(server);
            url = getServerUrl(server);
        }

        URL finalUrl = url;
        IOException e = Assert.assertThrows(IOException.class, () -> Util.openInputStream(finalUrl.toString(), null, null, null, mockChecker));

        TestCase.assertTrue(e.getMessage().contains("Redirect limit exceeded"));

        for (GenericContainer server : servers) {
            server.stop();
        }
    }

    @Test
    public void redirectShouldThrowExceptionWhenProtocolChangesWithFileLocation() throws Exception {
        URLAccessChecker mockChecker = mock(URLAccessChecker.class);
        httpServer = setUpServer("file:/etc/passwd");
        // given
        URL url = getServerUrl(httpServer);
        when( mockChecker.checkURL( url ) ).thenReturn( url );
        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.get(GraphDatabaseInternalSettings.cypher_ip_blocklist)).thenReturn(Collections.emptyList());

        // when
        RuntimeException e =
                Assert.assertThrows(RuntimeException.class, () -> Util.openInputStream(url.toString(), null, null, null, mockChecker));

        assertEquals("The redirect URI has a different protocol: file:/etc/passwd", e.getMessage());
    }

    private URL getServerUrl(GenericContainer httpServer) throws MalformedURLException
    {
        return new URL(String.format("http://%s:%s", httpServer.getContainerIpAddress(), httpServer.getMappedPort(8000)));
    }
}
