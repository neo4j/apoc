package apoc.util;

import apoc.ApocConfig;
import inet.ipaddr.IPAddressString;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UtilIT {
    private GenericContainer httpServer;

    private void setUpServer(Config neo4jConfig, String redirectURL) {
        new ApocConfig(neo4jConfig);
        TestUtil.ignoreException(() -> {
            httpServer = new GenericContainer("alpine")
                    .withCommand("/bin/sh", "-c", String.format("while true; do { echo -e 'HTTP/1.1 301 Moved Permanently\\r\\nLocation: %s'; echo ; } | nc -l -p 8000; done",
                            redirectURL))
                    .withExposedPorts(8000);
            httpServer.start();
        }, Exception.class);
        Assume.assumeNotNull(httpServer);
        Assume.assumeTrue(httpServer.isRunning());
    }

    @AfterEach
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop();
        }
    }

    @Test
    public void redirectShouldWorkWhenProtocolNotChangesWithUrlLocation() throws IOException {
        setUpServer(null, "http://www.google.com");
        // given
        String url = getServerUrl();

        // when
        String page = IOUtils.toString(Util.openInputStream(url, null, null, null), Charset.forName("UTF-8"));

        // then
        assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    public void redirectWithBlockedIPsWithUrlLocation() {
        List<IPAddressString> blockedIPs = List.of(new IPAddressString("127.168.0.1/8"));

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.get(GraphDatabaseInternalSettings.cypher_ip_blocklist)).thenReturn(blockedIPs);

        setUpServer(neo4jConfig, "http://127.168.0.1");
        String url = getServerUrl();

        IOException e = Assert.assertThrows(IOException.class,
                () -> Util.openInputStream(url, null, null, null)
        );
        TestCase.assertTrue(e.getMessage().contains("access to /127.168.0.1 is blocked via the configuration property internal.dbms.cypher_ip_blocklist"));
    }

    @Test(expected = RuntimeException.class)
    public void redirectShouldThrowExceptionWhenProtocolChangesWithFileLocation() throws IOException {
        try {
            setUpServer(null, "file:/etc/passwd");
            // given
            String url = getServerUrl();

            // when
            Util.openInputStream(url, null, null, null);
        } catch (RuntimeException e) {
            // then
            assertEquals("The redirect URI has a different protocol: file:/etc/passwd", e.getMessage());
            throw e;
        }
    }

    private String getServerUrl() {
        return String.format("http://%s:%s", httpServer.getContainerIpAddress(), httpServer.getMappedPort(8000));
    }
}
