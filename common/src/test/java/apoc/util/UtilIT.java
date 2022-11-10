package apoc.util;

import apoc.ApocConfig;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtilIT {

    @Rule
    public TestName testName = new TestName();

    private GenericContainer httpServer;

    private static final String WITH_URL_LOCATION = "WithUrlLocation";

    @Before
    public void setUp() throws Exception {
        new ApocConfig();  // empty test configuration, ensure ApocConfig.apocConfig() can be used
        boolean isHttpTest = testName.getMethodName().endsWith(WITH_URL_LOCATION);
        TestUtil.ignoreException(() -> {
            httpServer = new GenericContainer("alpine")
                    .withCommand("/bin/sh", "-c", String.format("while true; do { echo -e 'HTTP/1.1 301 Moved Permanently\\r\\nLocation: %s'; echo ; } | nc -l -p 8000; done",
                            isHttpTest ? "http://www.google.com" : "file:/etc/passwd"))
                    .withExposedPorts(8000);
            if (isHttpTest) {
                httpServer.waitingFor(Wait.forHttp("/").forStatusCode(301));
            }
            httpServer.start();
        }, Exception.class);
        Assume.assumeNotNull(httpServer);
        Assume.assumeTrue(httpServer.isRunning());
    }

    @After
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop();
        }
    }

    @Test
    public void redirectShouldWorkWhenProtocolNotChangesWithUrlLocation() throws IOException {
        // given
        String url = getServerUrl();

        // when
        String page = IOUtils.toString(Util.openInputStream(url, null, null, null), Charset.forName("UTF-8"));

        // then
        assertTrue(page.contains("<title>Google</title>"));
    }

    @Test(expected = RuntimeException.class)
    public void redirectShouldThrowExceptionWhenProtocolChangesWithFileLocation() throws IOException {
        try {
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
