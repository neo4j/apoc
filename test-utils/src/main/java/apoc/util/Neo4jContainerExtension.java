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
package apoc.util;

import static apoc.util.TestContainerUtil.Neo4jVersion;
import static apoc.util.TestContainerUtil.Neo4jVersion.ENTERPRISE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Scanner;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.ext.ScriptUtils;

/**
 * Extension for the Neo4jcontainer class of Testcontainers
 */
public class Neo4jContainerExtension extends Neo4jContainer<Neo4jContainerExtension> {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jContainerExtension.class);

    private Session session;
    private Driver driver;

    private String filePath;

    private boolean withDriver = true;

    private boolean isRunning = false;
    private final Path logsDir;

    public Neo4jContainerExtension() {
        super();
        this.logsDir = null;
    }

    public Neo4jContainerExtension(String dockerImage, Path logsDir) {
        setDockerImageName(dockerImage);
        this.logsDir = logsDir;
    }

    public Neo4jContainerExtension withInitScript(String filePath) {
        this.filePath = filePath;
        return this;
    }

    public Neo4jContainerExtension withoutDriver() {
        this.withDriver = false;
        return this;
    }

    @Override
    public void start() {
        try {
            super.start();
            if (withDriver) {
                driver = GraphDatabase.driver(getBoltUrl(), getAuth());
                session = driver.session();
                if (filePath != null && !filePath.isEmpty()) {
                    executeScript(filePath);
                }
            }
            isRunning = true;
        } catch (Exception startException) {
            dumpLogs();
            throw startException;
        }
    }

    public void dumpLogs() {
        try {
            if (logsDir != null && Files.exists(logsDir)) {
                System.err.println("--- Dumping logs ---");
                System.err.println();
                for (final var logFile : Files.list(logsDir).toList()) {
                    System.err.println(logFile.toString() + ":");
                    System.err.println(Files.readString(logFile));
                    System.err.println();
                    System.err.println();
                }
                System.err.println("--- No more logs ---");
            } else if (isRunning) {
                System.err.println(execInContainer("cat", "logs/debug.log").toString());
                System.err.println(execInContainer("cat", "logs/http.log").toString());
                System.err.println(execInContainer("cat", "logs/security.log").toString());
                System.err.println(execInContainer("cat", "logs/query.log").toString());
            } else {
                System.err.println("Failed to find logs");
            }
        } catch (Exception e) {
            System.err.println("Failed to dump logs: " + e.getMessage());
        }
    }

    public String queryLogs() throws IOException, InterruptedException {
        return execInContainer("cat", "logs/query.log").toString();
    }

    private void executeScript(String filePath) {
        InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
        if (resource == null) {
            logger().warn("Could not load classpath init script: {}", filePath);
            throw new ScriptUtils.ScriptLoadException(
                    "Could not load classpath init script: " + filePath + ". Resource not found.");
        }

        try (Scanner scanner = new Scanner(resource).useDelimiter(";")) {
            while (scanner.hasNext()) {
                String statement = scanner.next().trim();
                if (statement.isEmpty()) {
                    continue;
                }
                session.executeWriteWithoutResult(tx -> tx.run(statement));
            }
        }
    }

    public Session getSession() {
        return session;
    }

    @SuppressWarnings("unused") // used from extended
    public Driver getDriver() {
        return driver;
    }

    private AuthToken getAuth() {
        return getAdminPassword() != null && !getAdminPassword().isEmpty()
                ? AuthTokens.basic("neo4j", getAdminPassword())
                : AuthTokens.none();
    }

    public Neo4jContainerExtension withLogging() {
        withLogConsumer(new Slf4jLogConsumer(logger));
        return this;
    }

    @SuppressWarnings("unused") // can be used for debugging from TestContainerUtil
    public Neo4jContainerExtension withDebugger() {
        withExposedPorts(5005);
        withEnv(
                "NEO4J_dbms_jvm_additional",
                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=0.0.0.0:5005");
        return this;
    }

    private Neo4jContainerExtension withWaitForDatabaseReady(
            String username,
            String password,
            String database,
            Duration timeout,
            TestContainerUtil.Neo4jVersion version) {
        if (version == ENTERPRISE) {
            this.setWaitStrategy(Wait.forHttp("/db/" + database + "/cluster/available")
                    .withBasicCredentials(username, password)
                    .forPort(7474)
                    .forStatusCodeMatching(t -> {
                        logger.debug("/db/" + database + "/cluster/available [" + t.toString() + "]");
                        return t == 200;
                    })
                    .withReadTimeout(Duration.ofSeconds(3))
                    .withStartupTimeout(timeout));
        } else {
            this.setWaitStrategy(
                    Wait.forSuccessfulCommand("wget --no-verbose --tries=1 --spider localhost:7474 || exit 1")
                            .withStartupTimeout(timeout));
        }

        return this;
    }

    public Neo4jContainerExtension withWaitForNeo4jDatabaseReady(String password, Neo4jVersion version) {
        return withWaitForDatabaseReady("neo4j", password, "neo4j", Duration.ofSeconds(300), version);
    }

    @Override
    public void stop() {
        if (withDriver) {
            closeSafely(session);
            closeSafely(driver);
        }
        super.stop();
    }

    private static void closeSafely(AutoCloseable closeable) {
        try {
            if (closeable != null) closeable.close();
        } catch (Exception ignoed) {
        }
    }

    @Override
    public boolean isRunning() {
        return super.isRunning() && isRunning;
    }
}
