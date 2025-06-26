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

import static apoc.util.TestUtil.printFullStackTrace;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.github.dockerjava.api.exception.NotFoundException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.assertj.core.description.LazyTextDescription;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.testcontainers.containers.ContainerFetchException;
import org.testcontainers.utility.MountableFile;

public class TestContainerUtil {
    public enum Neo4jVersion {
        ENTERPRISE,
        COMMUNITY
    }

    public enum ApocPackage {
        CORE,
        EXTENDED
    }

    public static final String APOC_TEST_DOCKER_BUNDLE = "testDockerBundle";
    private static final Map<String, String> EMPTY_MAP = Collections.emptyMap();

    // read neo4j version from build.gradle
    public static final String neo4jEnterpriseDockerImageVersion = System.getProperty("neo4jDockerImage");
    public static final String neo4jCommunityDockerImageVersion = System.getProperty("neo4jCommunityDockerImage");

    public static final String password = "apoc12345";

    private TestContainerUtil() {}

    public static File baseDir = Paths.get("..").toFile();
    public static File pluginsFolder = new File(baseDir, "build/plugins");
    public static File importFolder = new File(baseDir, "build/import");

    public static String dockerImageForNeo4j(Neo4jVersion version) {
        if (version == Neo4jVersion.COMMUNITY) return neo4jCommunityDockerImageVersion;
        else return neo4jEnterpriseDockerImageVersion;
    }

    public static TestcontainersCausalCluster createEnterpriseCluster(
            List<ApocPackage> apocPackages,
            int numOfCoreInstances,
            int numberOfReadReplica,
            Map<String, Object> neo4jConfig,
            Map<String, String> envSettings) {
        return TestcontainersCausalCluster.create(
                apocPackages, numOfCoreInstances, numberOfReadReplica, Duration.ofMinutes(4), neo4jConfig, envSettings);
    }

    public static Neo4jContainerExtension createDB(
            Neo4jVersion version, List<ApocPackage> apocPackages, boolean withLogging) {
        return switch (version) {
            case ENTERPRISE -> createEnterpriseDB(apocPackages, withLogging);
            case COMMUNITY -> createCommunityDB(apocPackages, withLogging);
        };
    }

    public static Neo4jContainerExtension createDB(
            Neo4jVersion version,
            List<ApocPackage> apocPackages,
            boolean withLogging,
            GraphDatabaseSettings.CypherVersion cypherVersion,
            Map<String, String> withSystemProperties) {
        return switch (version) {
            case ENTERPRISE -> createEnterpriseDB(apocPackages, withLogging, cypherVersion, withSystemProperties);
            case COMMUNITY -> createCommunityDB(apocPackages, withLogging, cypherVersion, withSystemProperties);
        };
    }

    public static Neo4jContainerExtension createEnterpriseDB(List<ApocPackage> apocPackages, boolean withLogging) {
        return createNeo4jContainer(apocPackages, withLogging, Neo4jVersion.ENTERPRISE, null, EMPTY_MAP);
    }

    public static Neo4jContainerExtension createEnterpriseDB(
            List<ApocPackage> apocPackages,
            boolean withLogging,
            GraphDatabaseSettings.CypherVersion cypherVersion,
            Map<String, String> withSystemProperties) {
        return createNeo4jContainer(
                apocPackages, withLogging, Neo4jVersion.ENTERPRISE, cypherVersion, withSystemProperties);
    }

    public static Neo4jContainerExtension createCommunityDB(List<ApocPackage> apocPackages, boolean withLogging) {
        return createNeo4jContainer(apocPackages, withLogging, Neo4jVersion.COMMUNITY, null, EMPTY_MAP);
    }

    public static Neo4jContainerExtension createCommunityDB(
            List<ApocPackage> apocPackages,
            boolean withLogging,
            GraphDatabaseSettings.CypherVersion cypherVersion,
            Map<String, String> withSystemProperties) {
        return createNeo4jContainer(
                apocPackages, withLogging, Neo4jVersion.COMMUNITY, cypherVersion, withSystemProperties);
    }

    private static Neo4jContainerExtension createNeo4jContainer(
            List<ApocPackage> apocPackages,
            boolean withLogging,
            Neo4jVersion version,
            GraphDatabaseSettings.CypherVersion cypherVersion,
            Map<String, String> withSystemProperties) {
        String dockerImage;
        if (version == Neo4jVersion.ENTERPRISE) {
            dockerImage = neo4jEnterpriseDockerImageVersion;
        } else {
            dockerImage = neo4jCommunityDockerImageVersion;
        }

        try {
            FileUtils.deleteDirectory(pluginsFolder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // We define the container with external volumes
        importFolder.mkdirs();
        // use a separate folder for mounting plugins jar - build/libs might contain other jars as well.
        pluginsFolder.mkdirs();
        String canonicalPath = null;

        final Path logsDir;
        try {
            logsDir = Files.createTempDirectory("neo4j-logs");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            canonicalPath = importFolder.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }

        boolean testDockerBundle = System.getProperty(APOC_TEST_DOCKER_BUNDLE).equals("true");

        if (!testDockerBundle) {
            for (ApocPackage apocPackage : apocPackages) {
                final String jarPathProp =
                        switch (apocPackage) {
                            case CORE -> "apoc-core.test.jar.path";
                            case EXTENDED -> "apoc-extended.test.jar.path";
                        };
                final var jarPath = Path.of(System.getProperty(jarPathProp));
                final var destination = pluginsFolder.toPath().resolve(jarPath.getFileName());
                try {
                    System.out.println("Copying %s (prop %s) => %s".formatted(jarPath, jarPathProp, destination));
                    Files.createDirectories(pluginsFolder.toPath());
                    Files.copy(jarPath, destination, REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy %s to %s".formatted(jarPath, destination), e);
                }
            }
        }

        if (testDockerBundle && apocPackages.contains(ApocPackage.EXTENDED)) {
            throw new IllegalArgumentException("You cannot run these tests with apoc extended bundled inside "
                    + "the docker container because only apoc core comes bundled in those");
        }

        String cypherVersionSetting = cypherVersion == null
                ? System.getenv()
                        .getOrDefault(
                                "CYPHER_VERSION", Util.getCypherVersion(GraphDatabaseSettings.CypherVersion.Cypher5))
                : Util.getCypherVersion(cypherVersion);

        System.out.println("neo4jDockerImageVersion = " + dockerImage);
        Neo4jContainerExtension neo4jContainer = new Neo4jContainerExtension(dockerImage, logsDir)
                .withAdminPassword(password)
                .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
                .withEnv("apoc.export.file.enabled", "true")
                .withEnv("apoc.import.file.enabled", "true")
                .withNeo4jConfig("server.memory.heap.max_size", "512M")
                .withNeo4jConfig("server.memory.pagecache.size", "256M")
                .withNeo4jConfig("dbms.security.procedures.unrestricted", "apoc.*")
                .withNeo4jConfig("dbms.logs.http.enabled", "true")
                .withNeo4jConfig("dbms.logs.debug.level", "DEBUG")
                .withNeo4jConfig("dbms.routing.driver.logging.level", "DEBUG")
                .withNeo4jConfig("internal.dbms.cypher.enable_experimental_versions", "true")
                .withNeo4jConfig("db.query.default_language", "cypher_" + cypherVersionSetting)
                // Additional kernel assertions
                .withNeo4jConfig(
                        "server.jvm.additional",
                        "-Dorg.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracer.CHECK_REPORTED_COUNTERS=true")
                .withFileSystemBind(logsDir.toString(), "/logs")
                .withFileSystemBind(
                        canonicalPath, "/var/lib/neo4j/import") // map the "target/import" dir as the Neo4j's import dir
                .withCreateContainerCmdModifier(cmd -> cmd.withMemory(2024 * 1024 * 1024L)) // 2gb
                .withExposedPorts(7687, 7473, 7474)
                //                .withDebugger()  // attach debugger

                .withStartupAttempts(1)
                // set uid if possible - export tests do write to "/import"
                .withCreateContainerCmdModifier(cmd -> {
                    try {
                        Process p = Runtime.getRuntime().exec("id -u");
                        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                        String s = br.readLine();
                        p.waitFor();
                        p.destroy();
                        cmd.withUser(s);
                    } catch (Exception e) {
                        System.out.println("Exception while assign cmd user to docker container:\n"
                                + ExceptionUtils.getStackTrace(e));
                        // ignore since it may fail depending on operating system
                    }
                });

        if (withLogging) {
            neo4jContainer.withLogging();
        }

        if (testDockerBundle) {
            neo4jContainer.withEnv("NEO4J_PLUGINS", "[\"apoc\"]");
        } else {
            neo4jContainer.withPlugins(MountableFile.forHostPath(pluginsFolder.toPath()));
        }

        for (String key : withSystemProperties.keySet()) {
            neo4jContainer.withNeo4jConfig("-D" + key, withSystemProperties.get(key));
        }

        return neo4jContainer.withWaitForNeo4jDatabaseReady(password, version);
    }

    public static void testCall(
            Session session, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
        testResult(session, call, params, (res) -> {
            try {
                assertNotNull("result should be not null", res);
                assertTrue("result should be not empty", res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("result should not have next", res.hasNext());
            } catch (Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void testCall(Session session, String call, Consumer<Map<String, Object>> consumer) {
        testCall(session, call, null, consumer);
    }

    public static void testResult(
            Session session, String call, Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        testResult(session, call, null, resultConsumer);
    }

    public static void testResult(
            Session session,
            String call,
            Map<String, Object> params,
            Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        session.executeWrite(tx -> {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            final var result = tx.run(call, p).list();
            assertThat(result)
                    .describedAs(describe(call, params, result))
                    .satisfies(r ->
                            resultConsumer.accept(r.stream().map(Record::asMap).iterator()));
            return null;
        });
    }

    private static LazyTextDescription describe(final String cypher, Map<String, Object> params, List<Record> result) {
        return new LazyTextDescription(() -> {
            final var resultString = result.stream().map(Record::toString).collect(Collectors.joining("\n"));
            return """
            Cypher: %s
            Params: %s
            Results (%s rows):
            %s"""
                    .formatted(cypher, params, result.size(), resultString);
        });
    }

    public static void testCallEmpty(Session session, String call, Map<String, Object> params) {
        final var resultCount = session.run(call, params).stream().count();
        assertEquals(0, resultCount);
    }

    public static void testCallInReadTransaction(
            Session session, String call, Map<String, Object> params, Consumer<Map<String, Object>> consumer) {
        testResultInReadTransaction(session, call, params, (res) -> {
            try {
                assertNotNull("result should be not null", res);
                assertTrue("result should be not empty", res.hasNext());
                Map<String, Object> row = res.next();
                consumer.accept(row);
                assertFalse("result should not have next", res.hasNext());
            } catch (Throwable t) {
                printFullStackTrace(t);
                throw t;
            }
        });
    }

    public static void testResultInReadTransaction(
            Session session,
            String call,
            Map<String, Object> params,
            Consumer<Iterator<Map<String, Object>>> resultConsumer) {
        session.executeRead(tx -> {
            Map<String, Object> p = (params == null) ? Collections.<String, Object>emptyMap() : params;
            resultConsumer.accept(tx.run(call, p).list().stream()
                    .map(Record::asMap)
                    .collect(Collectors.toList())
                    .iterator());
            return null;
        });
    }

    public static boolean isDockerImageAvailable(Exception ex) {
        final Throwable cause = ex.getCause();
        final Throwable rootCause = ExceptionUtils.getRootCause(ex);
        return !(cause instanceof ContainerFetchException && rootCause instanceof NotFoundException);
    }
}
