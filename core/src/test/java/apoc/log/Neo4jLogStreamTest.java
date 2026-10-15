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
package apoc.log;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testResult;
import static org.junit.jupiter.api.Assertions.assertThrows;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class Neo4jLogStreamTest {

    private GraphDatabaseService db;
    private DatabaseManagementService dbManagementService;

    @BeforeEach
    void setUp() {
        dbManagementService = new TestDatabaseManagementServiceBuilder(
                        Paths.get("target", UUID.randomUUID().toString()).toAbsolutePath())
                .build();
        apocConfig().setProperty("server.directories.logs", "");
        db = dbManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Neo4jLogStream.class);
    }

    @AfterEach
    void teardown() {
        dbManagementService.shutdown();
    }

    @Test
    void testLogStream() {
        testResult(db, "CALL apoc.log.stream('debug.log')", res -> {
            final String wholeFile =
                    Iterators.stream(res.<String>columnAs("line")).collect(Collectors.joining(""));
            Assertions.assertTrue(wholeFile.contains("apoc.import.file.enabled=false"));
        });
    }

    @TempDir
    Path tmp;

    private Path newLogDir() throws IOException {
        Path logs = Files.createDirectory(tmp.resolve("logs"));
        apocConfig().setProperty("server.directories.logs", logs.toString());
        return logs;
    }

    private void assertRejected(String name, String expectedMessagePart) {
        Exception e = assertThrows(
                Exception.class,
                () -> testResult(db, "CALL apoc.log.stream($name)", Map.of("name", name), Result::resultAsString));
        Assertions.assertTrue(e.getMessage().contains(expectedMessagePart), "Unexpected message: " + e.getMessage());
    }

    @Test
    void readsFileInsideLogDir() throws IOException {
        Path logs = newLogDir();
        Files.write(logs.resolve("ok.log"), List.of("a", "b"));
        testResult(db, "CALL apoc.log.stream('ok.log')", res -> {
            Assertions.assertEquals(
                    List.of("a", "b"),
                    Iterators.stream(res.<String>columnAs("line")).collect(Collectors.toList()));
        });
    }

    @Test
    void rejectsRelativeTraversal() throws IOException {
        newLogDir();
        Files.write(tmp.resolve("secret.txt"), List.of("secret"));
        assertRejected("../secret.txt", "outside of the logs directory");
    }

    @Test
    void rejectsSymlinkToOutsideFile() throws IOException {
        Path logs = newLogDir();
        Path secret = Files.write(tmp.resolve("secret.txt"), List.of("secret"));
        Files.createSymbolicLink(logs.resolve("x.log"), secret);
        assertRejected("x.log", "outside of the logs directory");
    }

    @Test
    void rejectsHardlinkToOutsideFile() throws IOException {
        Path logs = newLogDir();
        Path secret = Files.write(tmp.resolve("secret.txt"), List.of("secret"));
        try {
            Files.createLink(logs.resolve("x.log"), secret);
            Files.getAttribute(secret, "unix:nlink");
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
            Assumptions.abort("hardlinks / unix:nlink unsupported on this platform");
        }
        assertRejected("x.log", "regular, unlinked file");
    }

    @Test
    void worksWhenLogDirIsReachedThroughSymlink() throws IOException {
        Path real = Files.createDirectory(tmp.resolve("real-logs"));
        Files.write(real.resolve("ok.log"), List.of("a"));
        Path link = Files.createSymbolicLink(tmp.resolve("link-logs"), real);
        apocConfig().setProperty("server.directories.logs", link.toString());
        testResult(db, "CALL apoc.log.stream('ok.log')", res -> Assertions.assertEquals(1, Iterators.count(res)));
    }

    @Test
    void missingFileGivesGenericMessage() throws IOException {
        newLogDir();
        assertRejected("nope.log", "No log file exists by that name");
    }
}
