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
package apoc.mvcc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.atomic.Atomic;
import apoc.cypher.Cypher;
import apoc.lock.Lock;
import apoc.periodic.Periodic;
import apoc.refactor.GraphRefactoring;
import apoc.trigger.Trigger;
import apoc.trigger.TriggerNewProcedures;
import apoc.util.TestUtil;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Checks that the procedures that are not supported on the multiversion (MVCC) store format fail with the shared
 * error from {@link apoc.util.MvccUtil}, and that they keep working on the standard store format.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MvccUnsupportedProceduresTest {

    private static final String MVCC_FORMAT = System.getProperty("apoc.mvcc.format", "multiversion_block");
    private static final String MVCC_MESSAGE = "multiversion (MVCC) store format";

    private static final Class<?>[] PROCEDURES = {
        Atomic.class,
        Cypher.class,
        GraphRefactoring.class,
        Lock.class,
        Periodic.class,
        Trigger.class,
        TriggerNewProcedures.class
    };

    @TempDir
    static Path tempDir;

    private DatabaseManagementService mvccDbms;
    private DatabaseManagementService standardDbms;
    private GraphDatabaseService mvcc;
    private GraphDatabaseService mvccSystem;
    private GraphDatabaseService standard;

    @BeforeAll
    void startDatabases() {
        System.setProperty("apoc.trigger.enabled", "true");

        mvccDbms = new TestEnterpriseDatabaseManagementServiceBuilder(tempDir.resolve("mvcc"))
                .setConfigRaw(Map.of("db.format", MVCC_FORMAT))
                .build();
        standardDbms = new TestEnterpriseDatabaseManagementServiceBuilder(tempDir.resolve("standard")).build();
        mvcc = mvccDbms.database("neo4j");
        mvccSystem = mvccDbms.database("system");
        standard = standardDbms.database("neo4j");

        TestUtil.registerProcedure(mvcc, PROCEDURES);
        TestUtil.registerProcedure(standard, PROCEDURES);
    }

    @AfterAll
    void stopDatabases() {
        if (mvccDbms != null) mvccDbms.shutdown();
        if (standardDbms != null) standardDbms.shutdown();
    }

    @BeforeEach
    void clean() {
        mvcc.executeTransactionally("MATCH (n) DETACH DELETE n");
        standard.executeTransactionally("MATCH (n) DETACH DELETE n");
        mvcc.executeTransactionally("CREATE (:Person {name: 'a', age: 1}), (:Person {name: 'a', age: 2})");
        standard.executeTransactionally("CREATE (:Person {name: 'a', age: 1}), (:Person {name: 'a', age: 2})");
    }

    private static void assertMvccError(GraphDatabaseService db, String procedure, String query) {
        var e = assertThrows(
                Exception.class,
                () -> db.executeTransactionally(query, Map.of(), r -> {
                    r.stream().count();
                    return null;
                }));
        String message = rootMessages(e);
        assertTrue(message.contains(MVCC_MESSAGE), "Missing MVCC message in: " + message);
        assertTrue(message.contains(procedure), "Missing procedure name '" + procedure + "' in: " + message);
    }

    private static String rootMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (; t != null; t = t.getCause()) {
            sb.append(t.getMessage()).append('\n');
        }
        return sb.toString();
    }

    static Stream<Arguments> unsupportedOnMvcc() {
        String iterate = "CALL apoc.periodic.iterate('UNWIND range(1, 10) AS i RETURN i', 'CREATE (:Item {i: i})', "
                + "{batchSize: 5, parallel: true})";
        return Stream.of(
                Arguments.of("apoc.periodic.iterate", iterate),
                Arguments.of("apoc.cypher.runMany", "CALL apoc.cypher.runMany('CREATE (:Item);', {})"),
                Arguments.of(
                        "apoc.refactor.categorize",
                        "CALL apoc.refactor.categorize('name', 'HAS_NAME', true, 'Name', 'name', [], 1)"),
                Arguments.of(
                        "apoc.refactor.mergeNodes",
                        "MATCH (n:Person) WITH collect(n) AS nodes CALL apoc.refactor.mergeNodes(nodes) YIELD node RETURN node"),
                Arguments.of(
                        "apoc.refactor.extractNode",
                        "CREATE (:A)-[r:R]->(:B) WITH r CALL apoc.refactor.extractNode([r], ['Mid'], 'IN', 'OUT') YIELD output RETURN output"),
                Arguments.of(
                        "apoc.refactor.cloneSubgraph",
                        "MATCH (n:Person) WITH collect(n) AS nodes CALL apoc.refactor.cloneSubgraph(nodes) YIELD input RETURN input"),
                Arguments.of(
                        "apoc.refactor.cloneSubgraphFromPaths",
                        "MATCH p = (n:Person) WITH collect(p) AS paths CALL apoc.refactor.cloneSubgraphFromPaths(paths) YIELD input RETURN input"),
                Arguments.of(
                        "apoc.lock.all", "MATCH (n:Person) WITH collect(n) AS nodes CALL apoc.lock.all(nodes, [])"),
                Arguments.of(
                        "apoc.lock.nodes", "MATCH (n:Person) WITH collect(n) AS nodes CALL apoc.lock.nodes(nodes)"),
                Arguments.of("apoc.lock.rels", "CALL apoc.lock.rels([])"),
                Arguments.of(
                        "apoc.lock.read.nodes",
                        "MATCH (n:Person) WITH collect(n) AS nodes CALL apoc.lock.read.nodes(nodes)"),
                Arguments.of("apoc.lock.read.rels", "CALL apoc.lock.read.rels([])"),
                Arguments.of(
                        "apoc.atomic.add",
                        "MATCH (n:Person {age: 1}) CALL apoc.atomic.add(n, 'age', 1) YIELD oldValue RETURN oldValue"),
                Arguments.of(
                        "apoc.atomic.subtract",
                        "MATCH (n:Person {age: 1}) CALL apoc.atomic.subtract(n, 'age', 1) YIELD oldValue RETURN oldValue"),
                Arguments.of(
                        "apoc.atomic.concat",
                        "MATCH (n:Person {age: 1}) CALL apoc.atomic.concat(n, 'name', 'x') YIELD oldValue RETURN oldValue"),
                Arguments.of(
                        "apoc.atomic.insert",
                        "MATCH (n:Person {age: 1}) SET n.list = [1] WITH n CALL apoc.atomic.insert(n, 'list', 0, 2) YIELD oldValue RETURN oldValue"),
                Arguments.of(
                        "apoc.atomic.remove",
                        "MATCH (n:Person {age: 1}) SET n.list = [1] WITH n CALL apoc.atomic.remove(n, 'list', 0) YIELD oldValue RETURN oldValue"),
                Arguments.of(
                        "apoc.atomic.update",
                        "MATCH (n:Person {age: 1}) CALL apoc.atomic.update(n, 'age', 'n.age + 1') YIELD oldValue RETURN oldValue"),
                Arguments.of("apoc.trigger.add", "CALL apoc.trigger.add('t', 'RETURN 1', {phase: 'after'})"),
                Arguments.of("apoc.trigger.remove", "CALL apoc.trigger.remove('t')"),
                Arguments.of("apoc.trigger.removeAll", "CALL apoc.trigger.removeAll()"),
                Arguments.of("apoc.trigger.list", "CALL apoc.trigger.list()"),
                Arguments.of("apoc.trigger.pause", "CALL apoc.trigger.pause('t')"),
                Arguments.of("apoc.trigger.resume", "CALL apoc.trigger.resume('t')"));
    }

    static Stream<Arguments> unsupportedTriggersOnSystem() {
        return Stream.of(
                Arguments.of("apoc.trigger.install", "CALL apoc.trigger.install('neo4j', 't', 'RETURN 1', {})"),
                Arguments.of("apoc.trigger.drop", "CALL apoc.trigger.drop('neo4j', 't')"),
                Arguments.of("apoc.trigger.dropAll", "CALL apoc.trigger.dropAll('neo4j')"),
                Arguments.of("apoc.trigger.stop", "CALL apoc.trigger.stop('neo4j', 't')"),
                Arguments.of("apoc.trigger.start", "CALL apoc.trigger.start('neo4j', 't')"),
                Arguments.of("apoc.trigger.show", "CALL apoc.trigger.show('neo4j')"));
    }

    @ParameterizedTest(name = "{0} errors on MVCC")
    @MethodSource("unsupportedOnMvcc")
    void unsupportedProceduresErrorOnMvcc(String procedure, String query) {
        assertMvccError(mvcc, procedure, query);
    }

    @ParameterizedTest(name = "{0} errors on MVCC (system database)")
    @MethodSource("unsupportedTriggersOnSystem")
    void unsupportedTriggerProceduresErrorOnMvcc(String procedure, String query) {
        assertMvccError(mvccSystem, procedure, query);
    }

    @Test
    void supportedUsageStillWorksOnMvcc() {
        assertDoesNotThrow(
                () -> mvcc.executeTransactionally(
                        "CALL apoc.periodic.iterate('UNWIND range(1, 10) AS i RETURN i', 'CREATE (:Item {i: i})', {batchSize: 5, parallel: false})"));
        assertDoesNotThrow(() -> mvcc.executeTransactionally("CALL apoc.cypher.runMany('RETURN 1 AS x;', {})"));
        assertDoesNotThrow(() ->
                mvcc.executeTransactionally("CALL apoc.cypher.runManyReadOnly('MATCH (n:Person) RETURN n;', {})"));
    }

    @Test
    void unsupportedProceduresStillWorkOnStandardFormat() {
        assertDoesNotThrow(() -> standard.executeTransactionally(
                "MATCH (n:Person) WITH collect(n) AS nodes CALL apoc.lock.nodes(nodes)"));
        assertDoesNotThrow(() -> standard.executeTransactionally(
                "MATCH (n:Person {age: 1}) CALL apoc.atomic.add(n, 'age', 1) YIELD oldValue RETURN oldValue"));
        assertDoesNotThrow(() -> standard.executeTransactionally("CALL apoc.cypher.runMany('CREATE (:Item);', {})"));
        assertDoesNotThrow(
                () -> standard.executeTransactionally(
                        "CALL apoc.periodic.iterate('UNWIND range(1, 10) AS i RETURN i', 'CREATE (:Item {i: i})', {batchSize: 5, parallel: true})"));
    }
}
