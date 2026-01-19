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
package apoc.warmup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

/**
 * CYPHER 5 only; moved to extended for Cypher 25
 */
@EnterpriseDbmsExtension(configurationCallback = "configure", createDatabasePerTest = false)
public class WarmupTest {

    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5)
                .setConfig(GraphDatabaseSettings.db_format, "aligned");
    }

    @BeforeAll
    public void setUp() {
        prepareData(db);
    }

    @Test
    public void testWarmup() {
        TestUtil.testCall(db, "CALL apoc.warmup.run()", r -> {
            assertEquals(4L, r.get("nodesTotal"));
            assertNotEquals(0L, r.get("nodePages"));
            assertEquals(2L, r.get("relsTotal"));
            assertNotEquals(0L, r.get("relPages"));
        });
    }

    @Test
    public void testWarmupProperties() {
        TestUtil.testCall(db, "CALL apoc.warmup.run(true)", r -> {
            assertEquals(true, r.get("propertiesLoaded"));
            assertNotEquals(0L, r.get("propPages"));
        });
    }

    @Test
    public void testWarmupDynamicProperties() {
        TestUtil.testCall(db, "CALL apoc.warmup.run(true,true)", r -> {
            assertEquals(true, r.get("propertiesLoaded"));
            assertEquals(true, r.get("dynamicPropertiesLoaded"));
            assertNotEquals(0L, r.get("arrayPropPages"));
        });
    }

    @Test
    public void testWarmupIndexes() {
        TestUtil.testCall(db, "CALL apoc.warmup.run(true,true,true)", r -> {
            assertEquals(true, r.get("indexesLoaded"));
            assertNotEquals(0L, r.get("indexPages"));
        });
    }

    @Test
    public void testWarmupOnDifferentStorageEngines() {
        testWarmupOnDifferentStorageEnginesParameterized("standard");
        testWarmupOnDifferentStorageEnginesParameterized("aligned");
    }

    @ParameterizedTest
    @ValueSource(strings = {"standard", "aligned"})
    public void testWarmupOnDifferentStorageEnginesParameterized(String storageType) {
        java.nio.file.Path tempDir = null;
        DatabaseManagementService localDbms = null;
        try {
            tempDir = java.nio.file.Files.createTempDirectory("warmup-test-" + storageType + "-");
            localDbms = new TestDatabaseManagementServiceBuilder(tempDir)
                    .setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5)
                    .setConfig(GraphDatabaseSettings.db_format, storageType)
                    .build();

            GraphDatabaseService localDb = localDbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

            prepareData(localDb);

            // Execute warmup and assert
            TestUtil.testCall(localDb, "CALL apoc.warmup.run(true,true,true)", r -> {
                assertEquals(true, r.get("indexesLoaded"));
                assertNotEquals(0L, r.get("indexPages"));
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (localDbms != null) {
                localDbms.shutdown();
            }
            if (tempDir != null) {
                try {
                    java.nio.file.Files.walk(tempDir)
                            .sorted(java.util.Comparator.reverseOrder())
                            .forEach(p -> {
                                try {
                                    java.nio.file.Files.deleteIfExists(p);
                                } catch (Exception ignore) {
                                }
                            });
                } catch (Exception ignore) {
                }
            }
        }
    }

    private static void prepareData(GraphDatabaseService database) {
        TestUtil.registerProcedure(database, Warmup.class);
        database.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.foo IS UNIQUE");
        database.executeTransactionally(
                "UNWIND range(1, 300) AS i CREATE (n:Foo {foo:i})-[:KNOWS {bar:2}]->(m {foobar:3, array:range(1,100)})");

        database.executeTransactionally(
                "MATCH ()-[r:KNOWS]->() WITH [min(id(r)), max(id(r))] AS ids MATCH (n)-[r:KNOWS]->(m) WHERE NOT id(r) IN ids DELETE n, m, r");
    }
}
