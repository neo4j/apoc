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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import apoc.util.TestUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * CYPHER 5 only; moved to extended for Cypher 25
 */
public class WarmupTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5);

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Warmup.class);
        // Create enough nodes and relationships to span 2 pages
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.foo IS UNIQUE");
        db.executeTransactionally(
                "UNWIND range(1, 300) AS i CREATE (n:Foo {foo:i})-[:KNOWS {bar:2}]->(m {foobar:3, array:range(1,100)})");
        // Delete all relationships and their nodes, but ones with the minimum and maximum relationship ids, so
        // they still span 2 pages
        db.executeTransactionally("MATCH ()-[r:KNOWS]->() " + "WITH [min(id(r)), max(id(r))] AS ids "
                + "MATCH (n)-[r:KNOWS]->(m) "
                + "WHERE NOT id(r) IN ids "
                + "DELETE n, m, r");
    }

    @After
    public void teardown() {
        db.shutdown();
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
        final List<String> supportedTypes = Arrays.asList("standard", "aligned");
        for (String storageType : supportedTypes) {
            db.restartDatabase(Map.of(GraphDatabaseSettings.db_format, storageType));
            TestUtil.registerProcedure(db, Warmup.class);

            TestUtil.testCall(db, "CALL apoc.warmup.run(true,true,true)", r -> {
                assertEquals(true, r.get("indexesLoaded"));
                assertNotEquals(0L, r.get("indexPages"));
            });
        }
    }
}
