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
package apoc.it.core;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Session;

import java.util.List;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WarmupEnterpriseTest {

    @Test
    public void testWarmupIsntAllowedWithOtherStorageEngines() {
        Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig(GraphDatabaseInternalSettings.include_versions_under_development.name(), "true")
                .withNeo4jConfig(GraphDatabaseSettings.db_format.name(), "multiversion");
        neo4jContainer.start();
        Session session = neo4jContainer.getSession();

        RuntimeException e = assertThrows(RuntimeException.class, () -> testCall(session, "CALL apoc.warmup.run()", (r) -> {}));
        assertTrue(e.getMessage().contains("Record engine type unsupported"));

        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testWarmupOnEnterpriseStorageEngine() {
        Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig(GraphDatabaseInternalSettings.include_versions_under_development.name(), "true")
                .withNeo4jConfig(GraphDatabaseSettings.db_format.name(), "high_limit");
        neo4jContainer.start();
        Session session = neo4jContainer.getSession();

        testCall(session, "CALL apoc.warmup.run(true,true,true)", r -> {
            assertEquals(true, r.get("indexesLoaded"));
            assertNotEquals( 0L, r.get("indexPages") );
        });

        session.close();
        neo4jContainer.close();
    }
}
