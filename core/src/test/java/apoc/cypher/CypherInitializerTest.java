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
package apoc.cypher;

import static apoc.ApocConfig.APOC_CONFIG_INITIALIZER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.ApocExtensionFactory;
import apoc.util.TestUtil;
import apoc.util.Utils;
import apoc.util.collection.Iterators;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

public class CypherInitializerTest {

    private static void waitForInitializerBeingFinished(GraphDatabaseAPI api) {
        CypherInitializer initializer = getInitializer(api);
        while (!initializer.isFinished()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    void configureAll(TestDatabaseManagementServiceBuilder builder) {
        // Ensure no initializer is configured via system properties
        System.clearProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME);
        System.clearProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".0");
        System.clearProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".1");
        System.clearProperty(APOC_CONFIG_INITIALIZER + "." + SYSTEM_DATABASE_NAME);
        builder.setConfigRaw(Map.of("server.config.strict_validation.enabled", "false"));
    }

    /**
     * get a reference to CypherInitializer for diagnosis.
     */
    private static CypherInitializer getInitializer(GraphDatabaseAPI api) {
        var apoc = api.getDependencyResolver().resolveDependency(ApocExtensionFactory.ApocLifecycle.class);
        var listeners = apoc.getRegisteredListeners();
        for (AvailabilityListener listener : listeners) {
            if (listener instanceof CypherInitializer) {
                return (CypherInitializer) listener;
            }
        }
        throw new IllegalStateException("found no cypher initializer");
    }

    @Nested
    @ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class NoInitializer {
        @Inject
        GraphDatabaseService db;

        @Inject
        DatabaseManagementService dbms;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            configureAll(builder);
        }

        @BeforeAll
        void beforeAll() {
            TestUtil.registerProcedure(db, Utils.class);
            waitForInitializerBeingFinished((GraphDatabaseAPI) db);
            waitForInitializerBeingFinished((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME));
        }

        @Test
        public void noInitializerWorks() {
            expectNodeCount(0);
        }

        private void expectNodeCount(long i) {
            assertEquals(i, TestUtil.count(db, "match (n) return n"));
        }
    }

    @Nested
    @ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class EmptyInitializer {
        @Inject
        GraphDatabaseService db;

        @Inject
        DatabaseManagementService dbms;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            System.setProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME, "");
            configureAll(builder);
        }

        @BeforeAll
        void beforeAll() {
            TestUtil.registerProcedure(db, Utils.class);
            waitForInitializerBeingFinished((GraphDatabaseAPI) db);
            waitForInitializerBeingFinished((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME));
        }

        @Test
        public void emptyInitializerWorks() {
            expectNodeCount(0);
        }

        private void expectNodeCount(long i) {
            assertEquals(i, TestUtil.count(db, "match (n) return n"));
        }
    }

    @Nested
    @ImpermanentEnterpriseDbmsExtension(createDatabasePerTest = false, configurationCallback = "configure")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class SingleInitializer {
        @Inject
        GraphDatabaseService db;

        @Inject
        DatabaseManagementService dbms;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            configureAll(builder);
            System.setProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".0", "create()");
        }

        @BeforeAll
        void beforeAll() {
            TestUtil.registerProcedure(db, Utils.class);
            waitForInitializerBeingFinished((GraphDatabaseAPI) db);
            waitForInitializerBeingFinished((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME));
        }

        @Test
        public void singleInitializerWorks() {
            expectNodeCount(1);
        }

        private void expectNodeCount(long i) {
            assertEquals(i, TestUtil.count(db, "match (n) return n"));
        }
    }

    @Nested
    @ImpermanentEnterpriseDbmsExtension(createDatabasePerTest = false, configurationCallback = "configure")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class MultipleInitializers {
        @Inject
        GraphDatabaseService db;

        @Inject
        DatabaseManagementService dbms;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            configureAll(builder);
            System.setProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".0", "create()");
            System.setProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".1", "match (n) create ()");
        }

        @BeforeAll
        void beforeAll() {
            TestUtil.registerProcedure(db, Utils.class);
            waitForInitializerBeingFinished((GraphDatabaseAPI) db);
            waitForInitializerBeingFinished((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME));
        }

        @Test
        public void multipleInitializersWorks() {
            expectNodeCount(2);
        }

        private void expectNodeCount(long i) {
            assertEquals(i, TestUtil.count(db, "match (n) return n"));
        }
    }

    @Nested
    @ImpermanentEnterpriseDbmsExtension(createDatabasePerTest = false, configurationCallback = "configure")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class MultipleInitializers2 {
        @Inject
        GraphDatabaseService db;

        @Inject
        DatabaseManagementService dbms;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            configureAll(builder);
            System.setProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".0", "match (n) create ()");
            System.setProperty(APOC_CONFIG_INITIALIZER + "." + DEFAULT_DATABASE_NAME + ".1", "create()");
        }

        @BeforeAll
        void beforeAll() {
            TestUtil.registerProcedure(db, Utils.class);
            waitForInitializerBeingFinished((GraphDatabaseAPI) db);
            waitForInitializerBeingFinished((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME));
        }

        @Test
        public void multipleInitializersWorks2() {
            expectNodeCount(1);
        }

        private void expectNodeCount(long i) {
            assertEquals(i, TestUtil.count(db, "match (n) return n"));
        }
    }

    @Nested
    @ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class SystemDatabaseInitializers {
        @Inject
        DatabaseManagementService dbms;

        @ExtensionCallback
        void configure(TestDatabaseManagementServiceBuilder builder) {
            configureAll(builder);
            System.setProperty(
                    APOC_CONFIG_INITIALIZER + "." + SYSTEM_DATABASE_NAME, "create user dummy set password 'abcd1234'");
        }

        @BeforeAll
        void beforeAll() {
            // register any APOC procedure to ensure initializers can see at least one APOC proc
            GraphDatabaseService db = dbms.database(DEFAULT_DATABASE_NAME);
            TestUtil.registerProcedure(db, Utils.class);
            waitForInitializerBeingFinished((GraphDatabaseAPI) db);
            waitForInitializerBeingFinished((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME));
        }

        @Test
        public void databaseSpecificInitializersForSystem() {
            GraphDatabaseService systemDb = dbms.database(SYSTEM_DATABASE_NAME);
            long numberOfUsers =
                    systemDb.executeTransactionally("show users", Collections.emptyMap(), Iterators::count);
            assertEquals(2L, numberOfUsers);
        }
    }
}
