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
package apoc;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;

/**
 * @author mh
 * @since 14.05.16
 */
@ServiceProvider
public class ApocExtensionFactory extends ExtensionFactory<ApocExtensionFactory.Dependencies> {

    public ApocExtensionFactory() {
        super(ExtensionType.DATABASE, "APOC");
    }

    public interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();

        JobScheduler scheduler();

        LogService log();

        AvailabilityGuard availabilityGuard();

        DatabaseManagementService databaseManagementService();

        ApocConfig apocConfig();

        DatabaseEventListeners databaseEventListeners();

        @SuppressWarnings("unused") // used from extended
        GlobalProcedures globalProceduresRegistry();

        RegisterComponentFactory.RegisterComponentLifecycle registerComponentLifecycle();

        Pools pools();
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        LogService log = dependencies.log();
        return new ApocLifecycle(log, db, dependencies);
    }

    public static class ApocLifecycle extends LifecycleAdapter {
        private final Log userLog;
        private final GraphDatabaseAPI db;
        private final Dependencies dependencies;
        private final Map<String, Lifecycle> services = new HashMap<>();
        private final Collection<ApocGlobalComponents> apocGlobalComponents;
        private final Collection<AvailabilityListener> registeredListeners = new ArrayList<>();

        public ApocLifecycle(LogService log, GraphDatabaseAPI db, Dependencies dependencies) {
            this.db = db;
            this.dependencies = dependencies;
            this.userLog = log.getUserLog(ApocExtensionFactory.class);
            this.apocGlobalComponents = Services.loadAll(ApocGlobalComponents.class);
        }

        public static void withNonSystemDatabase(GraphDatabaseService db, Consumer<Void> consumer) {
            if (!SYSTEM_DATABASE_NAME.equals(db.databaseName())) {
                consumer.accept(null);
            }
        }

        @Override
        public void init() {
            withNonSystemDatabase(db, aVoid -> {
                for (ApocGlobalComponents c : apocGlobalComponents) {
                    services.putAll(c.getServices(db, dependencies));
                }

                String databaseName = db.databaseName();
                services.values().forEach(lifecycle -> dependencies
                        .registerComponentLifecycle()
                        .addResolver(databaseName, lifecycle.getClass(), lifecycle));
            });
        }

        @Override
        public void start() {
            withNonSystemDatabase(db, aVoid -> {
                services.forEach((key, value) -> {
                    try {
                        value.start();
                    } catch (Exception e) {
                        userLog.error("failed to start service " + key, e);
                    }
                });
            });

            AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
            // APOC core has a listener that is not also a service, so this is registered here.
            for (ApocGlobalComponents c : apocGlobalComponents) {
                for (AvailabilityListener listener : c.getListeners(db, dependencies)) {
                    registeredListeners.add(listener);
                    availabilityGuard.addListener(listener);
                }
            }

            // For APOC extended, the Cypher Procedures listener is both a service and a listener
            // To stop needing to keep a Map containing it as an object, which stops it being
            // cleaned up, we check for all APOC services which are also listeners and register them here
            for (Object service : services.values()) {
                if (service instanceof AvailabilityListener serviceWithAvailabilityListener) {
                    registeredListeners.add(serviceWithAvailabilityListener);
                    availabilityGuard.addListener(serviceWithAvailabilityListener);
                }
            }
        }

        @Override
        public void stop() {
            withNonSystemDatabase(db, aVoid -> {
                services.forEach((key, value) -> {
                    try {
                        value.stop();
                    } catch (Exception e) {
                        userLog.error("failed to stop service " + key, e);
                    }
                });
            });

            AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
            registeredListeners.forEach(availabilityGuard::removeListener);
            registeredListeners.clear();
        }

        @Override
        public void shutdown() throws Exception {
            String databaseName = db.databaseName();
            services.values().forEach(lifecycle -> dependencies
                    .registerComponentLifecycle()
                    .cleanUpResolver(databaseName, lifecycle.getClass()));
        }

        public Collection<AvailabilityListener> getRegisteredListeners() {
            return registeredListeners;
        }
    }
}
