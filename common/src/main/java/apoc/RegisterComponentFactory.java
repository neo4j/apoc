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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.service.Services;

/**
 * NOTE: this is a GLOBAL component, so only once per DBMS
 */
@ServiceProvider
public class RegisterComponentFactory extends ExtensionFactory<RegisterComponentFactory.Dependencies> {

    private InternalLog log;
    private GlobalProcedures globalProceduresRegistry;

    public RegisterComponentFactory() {
        super(ExtensionType.GLOBAL, "ApocRegisterComponent");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        globalProceduresRegistry = dependencies.globalProceduresRegistry();
        log = dependencies.log().getUserLog(RegisterComponentFactory.class);
        return new RegisterComponentLifecycle();
    }

    public interface Dependencies {
        LogService log();

        GlobalProcedures globalProceduresRegistry();
    }

    public class RegisterComponentLifecycle extends LifecycleAdapter {

        private final Map<Class, Map<String, Object>> resolvers = new ConcurrentHashMap<>();

        public void addResolver(String databaseName, Class clazz, Object instance) {
            Map<String, Object> classInstanceMap = resolvers.computeIfAbsent(clazz, s -> new ConcurrentHashMap<>());
            classInstanceMap.put(databaseName, instance);
        }

        public void cleanUpResolver(String databaseName, Class clazz) {
            Map<String, Object> innerMap = resolvers.get(clazz);

            if (innerMap != null) {
                // Remove the database to instance value
                innerMap.remove(databaseName);

                // If the inner map is now empty, remove the key from the outer map
                if (innerMap.isEmpty()) {
                    resolvers.remove(clazz);
                }
            }
        }

        @SuppressWarnings("unused") // used from extended
        public Map<Class, Map<String, Object>> getResolvers() {
            return resolvers;
        }

        @Override
        public void init() {

            for (ApocGlobalComponents c : Services.loadAll(ApocGlobalComponents.class)) {
                for (Class clazz : c.getContextClasses()) {
                    resolvers.put(clazz, new ConcurrentHashMap<>());
                }
            }

            resolvers.forEach((clazz, dbFunctionMap) -> globalProceduresRegistry.registerComponent(
                    clazz,
                    context -> {
                        String databaseName = context.graphDatabaseAPI().databaseName();
                        Object instance = dbFunctionMap.get(databaseName);
                        if (instance == null) {
                            log.warn(
                                    "couldn't find a instance for clazz %s and database %s",
                                    clazz.getName(), databaseName);
                        }
                        return instance;
                    },
                    true));
        }
    }
}
