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

import static apoc.SystemPropertyKeys.database;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.util.LogsUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import apoc.version.Version;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.Log;

public class CypherInitializer implements AvailabilityListener {

    private final GraphDatabaseAPI db;
    private final Log userLog;
    private final DependencyResolver dependencyResolver;
    private final DatabaseManagementService databaseManagementService;
    private final DatabaseEventListeners databaseEventListeners;

    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private volatile boolean finished = false;

    public CypherInitializer(
            GraphDatabaseAPI db,
            Log userLog,
            DatabaseManagementService databaseManagementService,
            DatabaseEventListeners databaseEventListeners) {
        this.db = db;
        this.userLog = userLog;
        this.databaseManagementService = databaseManagementService;
        this.databaseEventListeners = databaseEventListeners;
        this.dependencyResolver = db.getDependencyResolver();
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public void available() {
        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        Util.newDaemonThread(() -> {
                    try {
                        // Wait for database to become available, once it is available
                        // procedures are also available by necessity.
                        while (!db.isAvailable(100))
                            ;

                        // An initializer is attached to the lifecycle for each database. To ensure that this
                        // check is performed **once** during the DBMS startup, we validate the version if and
                        // only if we are the AvailabilityListener for the system database - since there is only
                        // ever one of those.
                        if (db.databaseId().isSystemDatabase()) {
                            String neo4jVersion = org.neo4j.kernel.internal.Version.getNeo4jVersion();
                            final String apocVersion =
                                    Version.class.getPackage().getImplementationVersion();
                            if (isVersionDifferent(neo4jVersion, apocVersion)) {
                                userLog.warn(
                                        "The apoc version (%s) and the Neo4j DBMS versions %s are incompatible. \n"
                                                + "The two first numbers of both versions needs to be the same.",
                                        apocVersion, neo4jVersion);
                            }
                            databaseEventListeners.registerDatabaseEventListener(new SystemFunctionalityListener());
                        }

                        Configuration config = dependencyResolver
                                .resolveDependency(ApocConfig.class)
                                .getConfig();
                        for (final var query : collectInitializers(config)) {
                            final var sanitizedQuery =
                                    LogsUtil.sanitizeQuery(dependencyResolver.resolveDependency(Config.class), query);
                            try {
                                // we need to apply a retry strategy here since in systemdb we potentially conflict
                                // with
                                // creating constraints which could cause our query to fail with a transient error.
                                Util.retryInTx(
                                        userLog, db, tx -> Iterators.count(tx.execute(query)), 0, 5, retries -> {});
                                userLog.info("successfully initialized: " + sanitizedQuery);
                            } catch (Exception e) {
                                userLog.error("error upon initialization, running: " + sanitizedQuery, e);
                            }
                        }
                    } catch (Exception e) {
                        userLog.error("error upon initialization", e);
                    } finally {
                        finished = true;
                    }
                })
                .start();
    }

    // the visibility is public only for testing purpose, it could be private otherwise
    public static boolean isVersionDifferent(String neo4jVersion, String apocVersion) {
        final String[] apocSplit = splitVersion(apocVersion);
        final String[] neo4jSplit = splitVersion(neo4jVersion);

        return !(apocSplit != null
                && neo4jSplit != null
                && apocSplit[0].equals(neo4jSplit[0])
                && apocSplit[1].equals(neo4jSplit[1]));
    }

    private static String[] splitVersion(String completeVersion) {
        if (StringUtils.isBlank(completeVersion)) {
            return null;
        }
        return completeVersion.split("[^\\d]");
    }

    private Collection<String> collectInitializers(Configuration config) {
        Map<String, String> initializers = new TreeMap<>();

        config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER + "." + db.databaseName())
                .forEachRemaining(key -> putIfNotBlank(initializers, key, config.getString(key)));

        return initializers.values();
    }

    private void putIfNotBlank(Map<String, String> map, String key, String value) {
        if ((value != null) && (!value.isBlank())) {
            map.put(key, value);
        }
    }

    @Override
    public void unavailable() {
        // intentionally empty
    }

    private class SystemFunctionalityListener implements DatabaseEventListener {

        @Override
        public void databaseDrop(DatabaseEventContext eventContext) {
            final var dbName = eventContext.getDatabaseName();
            if (!Objects.equals(dbName, db.databaseName())) {
                if (!db.isAvailable()) {
                    userLog.warn(
                            "Database %s not available, skipping apoc trigger cleanup of database %s.",
                            db.databaseName(), dbName);
                    return;
                }
                try (final var tx = db.beginTx()) {
                    for (Label label : SystemLabels.values()) {
                        tx.findNodes(label, database.name(), eventContext.getDatabaseName())
                                .forEachRemaining(Node::delete);
                    }
                    tx.commit();
                } catch (Exception e) {
                    userLog.error(
                            "Failed to cleanup apoc triggers during database drop of %s, please run `apoc.trigger.dropAll` manually to cleanup: %s"
                                    .formatted(dbName, e),
                            e);
                }
            }
        }

        @Override
        public void databaseStart(DatabaseEventContext eventContext) {}

        @Override
        public void databaseShutdown(DatabaseEventContext eventContext) {}

        @Override
        public void databasePanic(DatabaseEventContext eventContext) {}

        @Override
        public void databaseCreate(DatabaseEventContext eventContext) {}
    }
}
