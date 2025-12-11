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
package apoc.trigger;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.trigger.TriggerInfo.fromNode;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.Util;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TriggerHandlerNewProcedures {
    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled."
            + " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    private static boolean isEnabled() {
        return apocConfig().getBoolean(APOC_TRIGGER_ENABLED);
    }

    public static void checkEnabled() {
        if (!isEnabled()) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public static TriggerInfo install(
            GraphDatabaseAPI db,
            String databaseName,
            String triggerName,
            String statement,
            Map<String, Object> selector,
            Map<String, Object> params,
            Transaction tx) {
        final TriggerInfo result;

        Node node = Util.mergeNode(
                tx,
                SystemLabels.ApocTrigger,
                null,
                Pair.of(SystemPropertyKeys.database.name(), databaseName),
                Pair.of(SystemPropertyKeys.name.name(), triggerName));

        node.setProperty(SystemPropertyKeys.statement.name(), statement);
        node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
        node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
        node.setProperty(SystemPropertyKeys.paused.name(), false);

        // we'll return current trigger info
        result = fromNode(node, true);

        setLastUpdate(db, databaseName, tx);

        return result;
    }

    public static TriggerInfo drop(GraphDatabaseAPI db, String databaseName, String triggerName, Transaction tx) {
        final TriggerInfo[] previous = new TriggerInfo[1];

        getTriggerNodes(databaseName, tx, triggerName).forEachRemaining(node -> {
            previous[0] = fromNode(node, false);
            node.delete();
        });

        setLastUpdate(db, databaseName, tx);

        return previous[0];
    }

    public static TriggerInfo updatePaused(
            GraphDatabaseAPI db, String databaseName, String name, boolean paused, Transaction tx) {
        final TriggerInfo[] result = new TriggerInfo[1];

        getTriggerNodes(databaseName, tx, name).forEachRemaining(node -> {
            node.setProperty(SystemPropertyKeys.paused.name(), paused);

            // we'll return previous trigger info
            result[0] = fromNode(node, true);
        });

        setLastUpdate(db, databaseName, tx);

        return result[0];
    }

    public static List<TriggerInfo> dropAll(GraphDatabaseAPI db, String databaseName, Transaction tx) {
        final List<TriggerInfo> previous = new ArrayList<>();

        getTriggerNodes(databaseName, tx).forEachRemaining(node -> {
            // we'll return previous trigger info
            previous.add(fromNode(node, false));
            node.delete();
        });
        setLastUpdate(db, databaseName, tx);

        return previous;
    }

    public static Stream<TriggerInfo> getTriggerNodesList(String databaseName, Transaction tx) {
        return getTriggerNodes(databaseName, tx).stream().map(trigger -> TriggerInfo.fromNode(trigger, true));
    }

    public static ResourceIterator<Node> getTriggerNodes(String databaseName, Transaction tx) {
        return getTriggerNodes(databaseName, tx, null);
    }

    public static ResourceIterator<Node> getTriggerNodes(String databaseName, Transaction tx, String name) {
        final SystemLabels label = SystemLabels.ApocTrigger;
        final String dbNameKey = SystemPropertyKeys.database.name();
        if (name == null) {
            return tx.findNodes(label, dbNameKey, databaseName);
        }
        return tx.findNodes(label, dbNameKey, databaseName, SystemPropertyKeys.name.name(), name);
    }

    public static void setLastUpdate(GraphDatabaseAPI db, String databaseName, Transaction tx) {
        setLastUpdate(db, databaseName, tx, 0);
    }

    public static void setLastUpdate(GraphDatabaseAPI db, String databaseName, Transaction tx, int retryNumber) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName);
        if (node == null) {
            try {
                node = tx.createNode(SystemLabels.ApocTriggerMeta);
                node.setProperty(SystemPropertyKeys.database.name(), databaseName);
            } catch (ConstraintViolationException e) {
                // This can happen if two threads try to create the same node concurrently,
                // after both having passed the null check.
                // In this case we retry once or otherwise ignore the failing tx.
                if (retryNumber < 1) {
                    try (final var newTx = db.beginTx()) {
                        TriggerHandlerNewProcedures.setLastUpdate(db, databaseName, newTx, retryNumber + 1);
                        newTx.commit();
                    }
                }
                return;
            }
        }
        final long value = System.currentTimeMillis();
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), value);
    }
}
