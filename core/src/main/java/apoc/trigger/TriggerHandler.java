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

import static apoc.ApocConfig.apocConfig;
import static apoc.SystemLabels.ApocTrigger;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.MapUtil;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class TriggerHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    private enum Phase {
        before,
        after,
        rollback,
        afterAsync
    }

    private static final Map<String, Object> TRIGGER_META = Map.of("apoc.trigger", true);

    public static final String TRIGGER_REFRESH = "apoc.trigger.refresh";

    // Snapshot of installed triggers. The containing map is immutable.
    private final AtomicReference<Map<String, Map<String, Object>>> triggersSnapshot = new AtomicReference<>(Map.of());

    private final Log log;
    private final GraphDatabaseService db;
    private final DatabaseManagementService databaseManagementService;
    private final ApocConfig apocConfig;
    private final Pools pools;
    private final JobScheduler jobScheduler;

    private volatile long lastUpdate;

    private JobHandle restoreTriggerHandler;

    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    public TriggerHandler(
            GraphDatabaseService db,
            DatabaseManagementService databaseManagementService,
            ApocConfig apocConfig,
            Log log,
            Pools pools,
            JobScheduler jobScheduler) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
        this.log = log;
        this.pools = pools;
        this.jobScheduler = jobScheduler;
    }

    public void updateCache() {
        try {
            doUpdateCache();
        } catch (Exception e) {
            log.error("Failed to update apoc triggers: " + e.getMessage(), e);
        }
    }

    private void doUpdateCache() {
        var attempt = 5;
        while (attempt > 0) {
            final var start = System.currentTimeMillis();
            final var oldTriggers = triggersSnapshot.get();
            final var newTriggers = getTriggers();
            if (triggersSnapshot.compareAndSet(oldTriggers, newTriggers)) {
                lastUpdate = start;
                reconcileKernelRegistration();
                break;
            }
            --attempt;
        }
    }

    private Map<String, Map<String, Object>> getTriggers() {
        return withSystemDb(tx -> {
            final var dbName = db.databaseName();
            return tx.findNodes(ApocTrigger, SystemPropertyKeys.database.name(), dbName).stream()
                    .collect(Collectors.toUnmodifiableMap(
                            node -> (String) node.getProperty(SystemPropertyKeys.name.name()),
                            node -> MapUtil.map(
                                    "statement",
                                    node.getProperty(SystemPropertyKeys.statement.name()),
                                    "selector",
                                    Util.fromJson(
                                            (String) node.getProperty(SystemPropertyKeys.selector.name()), Map.class),
                                    "params",
                                    Util.fromJson(
                                            (String) node.getProperty(SystemPropertyKeys.params.name()), Map.class),
                                    "paused",
                                    node.getProperty(SystemPropertyKeys.paused.name()))));
        });
    }

    /**
     * There is substantial memory overhead to the kernel event system, so if a user has enabled apoc triggers in
     * config, but there are no triggers set up, unregister to let the kernel bypass the event handling system.
     *
     * For most deployments this isn't an issue, since you can turn the config flag off, but in large fleet deployments
     * it's nice to have uniform config, and then the memory savings on databases that don't use triggers is good.
     */
    private void reconcileKernelRegistration() {
        // Register if there are triggers
        if (!triggersSnapshot.get().isEmpty()) {
            // This gets called every time triggers update; only register if we aren't already
            if (registeredWithKernel.compareAndSet(false, true)) {
                databaseManagementService.registerTransactionEventListener(db.databaseName(), this);
            }
        } else {
            // This gets called every time triggers update; only unregister if we aren't already
            if (registeredWithKernel.compareAndSet(true, false)) {
                databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
            }
        }
    }

    public Map<String, Object> add(
            String name, String statement, Map<String, Object> selector, Map<String, Object> params) {
        final var previous = triggersSnapshot.get().get(name);

        withSystemDb(tx -> {
            Node node = Util.mergeNode(
                    tx,
                    ApocTrigger,
                    null,
                    Pair.of(SystemPropertyKeys.database.name(), db.databaseName()),
                    Pair.of(SystemPropertyKeys.name.name(), name));
            node.setProperty(SystemPropertyKeys.statement.name(), statement);
            node.setProperty(SystemPropertyKeys.selector.name(), Util.toJson(selector));
            node.setProperty(SystemPropertyKeys.params.name(), Util.toJson(params));
            node.setProperty(SystemPropertyKeys.paused.name(), false);
            setLastUpdate(tx);
            return null;
        });

        updateCache();
        return previous;
    }

    public Map<String, Object> remove(String name) {
        final var previous = triggersSnapshot.get().get(name);

        withSystemDb(tx -> {
            tx.findNodes(
                            ApocTrigger,
                            SystemPropertyKeys.database.name(),
                            db.databaseName(),
                            SystemPropertyKeys.name.name(),
                            name)
                    .forEachRemaining(node -> node.delete());
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return previous;
    }

    public Map<String, Object> updatePaused(String name, boolean paused) {
        withSystemDb(tx -> {
            tx.findNodes(
                            ApocTrigger,
                            SystemPropertyKeys.database.name(),
                            db.databaseName(),
                            SystemPropertyKeys.name.name(),
                            name)
                    .forEachRemaining(node -> node.setProperty(SystemPropertyKeys.paused.name(), paused));
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return triggersSnapshot.get().get(name);
    }

    public Map<String, Map<String, Object>> removeAll() {
        final var previous = triggersSnapshot.get();
        withSystemDb(tx -> {
            tx.findNodes(ApocTrigger, SystemPropertyKeys.database.name(), db.databaseName())
                    .forEachRemaining(Node::delete);
            setLastUpdate(tx);
            return null;
        });
        updateCache();
        return previous;
    }

    public Map<String, Map<String, Object>> list() {
        return triggersSnapshot.get();
    }

    @Override
    public Void beforeCommit(TransactionData txData, Transaction transaction, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.before)) {
            executeTriggers(transaction, txData, Phase.before);
        }
        return null;
    }

    @Override
    public void afterCommit(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        // if `txData.metaData()` is equal to TRIGGER_META,
        // it means that the transaction comes from another TriggerHandler transaction,
        // therefore the execution must be blocked to prevent a deadlock due to cascading transactions
        if (isTransactionCreatedByTrigger(txData)) {
            return;
        }

        if (hasPhase(Phase.after)) {
            try (Transaction tx = db.beginTx()) {
                setTriggerMetadata(tx);
                executeTriggers(tx, txData, Phase.after);
                tx.commit();
            }
        }
        afterAsync(txData);
    }

    private static boolean isTransactionCreatedByTrigger(TransactionData txData) {
        Map<String, Object> metaData = txData.metaData();
        return metaData.equals(TRIGGER_META);
    }

    private void afterAsync(TransactionData txData) {
        if (hasPhase(Phase.afterAsync)) {
            TriggerMetadata triggerMetadata = TriggerMetadata.from(txData, true);
            Util.inTxFuture(pools.getDefaultExecutorService(), db, (inner) -> {
                setTriggerMetadata(inner);
                executeTriggers(inner, triggerMetadata.rebind(inner), Phase.afterAsync);
                return null;
            });
        }
    }

    private static void setTriggerMetadata(Transaction tx) {
        tx.execute("CALL tx.setMetaData($data)", Map.of("data", TRIGGER_META));
    }

    @Override
    public void afterRollback(TransactionData txData, Void state, GraphDatabaseService databaseService) {
        if (hasPhase(Phase.rollback)) {
            try (Transaction tx = db.beginTx()) {
                executeTriggers(tx, txData, Phase.rollback);
                tx.commit();
            }
        }
    }

    private boolean hasPhase(Phase phase) {
        return triggersSnapshot.get().values().stream()
                .map(data -> (Map<String, Object>) data.get("selector"))
                .anyMatch(selector -> when(selector, phase));
    }

    private void executeTriggers(Transaction tx, TransactionData txData, Phase phase) {
        executeTriggers(tx, TriggerMetadata.from(txData, false), phase);
    }

    private void executeTriggers(Transaction tx, TriggerMetadata triggerMetadata, Phase phase) {
        Map<String, String> exceptions = new LinkedHashMap<>();
        triggersSnapshot.get().forEach((name, data) -> {
            Map<String, Object> params = triggerMetadata.toMap();
            if (data.get("params") != null) {
                params.putAll((Map<String, Object>) data.get("params"));
            }
            Map<String, Object> selector = (Map<String, Object>) data.get("selector");
            if ((!(boolean) data.get("paused")) && when(selector, phase)) {
                try {
                    params.put("trigger", name);
                    Result result = tx.execute((String) data.get("statement"), params);
                    Iterators.count(result);
                } catch (Exception e) {
                    log.warn("Error executing trigger " + name + " in phase " + phase, e);
                    exceptions.put(name, e.getMessage());
                }
            }
        });
        if (!exceptions.isEmpty()) {
            throw new RuntimeException("Error executing triggers " + exceptions.toString());
        }
    }

    private boolean when(Map<String, Object> selector, Phase phase) {
        if (selector == null) return phase == Phase.before;
        return Phase.valueOf(selector.getOrDefault("phase", "before").toString()) == phase;
    }

    @Override
    public void start() {
        updateCache();
        long refreshInterval = apocConfig().getInt(TRIGGER_REFRESH, 60000);
        restoreTriggerHandler = jobScheduler.scheduleRecurring(
                Group.STORAGE_MAINTENANCE,
                () -> {
                    if (getLastUpdate() >= lastUpdate) {
                        updateCache();
                    }
                },
                refreshInterval,
                refreshInterval,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (registeredWithKernel.compareAndSet(true, false)) {
            databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
        }
        if (restoreTriggerHandler != null) {
            restoreTriggerHandler.cancel();
        }
    }

    private <T> T withSystemDb(Function<Transaction, T> action) {
        var timeout = 500;

        // When the timeout reaches 12 hours, we will have been trying for 24 hours - time to give up
        var upperTimeout = 43200000;

        return Util.withBackOffRetries(
                () -> {
                    Transaction tx = apocConfig.getSystemDb().beginTx();
                    T result = action.apply(tx);
                    tx.commit();
                    return result;
                },
                timeout,
                upperTimeout,
                log);
    }

    private long getLastUpdate() {
        return withSystemDb(tx -> {
            Node node =
                    tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
            return node == null ? 0L : (long) node.getProperty(SystemPropertyKeys.lastUpdated.name());
        });
    }

    private void setLastUpdate(Transaction tx) {
        Node node = tx.findNode(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), db.databaseName());
        if (node == null) {
            node = tx.createNode(SystemLabels.ApocTriggerMeta);
            node.setProperty(SystemPropertyKeys.database.name(), db.databaseName());
        }
        node.setProperty(SystemPropertyKeys.lastUpdated.name(), System.currentTimeMillis());
    }
}
