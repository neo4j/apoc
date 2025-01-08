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

import static apoc.util.Util.toBoolean;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import apoc.Pools;
import apoc.result.CypherStatementMapResult;
import apoc.util.Util;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

/**
 * @author mh
 * @since 20.02.18
 */
public class Timeboxed {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    private static final Map<String, Object> POISON = Collections.singletonMap("__magic", "POISON");

    @NotThreadSafe
    @Procedure("apoc.cypher.runTimeboxed")
    @Description("Terminates a Cypher statement if it has not finished before the set timeout (ms).")
    public Stream<CypherStatementMapResult> runTimeboxed(
            @Name(value = "statement", description = "The Cypher statement to run.") String cypher,
            @Name(value = "params", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params,
            @Name(value = "timeout", description = "The maximum time, in milliseconds, the statement can run for.")
                    long timeout,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description = "{ failOnError = false :: BOOLEAN, appendStatusRow = false :: BOOLEAN }")
                    Map<String, Object> config) {

        final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100);
        final AtomicReference<Transaction> txAtomic = new AtomicReference<>();

        boolean failOnError = toBoolean(config.get("failOnError"));
        boolean appendStatusRow = toBoolean(config.get("appendStatusRow"));

        // run query to be timeboxed in a separate thread to enable proper tx termination
        // if we'd run this in current thread, a tx.terminate would kill the transaction the procedure call uses itself.
        pools.getDefaultExecutorService().submit(() -> {
            try (Transaction innerTx = db.beginTx()) {
                txAtomic.set(innerTx);
                Result result = innerTx.execute(cypher, params == null ? Collections.EMPTY_MAP : params);
                while (result.hasNext()) {
                    if (Util.transactionIsTerminated(terminationGuard)) {
                        txAtomic.get().close();
                        offerToQueue(queue, POISON, timeout);
                        return;
                    }

                    final Map<String, Object> map = result.next();
                    offerToQueue(queue, map, timeout);
                }
                if (appendStatusRow) {
                    Map<String, Object> map = statusMap(true, false, null);
                    offerToQueue(queue, map, timeout);
                }
                innerTx.commit();
            } catch (TransactionTerminatedException e) {
                log.warn("query " + cypher + " has been terminated");
                if (appendStatusRow || failOnError) {
                    Map<String, Object> map = statusMap(false, true, null);
                    offerToQueue(queue, map, timeout);
                }
            } catch (QueryExecutionException e) {
                if (appendStatusRow || failOnError) {
                    Map<String, Object> map = statusMap(false, false, e.getMessage());
                    offerToQueue(queue, map, timeout);
                }
            } finally {
                offerToQueue(queue, POISON, timeout);
                txAtomic.set(null);
            }
        });

        //
        pools.getScheduledExecutorService()
                .schedule(
                        () -> {
                            Transaction tx = txAtomic.get();
                            if (tx == null) {
                                log.debug(
                                        "tx is null, either the other transaction finished gracefully or has not yet been start.");
                            } else {
                                if (appendStatusRow || failOnError) {
                                    Map<String, Object> map = statusMap(false, true, null);
                                    offerToQueue(queue, map, timeout);
                                }
                                tx.terminate();
                                offerToQueue(queue, POISON, timeout);
                                log.warn("terminating transaction, putting POISON onto queue");
                            }
                        },
                        timeout,
                        MILLISECONDS);

        // consume the blocking queue using a custom iterator finishing upon POISON
        Iterator<Map<String, Object>> queueConsumer = new Iterator<>() {
            Map<String, Object> nextElement = null;
            boolean hasFinished = false;

            @Override
            public boolean hasNext() {
                if (hasFinished) {
                    return false;
                } else {
                    try {
                        nextElement = queue.poll(timeout, MILLISECONDS);
                        if (nextElement == null) {
                            // Wait a little bit longer and try again, waiting exactly the timeout means that
                            // there might be a slight timing issue with termination vs. setting the queue as terminated
                            nextElement = queue.poll(100, MILLISECONDS);
                            // If it is still null, then accept and move on
                            if (nextElement == null) {
                                log.warn("Empty queue, aborting.");
                                if (failOnError) {
                                    throw new RuntimeException("The query has been terminated.");
                                }
                                hasFinished = true;
                            }
                        }

                        if (failOnError && nextElement.get("wasSuccessful").equals(Boolean.FALSE)) {
                            if (nextElement.get("failedWithError").equals(Boolean.TRUE)) {
                                throw new RuntimeException("The inner query errored with: " + nextElement.get("error"));
                            }
                            if (nextElement.get("wasTerminated").equals(Boolean.TRUE)) {
                                throw new RuntimeException("The query has been terminated.");
                            }
                        } else {
                            hasFinished = POISON.equals(nextElement);
                        }
                        return !hasFinished;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public Map<String, Object> next() {
                return nextElement;
            }
        };

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(queueConsumer, Spliterator.ORDERED), false)
                .map(CypherStatementMapResult::new);
    }

    private Map<String, Object> statusMap(boolean successful, boolean terminated, String errorMessage) {
        Map<String, Object> map = new HashMap<>();
        map.put("wasSuccessful", successful ? Boolean.TRUE : Boolean.FALSE);
        map.put("wasTerminated", terminated ? Boolean.TRUE : Boolean.FALSE);
        map.put("failedWithError", errorMessage == null ? Boolean.FALSE : Boolean.TRUE);
        map.put("error", errorMessage);
        return map;
    }

    private void offerToQueue(BlockingQueue<Map<String, Object>> queue, Map<String, Object> map, long timeout) {
        try {
            boolean hasBeenAdded = queue.offer(map, timeout, MILLISECONDS);
            if (!hasBeenAdded) {
                throw new IllegalStateException("couldn't add a value to a queue of size " + queue.size()
                        + ". Either increase capacity or fix consumption of the queue");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
