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
package apoc.periodic;

import static apoc.util.Util.CONSUME_VOID;
import static apoc.util.Util.merge;

import apoc.Pools;
import apoc.util.Util;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.TerminationGuard;

public class PeriodicUtils {

    private PeriodicUtils() {}

    public static class JobInfo {
        @Description("The name of the job.")
        public final String name;

        @Description("The delay on the job.")
        public long delay;

        @Description("The rate of the job.")
        public long rate;

        @Description("If the job has completed.")
        public boolean done;

        @Description("If the job has been cancelled.")
        public boolean cancelled;

        public JobInfo(String name) {
            this.name = name;
        }

        public JobInfo(String name, long delay, long rate) {
            this.name = name;
            this.delay = delay;
            this.rate = rate;
        }

        public JobInfo update(Future future) {
            this.done = future.isDone();
            this.cancelled = future.isCancelled();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return this == o || o instanceof JobInfo other && name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    abstract static class ExecuteBatch implements Function<Transaction, Long> {

        protected TerminationGuard terminationGuard;
        protected BatchAndTotalCollector collector;
        private List<Map<String, Object>> batch;
        protected BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer;

        ExecuteBatch(
                TerminationGuard terminationGuard,
                BatchAndTotalCollector collector,
                List<Map<String, Object>> batch,
                BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer) {
            this.terminationGuard = terminationGuard;
            this.collector = collector;
            this.batch = batch;
            this.consumer = consumer;
        }

        protected List<Map<String, Object>> rebindBatch(Transaction tx) {
            return Util.rebindRows(tx, batch);
        }

        public void release() {
            terminationGuard = null;
            collector = null;
            batch = null;
            consumer = null;
        }
    }

    static class ListExecuteBatch extends ExecuteBatch {

        ListExecuteBatch(
                TerminationGuard terminationGuard,
                BatchAndTotalCollector collector,
                List<Map<String, Object>> batch,
                BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer) {
            super(terminationGuard, collector, batch, consumer);
        }

        @Override
        public final Long apply(Transaction txInThread) {
            if (Util.transactionIsTerminated(terminationGuard)) return 0L;
            final var batch = rebindBatch(txInThread);
            Map<String, Object> params = Util.map("_count", collector.getCount(), "_batch", batch);
            return executeAndReportErrors(txInThread, consumer, params, batch, batch.size(), null, collector);
        }
    }

    static class OneByOneExecuteBatch extends ExecuteBatch {

        OneByOneExecuteBatch(
                TerminationGuard terminationGuard,
                BatchAndTotalCollector collector,
                List<Map<String, Object>> batch,
                BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer) {
            super(terminationGuard, collector, batch, consumer);
        }

        @Override
        public final Long apply(Transaction txInThread) {
            if (Util.transactionIsTerminated(terminationGuard)) return 0L;
            AtomicLong localCount = new AtomicLong(collector.getCount());
            final var batch = rebindBatch(txInThread);
            return batch.stream()
                    .mapToLong(p -> {
                        if (localCount.get() % 1000 == 0 && Util.transactionIsTerminated(terminationGuard)) {
                            return 0;
                        }
                        Map<String, Object> params = merge(p, Util.map("_count", localCount.get(), "_batch", batch));
                        return executeAndReportErrors(txInThread, consumer, params, batch, 1, localCount, collector);
                    })
                    .sum();
        }
    }

    private static long executeAndReportErrors(
            Transaction tx,
            BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer,
            Map<String, Object> params,
            List<Map<String, Object>> batch,
            int returnValue,
            AtomicLong localCount,
            BatchAndTotalCollector collector) {
        try {
            QueryStatistics statistics = consumer.apply(tx, params);
            if (localCount != null) {
                localCount.getAndIncrement();
            }
            collector.updateStatistics(statistics);
            return returnValue;
        } catch (Exception e) {
            collector.incrementFailedOps(batch.size());
            collector.amendFailedParamsMap(batch);
            recordError(collector.getOperationErrors(), e);
            throw e;
        }
    }

    public static void recordError(Map<String, Long> executionErrors, Exception e) {
        String msg = ExceptionUtils.getRootCause(e).getMessage();
        // String msg =
        // ExceptionUtils.getThrowableList(e).stream().map(Throwable::getMessage).collect(Collectors.joining(","))
        executionErrors.compute(msg, (s, i) -> i == null ? 1 : i + 1);
    }

    public static Pair<String, Boolean> prepareInnerStatement(
            String cypherAction, BatchMode batchMode, List<String> columns, String iteratorVariableName) {
        String names = columns.stream().map(Util::quote).collect(Collectors.joining("|"));
        boolean withCheck = regNoCaseMultiLine("[{$](" + names + ")\\}?\\s+AS\\s+")
                .matcher(cypherAction)
                .find();
        if (withCheck) return Pair.of(cypherAction, false);

        switch (batchMode) {
            case SINGLE:
                return Pair.of(
                        Util.withMapping(columns.stream(), (c) -> Util.param(c) + " AS " + Util.quote(c))
                                + cypherAction,
                        false);
            case BATCH:
                if (regNoCaseMultiLine("UNWIND\\s+[{$]" + iteratorVariableName + "\\}?\\s+AS\\s+")
                        .matcher(cypherAction)
                        .find()) {
                    return Pair.of(cypherAction, true);
                }
                String with = Util.withMapping(
                        columns.stream(),
                        (c) -> Util.quote(iteratorVariableName) + "." + Util.quote(c) + " AS " + Util.quote(c));
                return Pair.of(
                        "UNWIND " + Util.param(iteratorVariableName) + " AS " + Util.quote(iteratorVariableName) + with
                                + " " + cypherAction,
                        true);
            case BATCH_SINGLE:
                return Pair.of(cypherAction, true);
            default:
                throw new IllegalArgumentException("Unrecognised batch mode: [" + batchMode + "]");
        }
    }

    public static Pattern regNoCaseMultiLine(String pattern) {
        return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
    }

    public static Stream<BatchAndTotalResult> iterateAndExecuteBatchedInSeparateThread(
            GraphDatabaseService db,
            TerminationGuard terminationGuard,
            Log log,
            Pools pools,
            int batchsize,
            boolean parallel,
            boolean iterateList,
            long retries,
            Iterator<Map<String, Object>> iterator,
            BiFunction<Transaction, Map<String, Object>, QueryStatistics> consumer,
            int concurrency,
            int failedParams,
            String periodicId) {

        ExecutorService pool = parallel ? pools.getDefaultExecutorService() : pools.getSingleExecutorService();
        List<Future<Long>> futures = new ArrayList<>(concurrency);
        BatchAndTotalCollector collector = new BatchAndTotalCollector(terminationGuard, failedParams);
        AtomicInteger activeFutures = new AtomicInteger(0);

        do {
            if (Util.transactionIsTerminated(terminationGuard)) break;

            if (activeFutures.get() < concurrency || !parallel) {
                // we have capacity, add a new Future to the list
                activeFutures.incrementAndGet();

                if (log.isDebugEnabled())
                    log.debug("Execute, in periodic iteration with id %s, no %d batch size ", periodicId, batchsize);
                List<Map<String, Object>> batch = Util.take(iterator, batchsize);
                final long currentBatchSize = batch.size();
                ExecuteBatch executeBatch = iterateList
                        ? new ListExecuteBatch(terminationGuard, collector, batch, consumer)
                        : new OneByOneExecuteBatch(terminationGuard, collector, batch, consumer);

                futures.add(Util.inTxFuture(
                        log,
                        pool,
                        db,
                        executeBatch,
                        retries,
                        retryCount -> collector.incrementRetried(),
                        onComplete -> {
                            collector.incrementBatches();
                            executeBatch.release();
                            activeFutures.decrementAndGet();
                        }));
                collector.incrementCount(currentBatchSize);
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Processed in periodic iteration with id %s, %d iterations of %d total",
                            periodicId, batchsize, collector.getCount());
                }
            } else {
                // we can't block until the counter decrease as we might miss a cancellation, so
                // let this thread be preempted for a bit before we check for cancellation or
                // capacity.
                LockSupport.parkNanos(1000);
            }
        } while (iterator.hasNext());

        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        ToLongFunction<Future<Long>> toLongFunction = wasTerminated
                ? f -> Util.getFutureOrCancel(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L)
                : f -> Util.getFuture(f, collector.getBatchErrors(), collector.getFailedBatches(), 0L);
        collector.incrementSuccesses(futures.stream().mapToLong(toLongFunction).sum());

        Util.logErrors("Error during iterate.commit:", collector.getBatchErrors(), log);
        Util.logErrors("Error during iterate.execute:", collector.getOperationErrors(), log);
        if (log.isDebugEnabled()) {
            log.debug("Terminated periodic iteration with id %s with %d executions", periodicId, collector.getCount());
        }
        return Stream.of(collector.getResult());
    }

    public static Stream<JobInfo> submitProc(
            String name, String statement, Map<String, Object> config, GraphDatabaseService db, Log log, Pools pools) {
        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
        JobInfo info = submitJob(
                name,
                () -> {
                    try {
                        db.executeTransactionally(statement, params, CONSUME_VOID);
                    } catch (Exception e) {
                        log.warn("in background task via submit", e);
                        throw new RuntimeException(e);
                    }
                },
                log,
                pools);
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public static <T> JobInfo submitJob(String name, Runnable task, Log log, Pools pools) {
        final var info = new JobInfo(name);
        var future = pools.getJobList().remove(info);
        if (future != null) future.cancel(false);

        final var newFuture = pools.getScheduledExecutorService().submit(wrapTask(name, task, log));
        future = pools.getJobList().put(info, newFuture);
        if (future != null) future.cancel(false);
        return info;
    }

    public static Runnable wrapTask(String name, Runnable task, Log log) {
        return () -> {
            log.debug("Executing task " + name);
            try {
                task.run();
            } catch (Exception e) {
                log.error(
                        "Error while executing task " + name
                                + " because of the following exception (the task will be killed):",
                        e);
                throw e;
            }
            log.debug("Executed task " + name);
        };
    }
}

/*
a batchMode variable where:
* single -> call 2nd statement individually but in one tx (currently iterateList: false)
* batch -> prepend UNWIND _batch to 2nd statement (currently iterateList: true)
* batch_single -> pass _batch through to 2nd statement
 */
