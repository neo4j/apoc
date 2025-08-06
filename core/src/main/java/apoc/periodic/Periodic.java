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

import static apoc.periodic.PeriodicUtils.recordError;
import static apoc.periodic.PeriodicUtils.submitJob;
import static apoc.periodic.PeriodicUtils.submitProc;
import static apoc.periodic.PeriodicUtils.wrapTask;
import static apoc.util.Util.CONSUME_VOID;
import static apoc.util.Util.merge;
import static org.neo4j.graphdb.QueryExecutionType.QueryType;

import apoc.Pools;
import apoc.periodic.PeriodicUtils.JobInfo;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class Periodic {

    static final Pattern LIMIT_PATTERN = Pattern.compile("\\slimit\\s", Pattern.CASE_INSENSITIVE);

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    @Context
    public Pools pools;

    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    @Admin
    @Procedure(name = "apoc.periodic.truncate", mode = Mode.SCHEMA)
    @Description(
            "Removes all entities (and optionally indexes and constraints) from the database using the `apoc.periodic.iterate` procedure.")
    public void truncate(
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
            {
                dropSchema = true :: BOOLEAN,
                batchSize = 10000 :: INTEGER,
                parallel = false :: BOOLEAN,
                retries = 0 :: INTEGER,
                batchMode = "BATCH" :: STRING,
                params = {} :: MAP,
                concurrency :: INTEGER,
                failedParams = -1 :: INTEGER,
                planner = "DEFAULT" :: ["DEFAULT", "COST", "IDP", "DP"]
            }
            """)
                    Map<String, Object> config) {

        iterate("MATCH ()-[r]->() RETURN id(r) as id", "MATCH ()-[r]->() WHERE id(r) = id DELETE r", config);
        iterate("MATCH (n) RETURN id(n) as id", "MATCH (n) WHERE id(n) = id DELETE n", config);

        if (Util.toBoolean(config.get("dropSchema"))) {
            Schema schema = tx.schema();
            schema.getConstraints().forEach(ConstraintDefinition::drop);
            Util.getIndexes(tx).forEach(IndexDefinition::drop);
        }
    }

    @Procedure("apoc.periodic.list")
    @Description("Returns a `LIST<ANY>` of all background jobs.")
    public Stream<JobInfo> list() {
        return pools.getJobList().entrySet().stream().map((e) -> e.getKey().update(e.getValue()));
    }

    @Procedure(name = "apoc.periodic.commit", mode = Mode.WRITE)
    @Description("Runs the given statement in separate batched transactions.")
    public Stream<RundownResult> commit(
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "params", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> parameters) {
        validateQuery(statement);
        Map<String, Object> params = parameters == null ? Collections.emptyMap() : parameters;
        long total = 0, executions = 0, updates = 0;
        long start = System.nanoTime();

        if (!LIMIT_PATTERN.matcher(statement).find()) {
            throw new IllegalArgumentException("the statement sent to apoc.periodic.commit must contain a `limit`");
        }

        AtomicInteger batches = new AtomicInteger();
        AtomicInteger failedCommits = new AtomicInteger();
        Map<String, Long> commitErrors = new ConcurrentHashMap<>();
        AtomicInteger failedBatches = new AtomicInteger();
        Map<String, Long> batchErrors = new ConcurrentHashMap<>();
        String periodicId = UUID.randomUUID().toString();
        if (log.isDebugEnabled()) {
            log.debug("Starting periodic commit from `%s` in separate thread with id: `%s`", statement, periodicId);
        }
        do {
            Map<String, Object> window = Util.map("_count", updates, "_total", total);
            updates = Util.getFuture(
                    pools.getScheduledExecutorService().submit(() -> {
                        batches.incrementAndGet();
                        try {
                            return executeNumericResultStatement(statement, merge(window, params));
                        } catch (Exception e) {
                            failedBatches.incrementAndGet();
                            recordError(batchErrors, e);
                            return 0L;
                        }
                    }),
                    commitErrors,
                    failedCommits,
                    0L);
            total += updates;
            if (updates > 0) executions++;
            if (log.isDebugEnabled()) {
                log.debug("Processed in periodic commit with id %s, no %d executions", periodicId, executions);
            }
        } while (updates > 0 && !Util.transactionIsTerminated(terminationGuard));
        if (log.isDebugEnabled()) {
            log.debug("Terminated periodic commit with id %s with %d executions", periodicId, executions);
        }
        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        boolean wasTerminated = Util.transactionIsTerminated(terminationGuard);
        return Stream.of(new RundownResult(
                total,
                executions,
                timeTaken,
                batches.get(),
                failedBatches.get(),
                batchErrors,
                failedCommits.get(),
                commitErrors,
                wasTerminated));
    }

    public static class RundownResult {
        @Description("The total number of updates.")
        public final long updates;

        @Description("The total number of executions.")
        public final long executions;

        @Description("The total time taken in nanoseconds.")
        public final long runtime;

        @Description("The number of run batches.")
        public final long batches;

        @Description("The number of failed batches.")
        public final long failedBatches;

        @Description("Errors returned from the failed batches.")
        public final Map<String, Long> batchErrors;

        @Description("The number of failed commits.")
        public final long failedCommits;

        @Description("Errors returned from the failed commits.")
        public final Map<String, Long> commitErrors;

        @Description("If the job was terminated.")
        public final boolean wasTerminated;

        public RundownResult(
                long total,
                long executions,
                long timeTaken,
                long batches,
                long failedBatches,
                Map<String, Long> batchErrors,
                long failedCommits,
                Map<String, Long> commitErrors,
                boolean wasTerminated) {
            this.updates = total;
            this.executions = executions;
            this.runtime = timeTaken;
            this.batches = batches;
            this.failedBatches = failedBatches;
            this.batchErrors = batchErrors;
            this.failedCommits = failedCommits;
            this.commitErrors = commitErrors;
            this.wasTerminated = wasTerminated;
        }
    }

    private long executeNumericResultStatement(
            @Name("statement") String statement, @Name("params") Map<String, Object> parameters) {
        return db.executeTransactionally(
                Util.prefixQueryWithCheck(procedureCallContext, statement), parameters, result -> {
                    String column = Iterables.single(result.columns());
                    return result.columnAs(column).stream()
                            .mapToLong(o -> (long) o)
                            .sum();
                });
    }

    @Procedure("apoc.periodic.cancel")
    @Description("Cancels the given background job.")
    public Stream<JobInfo> cancel(@Name(value = "name", description = "The name of the job to cancel.") String name) {
        JobInfo info = new JobInfo(name);
        Future future = pools.getJobList().remove(info);
        if (future != null) {
            future.cancel(false);
            return Stream.of(info.update(future));
        }
        return Stream.empty();
    }

    @Procedure(name = "apoc.periodic.submit", mode = Mode.WRITE)
    @Description("Creates a background job which runs the given Cypher statement once.")
    public Stream<JobInfo> submit(
            @Name(value = "name", description = "The name of the job.") String name,
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "params", defaultValue = "{}", description = "{ params = {} :: MAP }")
                    Map<String, Object> config) {
        validateQuery(statement);
        var query = Util.prefixQueryWithCheck(procedureCallContext, statement);
        return submitProc(name, query, config, db, log, pools);
    }

    @Procedure(name = "apoc.periodic.repeat", mode = Mode.WRITE)
    @Description("Runs a repeatedly called background job. To stop this procedure, use `apoc.periodic.cancel`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    public Stream<JobInfo> repeatCypher5(
            @Name(value = "name", description = "The name of the job.") String name,
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "rate", description = "The delay in seconds to wait between each job execution.") long rate,
            @Name(value = "config", defaultValue = "{}", description = "{ params = {} :: MAP }")
                    Map<String, Object> config) {
        validateQuery(statement);
        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
        final var query = Util.prefixQueryWithCheck(procedureCallContext, statement);
        JobInfo info = schedule(name, () -> db.executeTransactionally(query, params, CONSUME_VOID), 0, rate, true);
        return Stream.of(info);
    }

    @Procedure(name = "apoc.periodic.repeat", mode = Mode.WRITE)
    @Description("Runs a repeatedly called background job. To stop this procedure, use `apoc.periodic.cancel`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<JobInfo> repeat(
            @Name(value = "name", description = "The name of the job.") String name,
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "rate", description = "The delay in seconds to wait between each job execution.") long rate,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description = "{ params = {} :: MAP, cancelOnError = true :: BOOLEAN }")
                    Map<String, Object> config) {
        validateQuery(statement);
        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
        Boolean cancelOnError = (Boolean) config.getOrDefault("cancelOnError", true);
        final var query = Util.prefixQueryWithCheck(procedureCallContext, statement);
        JobInfo info =
                schedule(name, () -> db.executeTransactionally(query, params, CONSUME_VOID), 0, rate, cancelOnError);
        return Stream.of(info);
    }

    private void validateQuery(String statement) {
        Util.validateQuery(
                db,
                statement,
                Set.of(Mode.WRITE, Mode.READ, Mode.DEFAULT),
                QueryType.READ_ONLY,
                QueryType.WRITE,
                QueryType.READ_WRITE);
    }

    @Procedure(name = "apoc.periodic.countdown", mode = Mode.WRITE)
    @Description("Runs a repeatedly called background statement until it returns 0.")
    public Stream<JobInfo> countdown(
            @Name(value = "name", description = "The name of the job.") String name,
            @Name(
                            value = "statement",
                            description =
                                    "The Cypher statement to run, returning a count on each run indicating the remaining iterations.")
                    String statement,
            @Name(value = "delay", description = "The delay in seconds to wait between each job execution.")
                    long delay) {
        validateQuery(statement);
        var query = Util.prefixQueryWithCheck(procedureCallContext, statement);
        JobInfo info = submitJob(name, new Countdown(name, query, delay, log), log, pools);
        info.delay = delay;
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public JobInfo schedule(String name, Runnable task, long delay, long repeat, boolean cancelOnError) {
        final var info = new JobInfo(name, delay, repeat);

        var future = pools.getJobList().remove(info);
        if (future != null) future.cancel(false);

        final var newFuture = pools.getScheduledExecutorService()
                .scheduleWithFixedDelay(wrapTask(name, task, log, cancelOnError), delay, repeat, TimeUnit.SECONDS);
        future = pools.getJobList().put(info, newFuture);
        if (future != null) future.cancel(false);

        return info;
    }

    /**
     * Invoke cypherAction in batched transactions being fed from cypherIteration running in main thread
     * @param cypherIterate
     * @param cypherAction
     */
    @Procedure(name = "apoc.periodic.iterate", mode = Mode.WRITE)
    @Description("Runs the second statement for each item returned by the first statement.\n"
            + "This procedure returns the number of batches and the total number of processed rows.")
    public Stream<BatchAndTotalResult> iterate(
            @Name(value = "cypherIterate", description = "The first Cypher statement to be run.") String cypherIterate,
            @Name(
                            value = "cypherAction",
                            description =
                                    "The Cypher statement to run for each item returned by the initial Cypher statement.")
                    String cypherAction,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        batchSize = 10000 :: INTEGER,
                        parallel = false :: BOOLEAN,
                        retries = 0 :: INTEGER,
                        batchMode = "BATCH" :: STRING,
                        params = {} :: MAP,
                        concurrency :: INTEGER,
                        failedParams = -1 :: INTEGER,
                        planner = "DEFAULT" :: ["DEFAULT", "COST", "IDP", "DP"]
                    }
                    """)
                    Map<String, Object> config) {
        validateQuery(cypherIterate);

        long batchSize = Util.toLong(config.getOrDefault("batchSize", 10000));
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize parameter must be > 0");
        }
        int concurrency = Util.toInteger(
                config.getOrDefault("concurrency", Runtime.getRuntime().availableProcessors()));
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency parameter must be > 0");
        }
        boolean parallel = Util.toBoolean(config.getOrDefault("parallel", false));
        long retries = Util.toLong(config.getOrDefault(
                "retries", 0)); // todo sleep/delay or push to end of batch to try again or immediate ?
        int failedParams = Util.toInteger(config.getOrDefault("failedParams", -1));

        final Map<String, Object> metaData;
        if (tx instanceof InternalTransaction iTx) {
            metaData = iTx.kernelTransaction().getMetaData();
        } else {
            metaData = Map.of();
        }

        BatchMode batchMode = BatchMode.fromConfig(config);
        Map<String, Object> params = (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());

        try (Result result = tx.execute(
                Util.slottedRuntime(cypherIterate, Util.getCypherVersionString(procedureCallContext)), params)) {
            Pair<String, Boolean> prepared =
                    PeriodicUtils.prepareInnerStatement(cypherAction, batchMode, result.columns(), "_batch");
            String innerStatement = Util.applyPlanner(
                    prepared.getLeft(),
                    Util.Planner.valueOf((String) config.getOrDefault("planner", Util.Planner.DEFAULT.name())),
                    Util.getCypherVersionString(procedureCallContext));
            boolean iterateList = prepared.getRight();
            String periodicId = UUID.randomUUID().toString();
            if (log.isDebugEnabled()) {
                log.debug(
                        "Starting periodic iterate from `%s` operation using iteration `%s` in separate thread with id: `%s`",
                        cypherIterate, cypherAction, periodicId);
            }
            return PeriodicUtils.iterateAndExecuteBatchedInSeparateThread(
                    db,
                    terminationGuard,
                    log,
                    pools,
                    (int) batchSize,
                    parallel,
                    iterateList,
                    retries,
                    result,
                    (tx, p) -> {
                        if (tx instanceof InternalTransaction iTx) {
                            iTx.setMetaData(metaData);
                        }
                        final Result r = tx.execute(innerStatement, merge(params, p));
                        Iterators.count(r); // XXX: consume all results
                        return r.getQueryStatistics();
                    },
                    concurrency,
                    failedParams,
                    periodicId);
        }
    }

    private class Countdown implements Runnable {
        private final String name;
        private final String statement;
        private final long delay;
        private final transient Log log;

        public Countdown(String name, String statement, long delay, Log log) {
            this.name = name;
            this.statement = statement;
            this.delay = delay;
            this.log = log;
        }

        @Override
        public void run() {
            if (Periodic.this.executeNumericResultStatement(statement, Collections.emptyMap()) > 0) {
                pools.getScheduledExecutorService()
                        .schedule(() -> submitJob(name, this, log, pools), delay, TimeUnit.SECONDS);
            }
        }
    }
}
