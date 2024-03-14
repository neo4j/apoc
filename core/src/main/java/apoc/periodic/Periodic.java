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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class Periodic {

    enum Planner {
        DEFAULT,
        COST,
        IDP,
        DP
    }

    public static final Pattern PLANNER_PATTERN =
            Pattern.compile("\\bplanner\\s*=\\s*[^\\s]*", Pattern.CASE_INSENSITIVE);
    public static final Pattern RUNTIME_PATTERN = Pattern.compile("\\bruntime\\s*=", Pattern.CASE_INSENSITIVE);
    public static final Pattern CYPHER_PREFIX_PATTERN = Pattern.compile("^\\s*\\bcypher\\b", Pattern.CASE_INSENSITIVE);
    public static final String CYPHER_RUNTIME_SLOTTED = " runtime=slotted ";
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

    @Admin
    @Procedure(name = "apoc.periodic.truncate", mode = Mode.SCHEMA)
    @Description(
            "Removes all entities (and optionally indexes and constraints) from the database using the `apoc.periodic.iterate` procedure.")
    public void truncate(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

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
            @Name("statement") String statement,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> parameters) {
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
        public final long updates;
        public final long executions;
        public final long runtime;
        public final long batches;
        public final long failedBatches;
        public final Map<String, Long> batchErrors;
        public final long failedCommits;
        public final Map<String, Long> commitErrors;
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
        return db.executeTransactionally(statement, parameters, result -> {
            String column = Iterables.single(result.columns());
            return result.columnAs(column).stream().mapToLong(o -> (long) o).sum();
        });
    }

    @Procedure("apoc.periodic.cancel")
    @Description("Cancels the given background job.")
    public Stream<JobInfo> cancel(@Name("name") String name) {
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
            @Name("name") String name,
            @Name("statement") String statement,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> config) {
        validateQuery(statement);
        return submitProc(name, statement, config, db, log, pools);
    }

    @Procedure(name = "apoc.periodic.repeat", mode = Mode.WRITE)
    @Description("Runs a repeatedly called background job.\n" + "To stop this procedure, use `apoc.periodic.cancel`.")
    public Stream<JobInfo> repeat(
            @Name("name") String name,
            @Name("statement") String statement,
            @Name("rate") long rate,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        validateQuery(statement);
        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
        JobInfo info = schedule(
                name,
                () -> {
                    // `resultAsString` in order to consume result
                    db.executeTransactionally(statement, params, Result::resultAsString);
                },
                0,
                rate);
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
            @Name("name") String name, @Name("statement") String statement, @Name("delay") long delay) {
        validateQuery(statement);
        JobInfo info = submitJob(name, new Countdown(name, statement, delay, log), log, pools);
        info.delay = delay;
        return Stream.of(info);
    }

    /**
     * Call from a procedure that gets a <code>@Context GraphDatbaseAPI db;</code> injected and provide that db to the runnable.
     */
    public JobInfo schedule(String name, Runnable task, long delay, long repeat) {
        JobInfo info = new JobInfo(name, delay, repeat);
        Future future = pools.getJobList().remove(info);
        if (future != null && !future.isDone()) future.cancel(false);

        Runnable wrappingTask = wrapTask(name, task, log);
        ScheduledFuture<?> newFuture = pools.getScheduledExecutorService()
                .scheduleWithFixedDelay(wrappingTask, delay, repeat, TimeUnit.SECONDS);
        pools.getJobList().put(info, newFuture);
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
            @Name("cypherIterate") String cypherIterate,
            @Name("cypherAction") String cypherAction,
            @Name("config") Map<String, Object> config) {
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
        if(tx instanceof InternalTransaction iTx){
            metaData = iTx.kernelTransaction().getMetaData();
        } else {
            metaData = Map.of();
        }

        BatchMode batchMode = BatchMode.fromConfig(config);
        Map<String, Object> params = (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());

        try (Result result = tx.execute(slottedRuntime(cypherIterate), params)) {
            Pair<String, Boolean> prepared =
                    PeriodicUtils.prepareInnerStatement(cypherAction, batchMode, result.columns(), "_batch");
            String innerStatement = applyPlanner(prepared.getLeft(), Planner.valueOf((String)
                    config.getOrDefault("planner", Planner.DEFAULT.name())));
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
                        if(tx instanceof InternalTransaction iTx){
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

    static String slottedRuntime(String cypherIterate) {
        if (RUNTIME_PATTERN.matcher(cypherIterate).find()) {
            return cypherIterate;
        }

        return prependQueryOption(cypherIterate, CYPHER_RUNTIME_SLOTTED);
    }

    public static String applyPlanner(String query, Planner planner) {
        if (planner.equals(Planner.DEFAULT)) {
            return query;
        }
        Matcher matcher = PLANNER_PATTERN.matcher(query);
        String cypherPlanner = String.format(" planner=%s ", planner.name().toLowerCase());
        if (matcher.find()) {
            return matcher.replaceFirst(cypherPlanner);
        }
        return prependQueryOption(query, cypherPlanner);
    }

    private static String prependQueryOption(String query, String cypherOption) {
        String cypherPrefix = "cypher";
        String completePrefix = cypherPrefix + cypherOption;
        return CYPHER_PREFIX_PATTERN.matcher(query).find()
                ? query.replaceFirst("(?i)" + cypherPrefix, completePrefix)
                : completePrefix + query;
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
