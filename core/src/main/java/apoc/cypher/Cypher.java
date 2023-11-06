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

import static apoc.cypher.CypherUtils.runCypherQuery;
import static apoc.cypher.CypherUtils.withParamMapping;
import static apoc.util.MapUtil.map;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.EntityUtil;
import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

/**
 * @author mh
 * @since 08.05.16
 */
public class Cypher {

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Pools pools;

    @NotThreadSafe
    @Procedure("apoc.cypher.run")
    @Description("Runs a dynamically constructed read-only statement with the given parameters.")
    public Stream<MapResult> run(@Name("statement") String statement, @Name("params") Map<String, Object> params) {
        return runCypherQuery(tx, statement, params);
    }

    private Stream<RowResult> runManyStatements(
            Reader reader,
            Map<String, Object> params,
            boolean schemaOperation,
            boolean addStatistics,
            int queueCapacity) {
        BlockingQueue<RowResult> queue = runInSeparateThreadAndSendTombstone(
                queueCapacity,
                internalQueue -> {
                    if (schemaOperation) {
                        runSchemaStatementsInTx(reader, internalQueue, params, addStatistics);
                    } else {
                        runDataStatementsInTx(reader, internalQueue, params, addStatistics);
                    }
                },
                RowResult.TOMBSTONE);
        return StreamSupport.stream(
                new QueueBasedSpliterator<>(queue, RowResult.TOMBSTONE, terminationGuard, Integer.MAX_VALUE), false);
    }

    private <T> BlockingQueue<T> runInSeparateThreadAndSendTombstone(
            int queueCapacity, Consumer<BlockingQueue<T>> action, T tombstone) {
        /* NB: this must not be called via an existing thread pool - otherwise we could run into a deadlock
          other jobs using the same pool might completely exhaust at and the thread sending TOMBSTONE will
          wait in the pool's job queue.
        */
        BlockingQueue<T> queue = new ArrayBlockingQueue<>(queueCapacity);
        Util.newDaemonThread(() -> {
                    try {
                        action.accept(queue);
                    } finally {
                        while (true) { // ensure we send TOMBSTONE even if there's an InterruptedException
                            try {
                                queue.put(tombstone);
                                return;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                })
                .start();
        return queue;
    }

    private void runDataStatementsInTx(
            Reader reader, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics) {
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter(";\r?\n");
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            if (!isSchemaOperation(stmt)) {
                if (isPeriodicOperation(stmt)) {
                    Util.inThread(
                            pools,
                            () -> db.executeTransactionally(
                                    stmt, params, result -> consumeResult(result, queue, addStatistics)));
                } else {
                    Util.inTx(db, pools, threadTx -> {
                        try (Result result = threadTx.execute(stmt, params)) {
                            return consumeResult(result, queue, addStatistics);
                        }
                    });
                }
            }
        }
    }

    private void runSchemaStatementsInTx(
            Reader reader, BlockingQueue<RowResult> queue, Map<String, Object> params, boolean addStatistics) {
        Scanner scanner = new Scanner(reader);
        scanner.useDelimiter(";\r?\n");
        while (scanner.hasNext()) {
            String stmt = removeShellControlCommands(scanner.next());
            if (stmt.trim().isEmpty()) continue;
            if (isSchemaOperation(stmt)) {
                Util.inTx(db, pools, txInThread -> {
                    try (Result result = txInThread.execute(stmt, params)) {
                        return consumeResult(result, queue, addStatistics);
                    }
                });
            }
        }
    }

    @Procedure(name = "apoc.cypher.runMany", mode = WRITE)
    @Description("Runs each semicolon separated statement and returns a summary of the statement outcomes.")
    public Stream<RowResult> runMany(
            @Name("statement") String cypher,
            @Name("params") Map<String, Object> params,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics", true));
        int queueCapacity = Util.toInteger(config.getOrDefault("queueCapacity", 100));

        StringReader stringReader = new StringReader(cypher);
        return runManyStatements(stringReader, params, false, addStatistics, queueCapacity);
    }

    @NotThreadSafe
    @Procedure(name = "apoc.cypher.runManyReadOnly", mode = READ)
    @Description("Runs each semicolon separated read-only statement and returns a summary of the statement outcomes.")
    public Stream<RowResult> runManyReadOnly(
            @Name("statement") String cypher,
            @Name("params") Map<String, Object> params,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return runMany(cypher, params, config);
    }

    private static final Pattern shellControl =
            Pattern.compile("^:?\\b(begin|commit|rollback)\\b", Pattern.CASE_INSENSITIVE);

    private Object consumeResult(Result result, BlockingQueue<RowResult> queue, boolean addStatistics) {
        try {
            long time = System.currentTimeMillis();
            int row = 0;
            while (result.hasNext()) {
                terminationGuard.check();
                Map<String, Object> mapResult = EntityUtil.anyRebind(tx, result.next());
                queue.put(new RowResult(row++, mapResult));
            }
            if (addStatistics) {
                queue.put(
                        new RowResult(-1, toMap(result.getQueryStatistics(), System.currentTimeMillis() - time, row)));
            }
            return row;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String removeShellControlCommands(String stmt) {
        Matcher matcher = shellControl.matcher(stmt.trim());
        if (matcher.find()) {
            // an empty file get transformed into ":begin\n:commit" and that statement is not matched by the pattern
            // because ":begin\n:commit".replaceAll("") => "\n:commit" with the recursion we avoid the problem
            return removeShellControlCommands(matcher.replaceAll(""));
        }
        return stmt;
    }

    private boolean isSchemaOperation(String stmt) {
        return stmt.matches("(?is).*(create|drop)\\s+(index|constraint).*");
    }

    private boolean isPeriodicOperation(String stmt) {
        return stmt.matches("(?is).*using\\s+periodic.*");
    }

    private Map<String, Object> toMap(QueryStatistics stats, long time, long rows) {
        final Map<String, Object> map = map(
                "rows", rows,
                "time", time);
        map.putAll(toMap(stats));
        return map;
    }

    public static Map<String, Object> toMap(QueryStatistics stats) {
        return map(
                "nodesCreated", stats.getNodesCreated(),
                "nodesDeleted", stats.getNodesDeleted(),
                "labelsAdded", stats.getLabelsAdded(),
                "labelsRemoved", stats.getLabelsRemoved(),
                "relationshipsCreated", stats.getRelationshipsCreated(),
                "relationshipsDeleted", stats.getRelationshipsDeleted(),
                "propertiesSet", stats.getPropertiesSet(),
                "constraintsAdded", stats.getConstraintsAdded(),
                "constraintsRemoved", stats.getConstraintsRemoved(),
                "indexesAdded", stats.getIndexesAdded(),
                "indexesRemoved", stats.getIndexesRemoved());
    }

    public static class RowResult {
        public static final RowResult TOMBSTONE = new RowResult(-1, null);
        public long row;
        public Map<String, Object> result;

        public RowResult(long row, Map<String, Object> result) {
            this.row = row;
            this.result = result;
        }
    }

    @Procedure(name = "apoc.cypher.doIt", mode = WRITE)
    @Description(
            "Runs a dynamically constructed statement with the given parameters. This procedure allows for both read and write statements.")
    public Stream<MapResult> doIt(@Name("statement") String statement, @Name("params") Map<String, Object> params) {
        return runCypherQuery(tx, statement, params);
    }

    @Procedure(name = "apoc.cypher.runWrite", mode = WRITE)
    @Description("Alias for `apoc.cypher.doIt`.")
    public Stream<MapResult> runWrite(@Name("statement") String statement, @Name("params") Map<String, Object> params) {
        return doIt(statement, params);
    }

    @Procedure(name = "apoc.cypher.runSchema", mode = SCHEMA)
    @Description("Runs the given query schema statement with the given parameters.")
    public Stream<MapResult> runSchema(
            @Name("statement") String statement, @Name("params") Map<String, Object> params) {
        return runCypherQuery(tx, statement, params);
    }

    @Procedure("apoc.when")
    @Description(
            "This procedure will run the read-only `ifQuery` if the conditional has evaluated to true, otherwise the `elseQuery` will run.")
    public Stream<MapResult> when(
            @Name("condition") boolean condition,
            @Name("ifQuery") String ifQuery,
            @Name(value = "elseQuery", defaultValue = "") String elseQuery,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        String targetQuery = condition ? ifQuery : elseQuery;

        if (targetQuery.isEmpty()) {
            return Stream.of(new MapResult(Collections.emptyMap()));
        } else {
            return tx.execute(withParamMapping(targetQuery, params.keySet()), params).stream()
                    .map(MapResult::new);
        }
    }

    @Procedure(value = "apoc.do.when", mode = Mode.WRITE)
    @Description(
            "Runs the given read/write `ifQuery` if the conditional has evaluated to true, otherwise the `elseQuery` will run.")
    public Stream<MapResult> doWhen(
            @Name("condition") boolean condition,
            @Name("ifQuery") String ifQuery,
            @Name(value = "elseQuery", defaultValue = "") String elseQuery,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        return when(condition, ifQuery, elseQuery, params);
    }

    @Procedure("apoc.case")
    @Description(
            "For each pair of conditional and read-only queries in the given `LIST<ANY>`, this procedure will run the first query for which the conditional is evaluated to true. If none of the conditionals are true, the `ELSE` query will run instead.")
    public Stream<MapResult> whenCase(
            @Name("conditionals") List<Object> conditionals,
            @Name(value = "elseQuery", defaultValue = "") String elseQuery,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();

        if (conditionals.size() % 2 != 0) {
            throw new IllegalArgumentException(
                    "Conditionals must be an even-sized collection of boolean, query entries");
        }

        Iterator caseItr = conditionals.iterator();

        while (caseItr.hasNext()) {
            boolean condition = (Boolean) caseItr.next();
            String ifQuery = (String) caseItr.next();

            if (condition) {
                return tx.execute(withParamMapping(ifQuery, params.keySet()), params).stream()
                        .map(MapResult::new);
            }
        }

        if (elseQuery.isEmpty()) {
            return Stream.of(new MapResult(Collections.emptyMap()));
        } else {
            return tx.execute(withParamMapping(elseQuery, params.keySet()), params).stream()
                    .map(MapResult::new);
        }
    }

    @Procedure(name = "apoc.do.case", mode = Mode.WRITE)
    @Description(
            "For each pair of conditional queries in the given `LIST<ANY>`, this procedure will run the first query for which the conditional is evaluated to true.\n"
                    + "If none of the conditionals are true, the `ELSE` query will run instead.")
    public Stream<MapResult> doWhenCase(
            @Name("conditionals") List<Object> conditionals,
            @Name(value = "elseQuery", defaultValue = "") String elseQuery,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params) {
        return whenCase(conditionals, elseQuery, params);
    }
}
