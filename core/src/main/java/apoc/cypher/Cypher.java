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

import static apoc.cypher.Cypher.toMap;
import static apoc.cypher.CypherUtils.runCypherQuery;
import static apoc.cypher.CypherUtils.withParamMapping;
import static apoc.util.MapUtil.map;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

import apoc.Pools;
import apoc.result.CypherStatementMapResult;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import java.io.StringReader;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
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

    @Context
    public ProcedureCallContext procedureCallContext;

    @NotThreadSafe
    @Procedure("apoc.cypher.run")
    @Description("Runs a dynamically constructed read-only statement with the given parameters.")
    public Stream<CypherStatementMapResult> run(
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "params", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return runCypherQuery(tx, statement, params, procedureCallContext);
    }

    @Procedure(name = "apoc.cypher.runMany", mode = WRITE)
    @Description("Runs each semicolon separated statement and returns a summary of the statement outcomes.")
    public Stream<RowResult> runMany(
            @Name(value = "statement", description = "The Cypher statements to run, semicolon separated (;).")
                    String cypher,
            @Name(value = "params", description = "The parameters for the given Cypher statements.")
                    Map<String, Object> params,
            @Name(value = "config", defaultValue = "{}", description = "{ statistics = true :: BOOLEAN }")
                    Map<String, Object> config) {
        boolean addStatistics = Util.toBoolean(config.getOrDefault("statistics", true));

        return Iterators.stream(new Scanner(new StringReader(cypher)).useDelimiter(";\r?\n"))
                .map(Cypher::removeShellControlCommands)
                .filter(s -> !s.isBlank())
                .flatMap(s -> streamInNewTx(s, params, addStatistics));
    }

    private Stream<Cypher.RowResult> streamInNewTx(String cypher, Map<String, Object> params, boolean stats) {
        final var innerTx = db.beginTx();
        try {
            // Hello fellow wanderer,
            // At this point you may have questions like;
            // - "Why do we execute this statement in a new transaction?"
            // My guess is as good as yours. This is the way of the apoc. Safe travels.
            final var results = new RunManyResultSpliterator(
                    innerTx.execute(Util.prefixQueryWithCheck(procedureCallContext, cypher), params), stats);
            return StreamSupport.stream(results, false).onClose(results::close).onClose(innerTx::commit);
        } catch (AuthorizationViolationException accessModeException) {
            // We meet again, few people make it this far into this world!
            // I hope you're not still seeking answers, there are few to give.
            // It has been written, in some long forgotten commits,
            // that failures of this kind should be avoided. The ancestors
            // were brave and used a regex based cypher parser to avoid
            // trying to execute schema changing statements all together.
            // We don't have that courage, and try to forget about it
            // after the fact instead.
            // One can only hope that by keeping this tradition alive,
            // in some form, we make some poor souls happier.
            innerTx.close();
            return Stream.empty();
        } catch (Throwable t) {
            innerTx.close();
            throw t;
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.cypher.runManyReadOnly", mode = READ)
    @Description("Runs each semicolon separated read-only statement and returns a summary of the statement outcomes.")
    public Stream<RowResult> runManyReadOnly(
            @Name(value = "statement", description = "The Cypher statements to run, semicolon separated (;).")
                    String cypher,
            @Name(value = "params", description = "The parameters for the given Cypher statements.")
                    Map<String, Object> params,
            @Name(value = "config", defaultValue = "{}", description = "{ statistics = true :: BOOLEAN }")
                    Map<String, Object> config) {
        return runMany(cypher, params, config);
    }

    private static final Pattern shellControl =
            Pattern.compile("^:?\\b(begin|commit|rollback)\\b", Pattern.CASE_INSENSITIVE);

    private static String removeShellControlCommands(String stmt) {
        Matcher matcher = shellControl.matcher(stmt.trim());
        if (matcher.find()) {
            // an empty file get transformed into ":begin\n:commit" and that statement is not matched by the pattern
            // because ":begin\n:commit".replaceAll("") => "\n:commit" with the recursion we avoid the problem
            return removeShellControlCommands(matcher.replaceAll(""));
        }
        return stmt;
    }

    protected static Map<String, Object> toMap(QueryStatistics stats, long time, long rows) {
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

    public record RowResult(
            @Description("The row number of the run Cypher statement.") long row,
            @Description("The result returned from the Cypher statement.") Map<String, Object> result) {}

    @Procedure(name = "apoc.cypher.doIt", mode = WRITE)
    @Description(
            "Runs a dynamically constructed statement with the given parameters. This procedure allows for both read and write statements.")
    public Stream<CypherStatementMapResult> doIt(
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "params", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return runCypherQuery(tx, statement, params, procedureCallContext);
    }

    @Procedure(name = "apoc.cypher.runWrite", mode = WRITE)
    @Description("Alias for `apoc.cypher.doIt`.")
    public Stream<CypherStatementMapResult> runWrite(
            @Name(value = "statement", description = "The Cypher statement to run.") String statement,
            @Name(value = "params", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return doIt(statement, params);
    }

    @Procedure(name = "apoc.cypher.runSchema", mode = SCHEMA)
    @Description("Runs the given query schema statement with the given parameters.")
    public Stream<CypherStatementMapResult> runSchema(
            @Name(value = "statement", description = "The Cypher schema statement to run.") String statement,
            @Name(value = "params", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return runCypherQuery(tx, statement, params, procedureCallContext);
    }

    @NotThreadSafe
    @Procedure("apoc.when")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "This procedure will run the read-only `ifQuery` if the conditional has evaluated to true, otherwise the `elseQuery` will run.")
    public Stream<CaseMapResult> whenCypher5(
            @Name(value = "condition", description = "The predicate deciding if to run the `ifQuery`or not.")
                    boolean condition,
            @Name(value = "ifQuery", description = "The Cypher statement to run if the condition is true.")
                    String ifQuery,
            @Name(
                            value = "elseQuery",
                            defaultValue = "",
                            description = "The Cypher statement to run if the condition is false.")
                    String elseQuery,
            @Name(value = "params", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return when(condition, ifQuery, elseQuery, params);
    }

    @NotThreadSafe
    @Deprecated
    @Procedure(value = "apoc.when", deprecatedBy = "Cypher's conditional queries; WHEN ... THEN.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "This procedure will run the read-only `ifQuery` if the conditional has evaluated to true, otherwise the `elseQuery` will run.")
    public Stream<CaseMapResult> when(
            @Name(value = "condition", description = "The predicate deciding if to run the `ifQuery`or not.")
                    boolean condition,
            @Name(value = "ifQuery", description = "The Cypher statement to run if the condition is true.")
                    String ifQuery,
            @Name(
                            value = "elseQuery",
                            defaultValue = "",
                            description = "The Cypher statement to run if the condition is false.")
                    String elseQuery,
            @Name(value = "params", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        if (params == null) params = Collections.emptyMap();
        String targetQuery = condition ? ifQuery : elseQuery;

        if (targetQuery.isEmpty()) {
            return Stream.of(new CaseMapResult(Collections.emptyMap()));
        } else {
            String query =
                    Util.prefixQueryWithCheck(procedureCallContext, withParamMapping(targetQuery, params.keySet()));
            return tx.execute(query, params).stream().map(CaseMapResult::new);
        }
    }

    @Procedure(value = "apoc.do.when", mode = Mode.WRITE)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Runs the given read/write `ifQuery` if the conditional has evaluated to true, otherwise the `elseQuery` will run.")
    public Stream<CaseMapResult> doWhenCypher5(
            @Name(value = "condition", description = "The predicate that determines whether to execute the `ifQuery`.")
                    boolean condition,
            @Name(value = "ifQuery", description = "The Cypher statement to run if the condition is true.")
                    String ifQuery,
            @Name(value = "elseQuery", description = "The Cypher statement to run if the condition is false.")
                    String elseQuery,
            @Name(value = "params", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return when(condition, ifQuery, elseQuery, params);
    }

    @Deprecated
    @Procedure(value = "apoc.do.when", mode = Mode.WRITE, deprecatedBy = "Cypher's conditional queries; WHEN ... THEN.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "Runs the given read/write `ifQuery` if the conditional has evaluated to true, otherwise the `elseQuery` will run.")
    public Stream<CaseMapResult> doWhen(
            @Name(value = "condition", description = "The predicate that determines whether to execute the `ifQuery`.")
                    boolean condition,
            @Name(value = "ifQuery", description = "The Cypher statement to run if the condition is true.")
                    String ifQuery,
            @Name(value = "elseQuery", description = "The Cypher statement to run if the condition is false.")
                    String elseQuery,
            @Name(value = "params", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params) {
        return when(condition, ifQuery, elseQuery, params);
    }

    public record CaseMapResult(
            @Description("The result returned from the evaluated Cypher query.") Map<String, Object> value) {
        public static final CaseMapResult EMPTY = new CaseMapResult(Collections.emptyMap());

        public static CaseMapResult empty() {
            return EMPTY;
        }
    }

    @NotThreadSafe
    @Procedure("apoc.case")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "For each pair of conditional and read-only queries in the given `LIST<ANY>`, this procedure will run the first query for which the conditional is evaluated to true. If none of the conditionals are true, the `ELSE` query will run instead.")
    public Stream<CaseMapResult> whenCaseCypher5(
            @Name(
                            value = "conditionals",
                            description =
                                    "A list of conditionals, where each conditional is a pair: the first element is a predicate, and the second is a Cypher query to be executed based on that predicate.")
                    List<Object> conditionals,
            @Name(
                            value = "elseQuery",
                            defaultValue = "",
                            description = "A Cypher query to evaluate if all conditionals evaluate to false.")
                    String elseQuery,
            @Name(
                            value = "params",
                            defaultValue = "{}",
                            description = "A map of parameters to be used in the executed Cypher query.")
                    Map<String, Object> params) {
        return whenCase(conditionals, elseQuery, params);
    }

    @NotThreadSafe
    @Deprecated
    @Procedure(value = "apoc.case", deprecatedBy = "Cypher's conditional queries; WHEN ... THEN.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "For each pair of conditional and read-only queries in the given `LIST<ANY>`, this procedure will run the first query for which the conditional is evaluated to true. If none of the conditionals are true, the `ELSE` query will run instead.")
    public Stream<CaseMapResult> whenCase(
            @Name(
                            value = "conditionals",
                            description =
                                    "A list of conditionals, where each conditional is a pair: the first element is a predicate, and the second is a Cypher query to be executed based on that predicate.")
                    List<Object> conditionals,
            @Name(
                            value = "elseQuery",
                            defaultValue = "",
                            description = "A Cypher query to evaluate if all conditionals evaluate to false.")
                    String elseQuery,
            @Name(
                            value = "params",
                            defaultValue = "{}",
                            description = "A map of parameters to be used in the executed Cypher query.")
                    Map<String, Object> params) {
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
                String query =
                        Util.prefixQueryWithCheck(procedureCallContext, withParamMapping(ifQuery, params.keySet()));
                return tx.execute(query, params).stream().map(CaseMapResult::new);
            }
        }

        if (elseQuery.isEmpty()) {
            return Stream.of(new CaseMapResult(Collections.emptyMap()));
        } else {
            String query =
                    Util.prefixQueryWithCheck(procedureCallContext, withParamMapping(elseQuery, params.keySet()));
            return tx.execute(query, params).stream().map(CaseMapResult::new);
        }
    }

    @Procedure(name = "apoc.do.case", mode = Mode.WRITE)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "For each pair of conditional queries in the given `LIST<ANY>`, this procedure will run the first query for which the conditional is evaluated to true.\n"
                    + "If none of the conditionals are true, the `ELSE` query will run instead.")
    public Stream<CaseMapResult> doWhenCaseCypher5(
            @Name(
                            value = "conditionals",
                            description =
                                    "A list of conditionals, where each conditional is a pair: the first element is a predicate, and the second is a Cypher query to be executed based on that predicate.")
                    List<Object> conditionals,
            @Name(
                            value = "elseQuery",
                            defaultValue = "",
                            description = "A Cypher query to evaluate if all conditionals evaluate to false.")
                    String elseQuery,
            @Name(
                            value = "params",
                            defaultValue = "{}",
                            description = "A map of parameters to be used in the executed Cypher query.")
                    Map<String, Object> params) {
        return whenCase(conditionals, elseQuery, params);
    }

    @Deprecated
    @Procedure(name = "apoc.do.case", mode = Mode.WRITE, deprecatedBy = "Cypher's conditional queries; WHEN ... THEN.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "For each pair of conditional queries in the given `LIST<ANY>`, this procedure will run the first query for which the conditional is evaluated to true.\n"
                    + "If none of the conditionals are true, the `ELSE` query will run instead.")
    public Stream<CaseMapResult> doWhenCase(
            @Name(
                            value = "conditionals",
                            description =
                                    "A list of conditionals, where each conditional is a pair: the first element is a predicate, and the second is a Cypher query to be executed based on that predicate.")
                    List<Object> conditionals,
            @Name(
                            value = "elseQuery",
                            defaultValue = "",
                            description = "A Cypher query to evaluate if all conditionals evaluate to false.")
                    String elseQuery,
            @Name(
                            value = "params",
                            defaultValue = "{}",
                            description = "A map of parameters to be used in the executed Cypher query.")
                    Map<String, Object> params) {
        return whenCase(conditionals, elseQuery, params);
    }
}

class RunManyResultSpliterator implements Spliterator<Cypher.RowResult>, AutoCloseable {
    private final Result result;
    private final long start;
    private boolean statistics;
    private int rowCount;

    RunManyResultSpliterator(Result result, boolean statistics) {
        this.result = result;
        this.start = System.currentTimeMillis();
        this.statistics = statistics;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Cypher.RowResult> action) {
        if (result.hasNext()) {
            action.accept(new Cypher.RowResult(rowCount++, result.next()));
            return true;
        } else if (statistics) {
            final var stats = toMap(result.getQueryStatistics(), System.currentTimeMillis() - start, rowCount);
            statistics = false;
            action.accept(new Cypher.RowResult(-1, stats));
            return true;
        }
        close();
        return false;
    }

    @Override
    public Spliterator<Cypher.RowResult> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return result.hasNext() ? Long.MAX_VALUE : 1;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED;
    }

    @Override
    public void close() {
        result.close();
    }
}
