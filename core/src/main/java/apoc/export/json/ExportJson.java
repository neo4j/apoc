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
package apoc.export.json;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ExportProgressInfo;
import apoc.util.Util;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

public class ExportJson {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ProcedureCallContext procedureCallContext;

    @NotThreadSafe
    @Procedure("apoc.export.json.all")
    @Description("Exports the full database to the provided JSON file.")
    public Stream<ExportProgressInfo> all(
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = false :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportJson(fileName, source, DatabaseSubGraph::new, config);
    }

    @NotThreadSafe
    @Procedure("apoc.export.json.data")
    @Description("Exports the given `NODE` and `RELATIONSHIP` values to the provided JSON file.")
    public Stream<ExportProgressInfo> data(
            @Name(value = "nodes", description = "A list of nodes to export.") List<Node> nodesIn,
            @Name(value = "rels", description = "A list of relationships to export.") List<Relationship> relsIn,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = false :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        // initialize empty lists if nodes or rels are null
        final var nodes = nodesIn == null ? Collections.<Node>emptyList() : nodesIn;
        final var rels = relsIn == null ? Collections.<Relationship>emptyList() : relsIn;

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportJson(fileName, source, threadTx -> new NodesAndRelsSubGraph(threadTx, nodes, rels, true), config);
    }

    @NotThreadSafe
    @Procedure("apoc.export.json.graph")
    @Description("Exports the given graph to the provided JSON file.")
    public Stream<ExportProgressInfo> graph(
            @Name(value = "graph", description = "The graph to export.") Map<String, Object> graph,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = false :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {

        final Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        final Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportJson(fileName, source, threadTx -> new NodesAndRelsSubGraph(threadTx, nodes, rels), config);
    }

    @NotThreadSafe
    @Procedure("apoc.export.json.query")
    @Description("Exports the results from the Cypher statement to the provided JSON file.")
    public Stream<ExportProgressInfo> query(
            @Name(value = "statement", description = "The query used to collect the data for export.") String query,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = false :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        Map<String, Object> params = config == null
                ? Collections.emptyMap()
                : (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(Util.prefixQueryWithCheck(procedureCallContext, query), params);
        String source = String.format("statement: cols(%d)", result.columns().size());
        return exportJson(fileName, source, threadTx -> threadTx.execute(Util.prefixQueryWithCheck(procedureCallContext, query), params), config);
    }

    private Stream<ExportProgressInfo> exportJson(
            String fileName, String source, Function<Transaction, Object> data, Map<String, Object> config) {
        final var exportConfig = new ExportConfig(config);
        apocConfig.checkWriteAllowed(exportConfig, fileName);
        final String format = "json";

        final var progressInfo = new ExportProgressInfo(fileName, source, format);
        final var exporter = new JsonFormat(db, getJsonFormat(config));
        if (exporter.supportsBatching()) {
            progressInfo.batchSize = exportConfig.getBatchSize();
        }

        final var reporter = new ProgressReporter(null, null, progressInfo);

        final var cypherFileManager = FileManagerFactory.createFileManager(fileName, false, exportConfig);
        if (exportConfig.streamStatements()) {
            return ExportUtils.getProgressInfoStream(
                    db,
                    pools.getDefaultExecutorService(),
                    terminationGuard,
                    format,
                    exportConfig,
                    reporter,
                    cypherFileManager,
                    (threadBoundTx, reporterWithConsumer) ->
                            dump(data.apply(threadBoundTx), exportConfig, reporterWithConsumer, exporter, cypherFileManager));
        } else {
            dump(data.apply(tx), exportConfig, reporter, exporter, cypherFileManager);
            return Stream.of((ExportProgressInfo) reporter.getTotal());
        }
    }

    private JsonFormat.Format getJsonFormat(Map<String, Object> config) {
        if (config == null) {
            return JsonFormat.Format.JSON_LINES;
        }
        final String jsonFormat = config.getOrDefault("jsonFormat", JsonFormat.Format.JSON_LINES.toString())
                .toString()
                .toUpperCase();
        return JsonFormat.Format.valueOf(jsonFormat);
    }

    private void dump(
            Object data,
            ExportConfig c,
            ProgressReporter reporter,
            JsonFormat exporter,
            ExportFileManager cypherFileManager) {
        try {
            if (data instanceof SubGraph) exporter.dump(((SubGraph) data), cypherFileManager, reporter, c);
            if (data instanceof Result) exporter.dump(((Result) data), cypherFileManager, reporter, c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
