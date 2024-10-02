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
package apoc.export.cypher;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.util.ExportConfig;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.DataProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypher {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @NotThreadSafe
    @Procedure("apoc.export.cypher.all")
    @Description(
            "Exports the full database (incl. indexes) as Cypher statements to the provided file (default: Cypher Shell).")
    public Stream<DataProgressInfo> all(
            @Name(
                            value = "file",
                            defaultValue = "",
                            description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = true :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        if (Util.isNullOrEmpty(fileName)) fileName = null;
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportCypher(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config), false);
    }

    @NotThreadSafe
    @Procedure("apoc.export.cypher.data")
    @Description(
            "Exports the given `NODE` and `RELATIONSHIP` values (incl. indexes) as Cypher statements to the provided file (default: Cypher Shell).")
    public Stream<DataProgressInfo> data(
            @Name(value = "nodes", description = "A list of nodes to export.") List<Node> nodes,
            @Name(value = "rels", description = "A list of relationships to export.") List<Relationship> rels,
            @Name(
                            value = "file",
                            defaultValue = "",
                            description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = true :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        if (Util.isNullOrEmpty(fileName)) fileName = null;
        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(
                fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config), false);
    }

    @NotThreadSafe
    @Procedure("apoc.export.cypher.graph")
    @Description(
            "Exports the given graph (incl. indexes) as Cypher statements to the provided file (default: Cypher Shell).")
    public Stream<DataProgressInfo> graph(
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
                            bulkImport = true :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        if (Util.isNullOrEmpty(fileName)) fileName = null;

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCypher(
                fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config), false);
    }

    @NotThreadSafe
    @Procedure("apoc.export.cypher.query")
    @Description(
            "Exports the `NODE` and `RELATIONSHIP` values from the given Cypher query (incl. indexes) as Cypher statements to the provided file (default: Cypher Shell).")
    public Stream<DataProgressInfo> query(
            @Name(value = "statement", description = "The query used to collect the data for export.") String query,
            @Name(
                            value = "file",
                            defaultValue = "",
                            description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = true :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        if (Util.isNullOrEmpty(fileName)) fileName = null;
        ExportConfig c = new ExportConfig(config);
        Result result = tx.execute(query);
        SubGraph graph;
        graph = CypherResultSubGraph.from(tx, result, c.getRelsInBetween(), false);
        String source = String.format(
                "statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportCypher(fileName, source, graph, c, false);
    }

    @NotThreadSafe
    @Procedure("apoc.export.cypher.schema")
    @Description("Exports all schema indexes and constraints to Cypher statements.")
    public Stream<DataProgressInfo> schema(
            @Name(
                            value = "file",
                            defaultValue = "",
                            description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = true :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        if (Util.isNullOrEmpty(fileName)) fileName = null;
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportCypher(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config), true);
    }

    private Stream<DataProgressInfo> exportCypher(
            @Name("file") String fileName, String source, SubGraph graph, ExportConfig c, boolean onlySchema) {
        apocConfig.checkWriteAllowed(c, fileName);

        DataProgressInfo progressInfo = new DataProgressInfo(fileName, source, "cypher");
        progressInfo.batchSize = c.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        boolean separatedFiles = !onlySchema && c.separateFiles();
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, separatedFiles, c);

        if (c.streamStatements()) {
            long timeout = c.getTimeoutSeconds();
            final BlockingQueue<DataProgressInfo> queue = new ArrayBlockingQueue<>(1000);
            ProgressReporter reporterWithConsumer = reporter.withConsumer((pi) -> QueueUtil.put(
                    queue,
                    pi == DataProgressInfo.EMPTY
                            ? DataProgressInfo.EMPTY
                            : new DataProgressInfo((DataProgressInfo) pi).enrich(cypherFileManager),
                    timeout));
            Util.inTxFuture(
                    null,
                    pools.getDefaultExecutorService(),
                    db,
                    txInThread -> {
                        doExport(graph, c, onlySchema, reporterWithConsumer, cypherFileManager);
                        return true;
                    },
                    0,
                    _ignored -> {},
                    _ignored -> QueueUtil.put(queue, DataProgressInfo.EMPTY, timeout));
            QueueBasedSpliterator<DataProgressInfo> spliterator =
                    new QueueBasedSpliterator<>(queue, DataProgressInfo.EMPTY, terminationGuard, Integer.MAX_VALUE);
            return StreamSupport.stream(spliterator, false);
        } else {
            doExport(graph, c, onlySchema, reporter, cypherFileManager);
            return reporter.stream().map(pi -> (DataProgressInfo) pi).map((dpi) -> dpi.enrich(cypherFileManager));
        }
    }

    private void doExport(
            SubGraph graph,
            ExportConfig c,
            boolean onlySchema,
            ProgressReporter reporter,
            ExportFileManager cypherFileManager) {
        MultiStatementCypherSubGraphExporter exporter = new MultiStatementCypherSubGraphExporter(graph, c, db);

        if (onlySchema) exporter.exportOnlySchema(cypherFileManager, reporter, c);
        else exporter.export(c, reporter, cypherFileManager);
    }
}
