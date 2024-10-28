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
package apoc.export.csv;

import static apoc.export.cypher.FileManagerFactory.createFileManager;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.export.cypher.ExportFileManager;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportFormat;
import apoc.export.util.ExportUtils;
import apoc.export.util.ProgressReporter;
import apoc.result.ExportProgressInfo;
import apoc.util.Util;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.cypher.export.ExportData;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
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
public class ExportCSV {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    public ExportCSV() {}

    @NotThreadSafe
    @Procedure("apoc.export.csv.all")
    @Description("Exports the full database to the provided CSV file.")
    public Stream<ExportProgressInfo> all(
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = false :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            quotes = 'always' :: ['always', 'none', 'ifNeeded'],
                            differentiateNulls = false :: BOOLEAN,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportCsv(fileName, source, new ExportData.Database(), new ExportConfig(config, ExportFormat.CSV));
    }

    @NotThreadSafe
    @Procedure("apoc.export.csv.data")
    @Description("Exports the given `NODE` and `RELATIONSHIP` values to the provided CSV file.")
    public Stream<ExportProgressInfo> data(
            @Name(value = "nodes", description = "A list of nodes to export.") List<Node> nodes,
            @Name(value = "rels", description = "A list of relationships to export.") List<Relationship> rels,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            quotes = 'always' :: ['always', 'none', 'ifNeeded'],
                            differentiateNulls = false :: BOOLEAN,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        ExportConfig exportConfig = new ExportConfig(config, ExportFormat.CSV);
        preventBulkImport(exportConfig);
        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCsv(fileName, source, new ExportData.NodesAndRels(nodes, rels), exportConfig);
    }

    @NotThreadSafe
    @Procedure("apoc.export.csv.graph")
    @Description("Exports the given graph to the provided CSV file.")
    public Stream<ExportProgressInfo> graph(
            @Name(value = "graph", description = "The graph to export.") Map<String, Object> graph,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            bulkImport = false :: BOOLEAN,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None' :: STRING,
                            charset = 'UTF_8' :: STRING,
                            quotes = 'always' :: ['always', 'none', 'ifNeeded'],
                            differentiateNulls = false :: BOOLEAN,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportCsv(
                fileName, source, new ExportData.NodesAndRels(nodes, rels), new ExportConfig(config, ExportFormat.CSV));
    }

    @NotThreadSafe
    @Procedure("apoc.export.csv.query")
    @Description("Exports the results from running the given Cypher query to the provided CSV file.")
    public Stream<ExportProgressInfo> query(
            @Name(value = "query", description = "The query used to collect the data for export.") String query,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            batchSize = 20000 :: INTEGER,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'None':: STRING,
                            charset = 'UTF_8' :: STRING,
                            quotes = 'always' :: ['always', 'none', 'ifNeeded'],
                            differentiateNulls = false :: BOOLEAN,
                            sampling = false :: BOOLEAN,
                            samplingConfig :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        ExportConfig exportConfig = new ExportConfig(config, ExportFormat.CSV);
        preventBulkImport(exportConfig);
        Map<String, Object> params = config == null
                ? Collections.emptyMap()
                : (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());

        final String source;
        try (final var result = tx.execute(query, params)) {
            source = String.format("statement: cols(%d)", result.columns().size());
        }

        return exportCsv(fileName, source, new ExportData.Query(query, params), exportConfig);
    }

    private void preventBulkImport(ExportConfig config) {
        if (config.isBulkImport()) {
            throw new RuntimeException(
                    "You can use the `bulkImport` only with apoc.export.csv.all and apoc.export.csv.graph");
        }
    }

    private Stream<ExportProgressInfo> exportCsv(
            @Name("file") String fileName, String source, ExportData data, ExportConfig exportConfig) {
        apocConfig.checkWriteAllowed(exportConfig, fileName);
        final String format = "csv";
        ExportProgressInfo progressInfo = new ExportProgressInfo(fileName, source, format);
        progressInfo.batchSize = exportConfig.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        CsvFormat exporter = new CsvFormat(db, exportConfig);

        ExportFileManager cypherFileManager = createFileManager(fileName, exportConfig.isBulkImport(), exportConfig);

        if (exportConfig.streamStatements()) {
            return ExportUtils.getProgressInfoStream(
                    db,
                    pools.getDefaultExecutorService(),
                    terminationGuard,
                    format,
                    exportConfig,
                    reporter,
                    cypherFileManager,
                    (threadBoundTx, reporterWithConsumer) -> exporter.dump(
                            (InternalTransaction) threadBoundTx, data, cypherFileManager, reporterWithConsumer, true));
        } else {
            exporter.dump((InternalTransaction) tx, data, cypherFileManager, reporter, false);
            return Stream.of((ExportProgressInfo) reporter.getTotal());
        }
    }
}
