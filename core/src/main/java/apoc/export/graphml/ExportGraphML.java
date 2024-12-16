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
package apoc.export.graphml;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.cypher.export.CypherResultSubGraph;
import apoc.cypher.export.DatabaseSubGraph;
import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.export.util.CountingReader;
import apoc.export.util.ExportConfig;
import apoc.export.util.ExportUtils;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.ProgressReporter;
import apoc.result.ExportProgressInfo;
import apoc.result.ImportProgressInfo;
import apoc.util.FileUtils;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportGraphML {
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public ApocConfig apocConfig;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure(name = "apoc.import.graphml", mode = Mode.WRITE)
    @Description("Imports a graph from the provided GraphML file.")
    public Stream<ImportProgressInfo> file(
            @Name(
                            value = "urlOrBinaryFile",
                            description = "The name of the file or binary data to import the data from.")
                    Object urlOrBinaryFile,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        readLabels = false :: BOOLEAN,
                        defaultRelationshipType = "RELATED" :: STRING,
                        storeNodeIds = false :: BOOLEAN,
                        batchSize = 20000 :: INTEGER,
                        compression = "NONE" :: ["NONE", "BYTES", "GZIP", "BZIP2", "DEFLATE", "BLOCK_LZ4", "FRAMED_SNAPPY"],
                        source = {} :: MAP,
                        target = {} :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        ImportProgressInfo result = Util.inThread(pools, () -> {
            ExportConfig exportConfig = new ExportConfig(config);
            String file = null;
            String source = "binary";
            if (urlOrBinaryFile instanceof String) {
                file = (String) urlOrBinaryFile;
                source = "file";
            }
            ProgressReporter reporter =
                    new ProgressReporter(null, null, new ImportProgressInfo(file, source, "graphml"));
            XmlGraphMLReader graphMLReader = new XmlGraphMLReader(db)
                    .reporter(reporter)
                    .batchSize(exportConfig.getBatchSize())
                    .relType(exportConfig.defaultRelationshipType())
                    .source(exportConfig.getSource())
                    .target(exportConfig.getTarget())
                    .nodeLabels(exportConfig.readLabels());

            if (exportConfig.storeNodeIds()) graphMLReader.storeNodeIds();

            try (CountingReader reader =
                    FileUtils.readerFor(urlOrBinaryFile, exportConfig.getCompressionAlgo(), urlAccessChecker)) {
                graphMLReader.parseXML(reader, terminationGuard);
            }

            return (ImportProgressInfo) reporter.getTotal();
        });
        return Stream.of(result);
    }

    @NotThreadSafe
    @Procedure("apoc.export.graphml.all")
    @Description("Exports the full database to the provided GraphML file.")
    public Stream<ExportProgressInfo> all(
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            format = 'cypher-shell' :: STRING,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'NONE' :: ['NONE', 'BYTES', 'GZIP', 'BZIP2', 'DEFLATE', 'BLOCK_LZ4', 'FRAMED_SNAPPY'],
                            charset = 'UTF_8' :: STRING,
                            source :: MAP,
                            target :: MAP,
                            useTypes :: BOOLEAN,
                            caption :: LIST<STRING>
                    }
                    """)
                    Map<String, Object> config)
            throws Exception {

        String source = String.format("database: nodes(%d), rels(%d)", Util.nodeCount(tx), Util.relCount(tx));
        return exportGraphML(fileName, source, new DatabaseSubGraph(tx), new ExportConfig(config));
    }

    @Procedure("apoc.export.graphml.data")
    @Description("Exports the given `NODE` and `RELATIONSHIP` values to the provided GraphML file.")
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
                            format = 'cypher-shell' :: STRING,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'NONE' :: ['NONE', 'BYTES', 'GZIP', 'BZIP2', 'DEFLATE', 'BLOCK_LZ4', 'FRAMED_SNAPPY'],
                            charset = 'UTF_8' :: STRING,
                            source :: MAP,
                            target :: MAP,
                            useTypes :: BOOLEAN,
                            caption :: LIST<STRING>
                    }
                    """)
                    Map<String, Object> config)
            throws Exception {

        String source = String.format("data: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportGraphML(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config));
    }

    @Procedure("apoc.export.graphml.graph")
    @Description("Exports the given graph to the provided GraphML file.")
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
                            format = 'cypher-shell' :: STRING,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'NONE' :: ['NONE', 'BYTES', 'GZIP', 'BZIP2', 'DEFLATE', 'BLOCK_LZ4', 'FRAMED_SNAPPY'],
                            charset = 'UTF_8' :: STRING,
                            source :: MAP,
                            target :: MAP,
                            useTypes :: BOOLEAN,
                            caption :: LIST<STRING>
                    }
                    """)
                    Map<String, Object> config)
            throws Exception {

        Collection<Node> nodes = (Collection<Node>) graph.get("nodes");
        Collection<Relationship> rels = (Collection<Relationship>) graph.get("relationships");
        String source = String.format("graph: nodes(%d), rels(%d)", nodes.size(), rels.size());
        return exportGraphML(fileName, source, new NodesAndRelsSubGraph(tx, nodes, rels), new ExportConfig(config));
    }

    @NotThreadSafe
    @Procedure("apoc.export.graphml.query")
    @Description(
            "Exports the given `NODE` and `RELATIONSHIP` values from the Cypher statement to the provided GraphML file.")
    public Stream<ExportProgressInfo> query(
            @Name(value = "statement", description = "The query used to collect the data for export.") String query,
            @Name(value = "file", description = "The name of the file to which the data will be exported.")
                    String fileName,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                            stream = false :: BOOLEAN,
                            format = 'cypher-shell' :: STRING,
                            timeoutSeconds = 100 :: INTEGER,
                            compression = 'NONE' :: ['NONE', 'BYTES', 'GZIP', 'BZIP2', 'DEFLATE', 'BLOCK_LZ4', 'FRAMED_SNAPPY'],
                            charset = 'UTF_8' :: STRING,
                            source :: MAP,
                            target :: MAP,
                            useTypes :: BOOLEAN,
                            caption :: LIST<STRING>,
                            nodesOfRelationships = false :: BOOLEAN
                    }
                    """)
                    Map<String, Object> config)
            throws Exception {
        ExportConfig c = new ExportConfig(config);
        Result result = tx.execute(query);
        SubGraph graph = CypherResultSubGraph.from(tx, result, c.getRelsInBetween(), false);
        String source = String.format(
                "statement: nodes(%d), rels(%d)",
                Iterables.count(graph.getNodes()), Iterables.count(graph.getRelationships()));
        return exportGraphML(fileName, source, graph, c);
    }

    private Stream<ExportProgressInfo> exportGraphML(
            @Name("file") String fileName, String source, SubGraph graph, ExportConfig exportConfig) throws Exception {
        apocConfig.checkWriteAllowed(exportConfig, fileName);
        final String format = "graphml";
        ProgressReporter reporter = new ProgressReporter(null, null, new ExportProgressInfo(fileName, source, format));
        XmlGraphMLWriter exporter = new XmlGraphMLWriter();
        ExportFileManager cypherFileManager = FileManagerFactory.createFileManager(fileName, false, exportConfig);
        final PrintWriter graphMl = cypherFileManager.getPrintWriter(format);
        if (exportConfig.streamStatements()) {
            return ExportUtils.getProgressInfoStream(
                    db,
                    pools.getDefaultExecutorService(),
                    terminationGuard,
                    format,
                    exportConfig,
                    reporter,
                    cypherFileManager,
                    (threadBoundTx, reporterWithConsumer) -> {
                        try {
                            exporter.write(graph, graphMl, reporterWithConsumer, exportConfig);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            exporter.write(graph, graphMl, reporter, exportConfig);
            closeWriter(graphMl);
            return Stream.of((ExportProgressInfo) reporter.getTotal());
        }
    }

    private void closeWriter(PrintWriter writer) {
        writer.flush();
        try {
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
