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

import static apoc.export.util.BulkImportUtil.formatHeader;
import static apoc.export.util.MetaInformation.collectPropTypesForNodes;
import static apoc.export.util.MetaInformation.collectPropTypesForRelationships;
import static apoc.export.util.MetaInformation.getLabelsString;
import static apoc.export.util.MetaInformation.updateKeyTypes;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static apoc.util.Util.getNodeId;
import static apoc.util.Util.getRelationshipId;
import static apoc.util.Util.joinLabels;

import apoc.export.cypher.ExportFileManager;
import apoc.export.util.ExportConfig;
import apoc.export.util.FormatUtils;
import apoc.export.util.MetaInformation;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.export.util.Reporter;
import com.opencsv.CSVWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.ExportData;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

/**
 * @author mh
 * @since 22.11.16
 */
public class CsvFormat {
    private final GraphDatabaseService db;
    private final ExportConfig config;
    private final boolean applyQuotesToAll;

    private static final String[] NODE_HEADER_FIXED_COLUMNS = {"_id:id", "_labels:label"};
    private static final String[] REL_HEADER_FIXED_COLUMNS = {"_start:id", "_end:id", "_type:label"};

    public CsvFormat(GraphDatabaseService db, ExportConfig exportConfig) {
        this.db = db;
        this.config = exportConfig;
        this.applyQuotesToAll = !ExportConfig.NO_QUOTES.equals(exportConfig.isQuotes())
                && !ExportConfig.IF_NEEDED_QUOTES.equals(exportConfig.isQuotes());
    }

    public void dump(
            InternalTransaction threadBoundTx,
            ExportData data,
            ExportFileManager writer,
            Reporter reporter,
            boolean needsRebind) {
        if (data instanceof ExportData.Database) {
            dump(threadBoundTx, new DatabaseSubGraph(threadBoundTx), writer, reporter);
        } else if (data instanceof ExportData.NodesAndRels e) {
            dump(
                    threadBoundTx,
                    new NodesAndRelsSubGraph(threadBoundTx, e.nodes(), e.rels(), needsRebind),
                    writer,
                    reporter);
        } else if (data instanceof ExportData.Query e) {
            dump(threadBoundTx, e, writer, reporter);
        }
    }

    private void dump(InternalTransaction threadBoundTx, SubGraph graph, ExportFileManager writer, Reporter reporter) {
        if (config.isBulkImport()) {
            writeAllBulkImport(threadBoundTx, graph, reporter, writer);
        } else {
            try (PrintWriter printWriter = writer.getPrintWriter("csv")) {
                CSVWriter out = getCsvWriter(printWriter);
                writeAll(threadBoundTx, graph, reporter, out);
            }
        }
        reporter.done();
    }

    private CSVWriter getCsvWriter(Writer writer) {
        return switch (config.isQuotes()) {
            case ExportConfig.NO_QUOTES -> new CustomCSVWriter(
                    writer,
                    config.getDelimChar(),
                    '\0', // quote char
                    '\0', // escape char
                    CSVWriter.DEFAULT_LINE_END,
                    config.shouldDifferentiateNulls());
            default -> new CustomCSVWriter(
                    writer,
                    config.getDelimChar(),
                    ExportConfig.QUOTECHAR,
                    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                    CSVWriter.DEFAULT_LINE_END,
                    config.shouldDifferentiateNulls());
        };
    }

    private void dump(
            InternalTransaction threadBoundTx, ExportData.Query query, ExportFileManager writer, Reporter reporter) {
        try (final var result = threadBoundTx.execute(query.cypher(), query.params());
                final var printWriter = writer.getPrintWriter("csv")) {
            CSVWriter out = getCsvWriter(printWriter);
            String[] header = writeResultHeader(result, out);

            String[] data = new String[header.length];
            result.accept((row) -> {
                for (int col = 0; col < header.length; col++) {
                    String key = header[col];
                    Object value = row.get(key);
                    data[col] = FormatUtils.toString(value, config.shouldDifferentiateNulls());
                    reporter.update(
                            value instanceof Node ? 1 : 0,
                            value instanceof Relationship ? 1 : 0,
                            value instanceof Entity ? 0 : 1);
                }
                out.writeNext(data, applyQuotesToAll);
                reporter.nextRow();
                return true;
            });
            reporter.done();
        } catch (AuthorizationViolationException e) {
            throw new RuntimeException(INVALID_QUERY_MODE_ERROR);
        }
    }

    public String[] writeResultHeader(Result result, CSVWriter out) {
        List<String> columns = result.columns();
        int cols = columns.size();
        String[] header = columns.toArray(new String[cols]);
        out.writeNext(header, applyQuotesToAll);
        return header;
    }

    public void writeAll(InternalTransaction threadBoundTx, SubGraph graph, Reporter reporter, CSVWriter out) {
        final var nodePropTypes = collectPropTypesForNodes(graph, db, config);
        final var relPropTypes = collectPropTypesForRelationships(graph, db, config);
        final var nodeHeader = generateHeader(nodePropTypes, config.useTypes(), NODE_HEADER_FIXED_COLUMNS);
        final var relHeader = generateHeader(relPropTypes, config.useTypes(), REL_HEADER_FIXED_COLUMNS);
        final var nodePropNames = nodePropTypes.keySet().stream().sorted().toList();
        final var relPropNames = relPropTypes.keySet().stream().sorted().toList();
        final var header = new ArrayList<>(nodeHeader);
        header.addAll(relHeader);
        out.writeNext(header.toArray(String[]::new), applyQuotesToAll);
        int cols = header.size();

        writeNodes(
                threadBoundTx,
                graph,
                out,
                reporter,
                nodePropNames,
                cols,
                config.getBatchSize(),
                config.shouldDifferentiateNulls());
        writeRels(
                threadBoundTx,
                graph,
                out,
                reporter,
                relPropNames,
                cols,
                nodeHeader.size(),
                config.getBatchSize(),
                config.shouldDifferentiateNulls());
    }

    private void writeAllBulkImport(
            InternalTransaction threadBoundTx, SubGraph graph, Reporter reporter, ExportFileManager writer) {
        Map<Iterable<Label>, List<Node>> objectNodes = StreamSupport.stream(
                        graph.getNodes().spliterator(), false)
                .collect(Collectors.groupingBy(Node::getLabels));
        Map<RelationshipType, List<Relationship>> objectRels = StreamSupport.stream(
                        graph.getRelationships().spliterator(), false)
                .collect(Collectors.groupingBy(Relationship::getType));
        writeNodesBulkImport(threadBoundTx, reporter, writer, objectNodes);
        writeRelsBulkImport(threadBoundTx, reporter, writer, objectRels);
    }

    private void writeNodesBulkImport(
            InternalTransaction threadBoundTransaction,
            Reporter reporter,
            ExportFileManager writer,
            Map<Iterable<Label>, List<Node>> objectNode) {
        objectNode.forEach((labels, nodes) -> {
            Set<String> headerNode = generateHeaderNodeBulkImport(nodes);

            List<List<String>> rows = nodes.stream()
                    .map(n -> {
                        reporter.update(1, 0, n.getAllProperties().size());
                        return headerNode.stream()
                                .map(s -> {
                                    if (s.equals(":LABEL")) {
                                        return joinLabels(labels, config.getArrayDelim());
                                    }
                                    String prop = s.split(":")[0];
                                    return prop.isEmpty()
                                            ? String.valueOf(getNodeId(threadBoundTransaction, n.getElementId()))
                                            : FormatUtils.toString(
                                                    n.getProperty(prop, null), config.shouldDifferentiateNulls());
                                })
                                .collect(Collectors.toList());
                    })
                    .collect(Collectors.toList());

            String type = joinLabels(labels, ".");
            writeRow(config, writer, headerNode, rows, "nodes." + type);
        });
    }

    private void writeRelsBulkImport(
            InternalTransaction threadBoundTx,
            Reporter reporter,
            ExportFileManager writer,
            Map<RelationshipType, List<Relationship>> objectRel) {
        objectRel.entrySet().forEach(entrySet -> {
            Set<String> headerRel = generateHeaderRelationshipBulkImport(entrySet);

            List<List<String>> rows = entrySet.getValue().stream()
                    .map(r -> {
                        reporter.update(0, 1, r.getAllProperties().size());
                        return headerRel.stream()
                                .map(s -> switch (s) {
                                    case ":START_ID" -> String.valueOf(getNodeId(
                                            threadBoundTx, r.getStartNode().getElementId()));
                                    case ":END_ID" -> String.valueOf(getNodeId(
                                            threadBoundTx, r.getEndNode().getElementId()));
                                    case ":TYPE" -> entrySet.getKey().name();
                                    default -> {
                                        String prop = s.split(":")[0];
                                        yield prop.isEmpty()
                                                ? String.valueOf(getRelationshipId(threadBoundTx, r.getElementId()))
                                                : FormatUtils.toString(r.getProperty(prop, ""));
                                    }
                                })
                                .collect(Collectors.toList());
                    })
                    .collect(Collectors.toList());
            writeRow(
                    config,
                    writer,
                    headerRel,
                    rows,
                    "relationships." + entrySet.getKey().name());
        });
    }

    private static Set<String> generateHeaderNodeBulkImport(final List<Node> nodes) {
        Set<String> headerNode = new LinkedHashSet<>();
        headerNode.add(":ID");
        Map<String, Class> keyTypes = new LinkedHashMap<>();
        nodes.forEach(node -> updateKeyTypes(keyTypes, node));
        final LinkedHashSet<String> otherFields = keyTypes.entrySet().stream()
                .map(stringClassEntry -> formatHeader(stringClassEntry))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        headerNode.addAll(otherFields);
        headerNode.add(":LABEL");
        return headerNode;
    }

    private Set<String> generateHeaderRelationshipBulkImport(Map.Entry<RelationshipType, List<Relationship>> entrySet) {
        Set<String> headerNode = new LinkedHashSet<>();
        Map<String, Class> keyTypes = new LinkedHashMap<>();
        entrySet.getValue().forEach(relationship -> updateKeyTypes(keyTypes, relationship));
        headerNode.add(":START_ID");
        headerNode.add(":END_ID");
        headerNode.add(":TYPE");
        headerNode.addAll(keyTypes.entrySet().stream()
                .map(stringClassEntry -> formatHeader(stringClassEntry))
                .collect(Collectors.toCollection(LinkedHashSet::new)));
        return headerNode;
    }

    private void writeRow(
            ExportConfig config,
            ExportFileManager writer,
            Set<String> headerNode,
            List<List<String>> rows,
            String name) {
        try (PrintWriter pw = writer.getPrintWriter(name);
                CSVWriter csvWriter = getCsvWriter(pw)) {
            if (config.isSeparateHeader()) {
                try (PrintWriter pwHeader = writer.getPrintWriter("header." + name)) {
                    CSVWriter csvWriterHeader = getCsvWriter(pwHeader);
                    csvWriterHeader.writeNext(headerNode.toArray(String[]::new), applyQuotesToAll);
                }
            } else {
                csvWriter.writeNext(headerNode.toArray(String[]::new), applyQuotesToAll);
            }
            rows.forEach(row -> csvWriter.writeNext(row.toArray(String[]::new), applyQuotesToAll));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> generateHeader(Map<String, Class> propTypes, boolean useTypes, String... starters) {
        List<String> result = new ArrayList<>();
        if (useTypes) {
            Collections.addAll(result, starters);
        } else {
            result.addAll(Stream.of(starters).map(s -> s.split(":")[0]).collect(Collectors.toList()));
        }
        result.addAll(propTypes.entrySet().stream()
                .map(entry -> {
                    String type = MetaInformation.typeFor(entry.getValue(), null);
                    return (type == null || type.equals("string") || !useTypes)
                            ? entry.getKey()
                            : entry.getKey() + ":" + type;
                })
                .sorted()
                .toList());
        return result;
    }

    private void writeNodes(
            InternalTransaction threadBoundTx,
            SubGraph graph,
            CSVWriter out,
            Reporter reporter,
            List<String> nodePropTypes,
            int cols,
            int batchSize,
            boolean keepNulls) {
        String[] row = new String[cols];
        int nodes = 0;
        for (Node node : graph.getNodes()) {
            row[0] = String.valueOf(getNodeId(threadBoundTx, node.getElementId()));
            row[1] = getLabelsString(node);
            collectProps(nodePropTypes, node, reporter, row, 2, keepNulls);
            out.writeNext(row, applyQuotesToAll);
            nodes++;
            if (batchSize == -1 || nodes % batchSize == 0) {
                reporter.update(nodes, 0, 0);
                nodes = 0;
            }
        }
        if (nodes > 0) {
            reporter.update(nodes, 0, 0);
        }
    }

    private void collectProps(
            Collection<String> fields, Entity pc, Reporter reporter, String[] row, int offset, boolean keepNulls) {
        for (String field : fields) {
            if (pc.hasProperty(field)) {
                row[offset] = FormatUtils.toString(pc.getProperty(field));
                reporter.update(0, 0, 1);
            } else if (keepNulls) {
                row[offset] = null;
            } else {
                row[offset] = "";
            }
            offset++;
        }
    }

    private void writeRels(
            InternalTransaction threadBoundTx,
            SubGraph graph,
            CSVWriter out,
            Reporter reporter,
            List<String> relPropNames,
            int cols,
            int offset,
            int batchSize,
            boolean keepNull) {
        String[] row = new String[cols];
        int rels = 0;
        for (Relationship rel : graph.getRelationships()) {
            row[offset] =
                    String.valueOf(getNodeId(threadBoundTx, rel.getStartNode().getElementId()));
            row[offset + 1] =
                    String.valueOf(getNodeId(threadBoundTx, rel.getEndNode().getElementId()));
            row[offset + 2] = rel.getType().name();
            collectProps(relPropNames, rel, reporter, row, 3 + offset, keepNull);
            out.writeNext(row, applyQuotesToAll);
            rels++;
            if (batchSize == -1 || rels % batchSize == 0) {
                reporter.update(0, rels, 0);
                rels = 0;
            }
        }
        if (rels > 0) {
            reporter.update(0, rels, 0);
        }
    }
}
