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

import apoc.export.util.BatchTransaction;
import apoc.export.util.CountingReader;
import apoc.export.util.ProgressReporter;
import apoc.load.CSVResult;
import apoc.load.Mapping;
import apoc.load.util.Results;
import apoc.util.FileUtils;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.logging.Log;

public class CsvEntityLoader {

    private final CsvLoaderConfig clc;
    private final ProgressReporter reporter;
    private final Log log;
    private final URLAccessChecker urlAccessChecker;

    /**
     * @param clc configuration object
     * @param reporter
     */
    public CsvEntityLoader(CsvLoaderConfig clc, ProgressReporter reporter, Log log, URLAccessChecker urlAccessChecker) {
        this.clc = clc;
        this.reporter = reporter;
        this.log = log;
        this.urlAccessChecker = urlAccessChecker;
    }

    /**
     * Loads nodes from a CSV file with given labels to an online database, and fills the {@code idMapping},
     * which will be used by the {@link #loadRelationships(Object, String, GraphDatabaseService, Map)}
     * method.
     *
     * @param fileName URI/Binary of the CSV file representing the node
     * @param labels list of node labels to be applied to each node
     * @param db running database instance
     * @param idMapping to be filled with the mapping between the CSV ids and the DB's internal node ids
     * @throws IOException
     */
    public void loadNodes(
            final Object fileName,
            final List<String> labels,
            final GraphDatabaseService db,
            final Map<String, Map<String, String>> idMapping)
            throws IOException, URISyntaxException, URLAccessValidationError {

        try (final CountingReader reader = FileUtils.readerFor(fileName, clc.getCompressionAlgo(), urlAccessChecker)) {
            final String header = readFirstLine(reader);
            final List<CsvHeaderField> fields =
                    CsvHeaderFields.processHeader(header, clc.getDelimiter(), clc.getQuotationCharacter());

            final Optional<CsvHeaderField> idField = fields.stream()
                    .filter(f -> CsvLoaderConstants.ID_FIELD.equals(f.getType()))
                    .findFirst();

            if (!idField.isPresent()) {
                log.warn(
                        "Please note that if no ID is specified, the node will be imported but it will not be able to be connected by any relationships during the import");
            }

            final Optional<String> idAttribute =
                    idField.isPresent() ? Optional.of(idField.get().getName()) : Optional.empty();
            final String idSpace =
                    idField.isPresent() ? idField.get().getIdSpace() : CsvLoaderConstants.DEFAULT_IDSPACE;

            idMapping.putIfAbsent(idSpace, new HashMap<>());
            final Map<String, String> idspaceIdMapping = idMapping.get(idSpace);

            final Map<String, Mapping> mapping = getMapping(fields);

            final CSVReader csv = new CSVReaderBuilder(reader)
                    .withCSVParser(new RFC4180ParserBuilder()
                            .withSeparator(clc.getDelimiter())
                            .withQuoteChar(clc.getQuotationCharacter())
                            .build())
                    .withSkipLines(clc.getSkipLines() - 1)
                    .build();

            final String[] loadCsvCompatibleHeader =
                    fields.stream().map(f -> f.getName()).toArray(String[]::new);
            AtomicInteger lineNo = new AtomicInteger();
            BatchTransaction btx = new BatchTransaction(db, clc.getBatchSize(), reporter);
            try {
                csv.forEach(line -> {
                    lineNo.getAndIncrement();

                    final EnumSet<Results> results = EnumSet.of(Results.map);
                    final CSVResult result = new CSVResult(
                            loadCsvCompatibleHeader,
                            line,
                            lineNo.get(),
                            false,
                            mapping,
                            Collections.emptyList(),
                            results);

                    final String nodeCsvId =
                            (String) idAttribute.map(result.map::get).orElse(null);

                    // if 'ignore duplicate nodes' is false, there is an id field and the mapping already has the
                    // current id,
                    // we either fail the loading process or skip it depending on the 'ignore duplicate nodes' setting
                    if (idField.isPresent() && idspaceIdMapping.containsKey(nodeCsvId)) {
                        if (clc.getIgnoreDuplicateNodes()) {
                            return;
                        } else {
                            throw new IllegalStateException("Duplicate node with id " + nodeCsvId + " found on line "
                                    + lineNo + "\n" + Arrays.toString(line));
                        }
                    }

                    // create node and add its id to the mapping
                    final Node node = btx.getTransaction().createNode();
                    if (idField.isPresent()) {
                        idspaceIdMapping.put(nodeCsvId, node.getElementId());
                    }

                    // add labels
                    for (String label : labels) {
                        node.addLabel(Label.label(label));
                    }

                    // add properties
                    int props = 0;
                    for (CsvHeaderField field : fields) {
                        final String name = field.getName();
                        Object value = result.map.get(name);

                        if (field.isMeta()) {
                            final List<String> customLabels = (List<String>) value;
                            for (String customLabel : customLabels) {
                                node.addLabel(Label.label(customLabel));
                            }
                        } else if (field.isId()) {
                            final Object idValue;
                            if (clc.getStringIds()) {
                                idValue = value;
                            } else {
                                idValue = Long.valueOf((String) value);
                            }
                            node.setProperty(field.getName(), idValue);
                            props++;
                        } else {
                            boolean propertyAdded =
                                    CsvPropertyConverter.addPropertyToGraphEntity(node, field, value, clc);
                            props += propertyAdded ? 1 : 0;
                        }
                    }
                    btx.increment();
                    reporter.update(1, 0, props++);
                });
                btx.doCommit();
            } catch (RuntimeException e) {
                btx.rollback();
                throw e;
            } finally {
                btx.close();
            }
        }
    }

    /**
     * Loads relationships from a CSV file with given relationship types to an online database,
     * using the {@code idMapping} created by the
     * {@link #loadNodes(Object, List, GraphDatabaseService, Map)} method.
     *
     * @param data URI / Binary of the CSV file representing the relationship
     * @param type relationship type to be applied to each relationships
     *      and a key type, that is the relationship type to be applied to each relationships
     * @param db running database instance
     * @param idMapping stores mapping between the CSV ids and the DB's internal node ids
     * @throws IOException
     */
    public void loadRelationships(
            final Object data,
            final String type,
            final GraphDatabaseService db,
            final Map<String, Map<String, String>> idMapping)
            throws IOException, URISyntaxException, URLAccessValidationError {

        try (final CountingReader reader = FileUtils.readerFor(data, clc.getCompressionAlgo(), urlAccessChecker)) {
            final String header = readFirstLine(reader);
            final List<CsvHeaderField> fields =
                    CsvHeaderFields.processHeader(header, clc.getDelimiter(), clc.getQuotationCharacter());

            final CsvHeaderField startIdField = fields.stream()
                    .filter(f -> CsvLoaderConstants.START_ID_FIELD.equals(f.getType()))
                    .findFirst()
                    .get();

            final CsvHeaderField endIdField = fields.stream()
                    .filter(f -> CsvLoaderConstants.END_ID_FIELD.equals(f.getType()))
                    .findFirst()
                    .get();

            final List<CsvHeaderField> edgePropertiesFields = fields.stream()
                    .filter(field -> !CsvLoaderConstants.START_ID_FIELD.equals(field.getType()))
                    .filter(field -> !CsvLoaderConstants.END_ID_FIELD.equals(field.getType()))
                    .collect(Collectors.toList());

            final Map<String, Mapping> mapping = getMapping(fields);

            try (final var csv = new CSVReaderBuilder(reader)
                    .withCSVParser(new RFC4180ParserBuilder()
                            .withSeparator(clc.getDelimiter())
                            .withQuoteChar(clc.getQuotationCharacter())
                            .build())
                    .withSkipLines(clc.getSkipLines() - 1)
                    .build()) {
                final String[] loadCsvCompatibleHeader =
                        fields.stream().map(f -> f.getName()).toArray(String[]::new);

                AtomicInteger lineNo = new AtomicInteger();
                BatchTransaction btx = new BatchTransaction(db, clc.getBatchSize(), reporter);
                try {
                    csv.forEach(line -> {
                        lineNo.getAndIncrement();

                        final EnumSet<Results> results = EnumSet.of(Results.map);
                        final CSVResult result = new CSVResult(
                                loadCsvCompatibleHeader,
                                line,
                                lineNo.get(),
                                false,
                                mapping,
                                Collections.emptyList(),
                                results);

                        final Object startId = result.map.get(CsvLoaderConstants.START_ID_ATTR);
                        final Object startInternalId =
                                idMapping.get(startIdField.getIdSpace()).get(startId.toString());
                        if (startInternalId == null) {
                            throw new IllegalStateException("Node for id space " + endIdField.getIdSpace() + " and id "
                                    + startId + " not found");
                        }
                        final Node source = btx.getTransaction().getNodeByElementId(startInternalId.toString());

                        final Object endId = result.map.get(CsvLoaderConstants.END_ID_ATTR);
                        final Object endInternalId =
                                idMapping.get(endIdField.getIdSpace()).get(endId.toString());
                        if (endInternalId == null) {
                            throw new IllegalStateException(
                                    "Node for id space " + endIdField.getIdSpace() + " and id " + endId + " not found");
                        }
                        final Node target = btx.getTransaction().getNodeByElementId(endInternalId.toString());

                        final String currentType;
                        final Object overridingType = result.map.get(CsvLoaderConstants.TYPE_ATTR);
                        if (overridingType != null && !((String) overridingType).isEmpty()) {
                            currentType = (String) overridingType;
                        } else {
                            currentType = type;
                        }
                        final Relationship rel =
                                source.createRelationshipTo(target, RelationshipType.withName(currentType));

                        // add properties
                        int props = 0;
                        for (CsvHeaderField field : edgePropertiesFields) {
                            final String name = field.getName();
                            Object value = result.map.get(name);
                            boolean propertyAdded =
                                    CsvPropertyConverter.addPropertyToGraphEntity(rel, field, value, clc);
                            props += propertyAdded ? 1 : 0;
                        }
                        btx.increment();
                        reporter.update(0, 1, props);
                    });
                    btx.doCommit();
                } catch (RuntimeException e) {
                    btx.rollback();
                    throw e;
                } finally {
                    btx.close();
                }
            }
        }
    }

    private Map<String, Mapping> getMapping(List<CsvHeaderField> fields) {
        return fields.stream().collect(Collectors.toMap(CsvHeaderField::getName, f -> {
            final Map<String, Object> mappingMap = Collections.unmodifiableMap(Stream.of(
                            new AbstractMap.SimpleEntry<>("type", f.getType()),
                            new AbstractMap.SimpleEntry<>("array", f.isArray()),
                            new AbstractMap.SimpleEntry<>("optionalData", f.getOptionalData()))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

            return new Mapping(f.getName(), mappingMap, clc.getArrayDelimiter(), false);
        }));
    }

    private static String readFirstLine(CountingReader reader) throws IOException {
        String line = "";
        int i;
        while ((i = reader.read()) != 0) {
            char c = (char) i;
            if (c == '\n') break;
            line += c;
        }
        return line;
    }
}
