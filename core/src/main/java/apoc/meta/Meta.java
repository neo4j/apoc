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
package apoc.meta;

import static apoc.util.MapUtil.map;
import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_RELATIONSHIP_TYPE;

import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.GraphResult;
import apoc.result.MapResult;
import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.MapUtil;
import apoc.util.collection.Iterables;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

/**
 * The Meta class provides metadata-related operations and functions for working with Neo4j graph database.
 * It is part of the APOC (Awesome Procedures on Cypher) library.
 */
public class Meta {
    private record MetadataKey(Types type, String key) {}

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction kernelTx;

    @Context
    public Transaction transaction;

    @Context
    public Log log;

    /**
     * Represents the result of a metadata operation.
     */
    public static class MetaResult {
        public String label;
        public String property;
        public long count;
        public boolean unique;
        public boolean index;
        public boolean existence;
        public String type;
        public boolean array;
        public List<Object> sample;
        public long left; // 0,1,
        public long right; // 0,1,many
        public List<String> other = new ArrayList<>();
        public List<String> otherLabels = new ArrayList<>();
        public String elementType;
    }

    /**
     * Represents a specific metadata item, extending MetaResult.
     */
    public static class MetaItem extends MetaResult {
        public long leftCount; // 0,1,
        public long rightCount; // 0,1,many

        public MetaItem addLabel(String label) {
            this.otherLabels.add(label);
            return this;
        }

        public MetaItem(String label, String name) {
            this.label = label;
            this.property = name;
        }

        public MetaItem inc() {
            count++;
            return this;
        }

        public MetaItem rel(long out, long in) {
            this.type = Types.RELATIONSHIP.name();
            if (out > 1) array = true;
            leftCount += out;
            rightCount += in;
            left = leftCount / count;
            right = rightCount / count;
            return this;
        }

        public MetaItem other(List<String> labels) {
            for (String l : labels) {
                if (!this.other.contains(l)) this.other.add(l);
            }
            return this;
        }

        public MetaItem type(String type) {
            this.type = type;
            return this;
        }

        public MetaItem array(boolean array) {
            this.array = array;
            return this;
        }

        public MetaItem elementType(String elementType) {
            switch (elementType) {
                case "NODE":
                    this.elementType = "node";
                    break;
                case "RELATIONSHIP":
                    this.elementType = "relationship";
                    break;
            }
            return this;
        }
    }

    @UserFunction("apoc.meta.cypher.isType")
    @Description("Returns true if the given value matches the given type.")
    public boolean isTypeCypher(@Name("value") Object value, @Name("type") String type) {
        return type.equalsIgnoreCase(typeCypher(value));
    }

    @UserFunction("apoc.meta.cypher.type")
    @Description("Returns the type name of the given value.")
    public String typeCypher(@Name("value") Object value) {
        Types type = Types.of(value);

        switch (type) {
            case ANY: // TODO Check if it's necessary
                return value.getClass().getSimpleName();
            default:
                return type.toString();
        }
    }

    @UserFunction("apoc.meta.cypher.types")
    @Description("Returns a `MAP` containing the type names of the given values.")
    public Map<String, Object> typesCypher(@Name("props") Object target) {
        Map<String, Object> properties = Collections.emptyMap();
        if (target instanceof Node) properties = ((Node) target).getAllProperties();
        if (target instanceof Relationship) properties = ((Relationship) target).getAllProperties();
        if (target instanceof Map) {
            //noinspection unchecked
            properties = (Map<String, Object>) target;
        }

        Map<String, Object> result = new LinkedHashMap<>(properties.size());
        properties.forEach((key, value) -> {
            result.put(key, typeCypher(value));
        });

        return result;
    }

    /**
     * The MetaStats class represents metadata statistics collected from the transactional database.
     * It includes counts for labels, relationship types, property keys, nodes, relationships, and various maps for stats.
     */
    public static class MetaStats {
        public final long labelCount;
        public final long relTypeCount;
        public final long propertyKeyCount;
        public final long nodeCount;
        public final long relCount;
        public final Map<String, Long> labels;
        public final Map<String, Long> relTypes;
        public final Map<String, Long> relTypesCount;
        public final Map<String, Object> stats;

        /**
         * Constructs a MetaStats object with the provided metadata statistics.
         *
         * @param labelCount       The count of labels in the database.
         * @param relTypeCount     The count of relationship types in the database.
         * @param propertyKeyCount The count of property keys in the database.
         * @param nodeCount        The count of nodes in the database.
         * @param relCount         The count of relationships in the database.
         * @param labels           A map of label names and their corresponding counts.
         * @param relTypes         A map of relationship type names and their corresponding counts.
         * @param relTypesCount    A map of relationship type names and their total count.
         */
        public MetaStats(
                long labelCount,
                long relTypeCount,
                long propertyKeyCount,
                long nodeCount,
                long relCount,
                Map<String, Long> labels,
                Map<String, Long> relTypes,
                Map<String, Long> relTypesCount) {
            this.labelCount = labelCount;
            this.relTypeCount = relTypeCount;
            this.propertyKeyCount = propertyKeyCount;
            this.nodeCount = nodeCount;
            this.relCount = relCount;
            this.labels = labels;
            this.relTypes = relTypes;
            this.relTypesCount = relTypesCount;
            this.stats = map(
                    "labelCount",
                    labelCount,
                    "relTypeCount",
                    relTypeCount,
                    "propertyKeyCount",
                    propertyKeyCount,
                    "nodeCount",
                    nodeCount,
                    "relCount",
                    relCount,
                    "labels",
                    labels,
                    "relTypes",
                    relTypes);
        }
    }

    /**
     * The StatsCallback interface defines callback methods for collecting label and relationship statistics.
     */
    interface StatsCallback {
        void label(int labelId, String labelName, long count);

        void rel(int typeId, String typeName, long count);

        void rel(int typeId, String typeName, int labelId, String labelName, long out, long in);
    }

    @NotThreadSafe
    @Procedure("apoc.meta.stats")
    @Description("Returns the metadata stored in the transactional database statistics.")
    public Stream<MetaStats> stats() {
        return Stream.of(collectStats());
    }

    @NotThreadSafe
    @UserFunction(name = "apoc.meta.nodes.count")
    @Description("Returns the sum of the `NODE` values with the given labels in the `LIST<STRING>`.")
    public long count(
            @Name(value = "nodes", defaultValue = "[]") List<String> nodes,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MetaConfig conf = new MetaConfig(config);
        final DatabaseSubGraph subGraph = new DatabaseSubGraph(transaction);
        Stream<Label> labels = CollectionUtils.isEmpty(nodes)
                ? StreamSupport.stream(subGraph.getAllLabelsInUse().spliterator(), false)
                : nodes.stream().filter(Objects::nonNull).map(String::trim).map(Label::label);

        final boolean isIncludeRels = CollectionUtils.isEmpty(conf.getIncludeRels());
        Set<String> visitedNodes = new HashSet<>();
        return labels.flatMap(label -> isIncludeRels
                        ? Stream.of(subGraph.countsForNode(label))
                        : conf.getIncludeRels().stream()
                                .filter(Objects::nonNull)
                                .map(String::trim)
                                .map(rel -> {
                                    final int lastCharIdx = rel.length() - 1;
                                    final Direction direction;
                                    switch (rel.charAt(lastCharIdx)) {
                                        case '>':
                                            direction = Direction.OUTGOING;
                                            rel = rel.substring(0, lastCharIdx);
                                            break;
                                        case '<':
                                            direction = Direction.INCOMING;
                                            rel = rel.substring(0, lastCharIdx);
                                            break;
                                        default:
                                            direction = Direction.BOTH;
                                    }
                                    return Pair.of(direction, rel);
                                })
                                .flatMap(pair -> transaction
                                        .findNodes(label)
                                        .map(node -> {
                                            if (!visitedNodes.contains(node.getElementId())
                                                    && node.hasRelationship(
                                                            pair.getLeft(),
                                                            RelationshipType.withName(pair.getRight()))) {
                                                visitedNodes.add(node.getElementId());
                                                return 1L;
                                            } else {
                                                return 0L;
                                            }
                                        })
                                        .stream()))
                .reduce(0L, Math::addExact);
    }

    /**
     * Collects statistics about the graph database.
     *
     * @return The collected statistics about the graph database.
     */
    private MetaStats collectStats() {
        Map<String, Long> relStatsCount = new LinkedHashMap<>();
        TokenRead tokenRead = kernelTx.tokenRead();
        Read read = kernelTx.dataRead();

        long relTypeCount = Iterables.count(tx.getAllRelationshipTypesInUse());
        long labelCount = Iterables.count(tx.getAllLabelsInUse());

        Map<String, Long> labelStats = new LinkedHashMap<>((int) labelCount);
        Map<String, Long> relStats = new LinkedHashMap<>(2 * (int) relTypeCount);

        collectStats(new DatabaseSubGraph(transaction), null, null, new StatsCallback() {
            public void label(int labelId, String labelName, long count) {
                if (count > 0) labelStats.put(labelName, count);
            }

            public void rel(int typeId, String typeName, long count) {
                if (count > 0) {
                    relStatsCount.merge(typeName, count, Long::sum);
                    relStats.put("()-[:" + typeName + "]->()", count);
                }
            }

            public void rel(int typeId, String typeName, int labelId, String labelName, long out, long in) {
                if (out > 0) {
                    relStats.put("(:" + labelName + ")-[:" + typeName + "]->()", out);
                }
                if (in > 0) {
                    relStats.put("()-[:" + typeName + "]->(:" + labelName + ")", in);
                }
            }
        });

        return new MetaStats(
                labelCount,
                relTypeCount,
                tokenRead.propertyKeyCount(),
                read.countsForNode(ANY_LABEL),
                read.countsForRelationship(ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL),
                labelStats,
                relStats,
                relStatsCount);
    }

    /**
     * Collects statistics about a subgraph of the database.
     *
     * @param subGraph      The subgraph to collect statistics for.
     * @param labelNames    Optional collection of label names to filter the statistics. Defaults to null if not provided.
     * @param relTypeNames  Optional collection of relationship type names to filter the statistics. Defaults to null if not provided.
     * @param cb            The callback to receive the collected statistics.
     */
    private void collectStats(
            SubGraph subGraph, Collection<String> labelNames, Collection<String> relTypeNames, StatsCallback cb) {
        TokenRead tokenRead = kernelTx.tokenRead();

        Map<String, Integer> labelMap = subGraph.labelsInUse(tokenRead, labelNames);
        Map<String, Integer> typeMap = subGraph.relTypesInUse(tokenRead, relTypeNames);

        Iterable<Label> labels = CollectionUtils.isNotEmpty(labelNames)
                ? labelNames.stream().map(Label::label).collect(Collectors.toList())
                : subGraph.getAllLabelsInUse();
        Iterable<RelationshipType> types = CollectionUtils.isNotEmpty(relTypeNames)
                ? relTypeNames.stream().map(RelationshipType::withName).collect(Collectors.toList())
                : subGraph.getAllRelationshipTypesInUse();

        labels.forEach(label -> {
            long count = subGraph.countsForNode(label);
            if (count > 0) {
                String name = label.name();
                int id = labelMap.get(name);
                cb.label(id, name, count);
                types.forEach(type -> {
                    long relCountOut = subGraph.countsForRelationship(label, type);
                    long relCountIn = subGraph.countsForRelationship(type, label);
                    cb.rel(typeMap.get(type.name()), type.name(), id, name, relCountOut, relCountIn);
                });
            }
        });
        types.forEach(type -> {
            String name = type.name();
            int id = typeMap.get(name);
            cb.rel(id, name, subGraph.countsForRelationship(type));
        });
    }

    @NotThreadSafe
    @Procedure("apoc.meta.data.of")
    @Description("Examines the given sub-graph and returns a table of metadata.")
    public Stream<MetaResult> dataOf(
            @Name(value = "graph") Object graph,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MetaConfig metaConfig = new MetaConfig(config);
        final SubGraph subGraph;
        if (graph instanceof String) {
            Result result = tx.execute((String) graph);
            subGraph = CypherResultSubGraph.from(tx, result, metaConfig.isAddRelationshipsBetweenNodes());
        } else if (graph instanceof Map) {
            Map<String, Object> mGraph = (Map<String, Object>) graph;
            if (!mGraph.containsKey("nodes")) {
                throw new IllegalArgumentException(
                        "Graph Map must contains `nodes` field and `relationships` optionally");
            }
            subGraph = new NodesAndRelsSubGraph(
                    tx, (Collection<Node>) mGraph.get("nodes"), (Collection<Relationship>) mGraph.get("relationships"));
        } else if (graph instanceof VirtualGraph) {
            VirtualGraph vGraph = (VirtualGraph) graph;
            subGraph = new NodesAndRelsSubGraph(tx, vGraph.nodes(), vGraph.relationships());
        } else {
            throw new IllegalArgumentException("Supported inputs are String, VirtualGraph, Map");
        }
        return collectMetaData(subGraph, metaConfig.getSampleMetaConfig()).values().stream()
                .flatMap(x -> x.values().stream());
    }

    // todo ask index for distinct values if index size < 10 or so
    // todo put index sizes for indexed properties
    @NotThreadSafe
    @Procedure("apoc.meta.data")
    @Description("Examines the full graph and returns a table of metadata.")
    public Stream<MetaResult> data(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        SampleMetaConfig metaConfig = new SampleMetaConfig(config);
        return collectMetaData(new DatabaseSubGraph(transaction), metaConfig).values().stream()
                .flatMap(x -> x.values().stream());
    }

    @NotThreadSafe
    @Procedure("apoc.meta.schema")
    @Description("Examines the given sub-graph and returns metadata as a `MAP`.")
    public Stream<MapResult> schema(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MetaStats metaStats = collectStats();
        SampleMetaConfig metaConfig = new SampleMetaConfig(config);
        Map<MetadataKey, Map<String, MetaItem>> metaData =
                collectMetaData(new DatabaseSubGraph(transaction), metaConfig);

        Map<String, Object> relationships = collectRelationshipsMetaData(metaStats, metaData);
        Map<String, Object> nodes = collectNodesMetaData(metaStats, metaData, relationships);
        final Collection<String> commonKeys = CollectionUtils.intersection(nodes.keySet(), relationships.keySet());
        if (!commonKeys.isEmpty()) {
            relationships = relationships.entrySet().stream()
                    .map(e -> {
                        final String key = e.getKey();
                        return commonKeys.contains(key)
                                ? new AbstractMap.SimpleEntry<>(
                                        format("%s (%s)", key, Types.RELATIONSHIP.name()), e.getValue())
                                : e;
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        nodes.putAll(relationships);
        return Stream.of(new MapResult(nodes));
    }

    /**
     * This procedure is intended to replicate what's in the core Neo4j product, but with the crucial difference that it
     * supports flexible sampling options, and does not scan the entire database.  The result is producing a table of
     * metadata that is useful for generating "Tables 4 Labels" schema designs for RDBMSs, but in a more performant way.
     */
    @NotThreadSafe
    @Procedure("apoc.meta.nodeTypeProperties")
    @Description(
            "Examines the full graph and returns a table of metadata with information about the `NODE` values therein.")
    public Stream<Tables4LabelsProfile.NodeTypePropertiesEntry> nodeTypeProperties(
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MetaConfig metaConfig = new MetaConfig(config);
        try {
            return collectTables4LabelsProfile(metaConfig).asNodeStream();
        } catch (Exception e) {
            log.debug("apoc.meta.nodeTypeProperties(): Failed to return stream", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * This procedure is intended to replicate what's in the core Neo4j product, but with the crucial difference that it supports flexible sampling options, and
     * does not scan the entire database.  The result is producing a table of metadata that is useful for generating "Tables 4 Labels" schema designs for
     * RDBMSs, but in a more performant way.
     */
    @NotThreadSafe
    @Procedure("apoc.meta.relTypeProperties")
    @Description(
            "Examines the full graph and returns a table of metadata with information about the `RELATIONSHIP` values therein.")
    public Stream<Tables4LabelsProfile.RelTypePropertiesEntry> relTypeProperties(
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MetaConfig metaConfig = new MetaConfig(config);
        try {
            return collectTables4LabelsProfile(metaConfig).asRelStream();
        } catch (Exception e) {
            log.debug("apoc.meta.relTypeProperties(): Failed to return stream", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Collects metadata for generating a "Tables 4 Labels" profile based on the provided configuration. This method
     * examines the graph schema and counts the occurrences of labels and relationships. It also observes nodes and their
     * relationships, applying sampling and filtering based on the configuration. The collected metadata is stored in a
     * Tables4LabelsProfile object.
     */
    private Tables4LabelsProfile collectTables4LabelsProfile(MetaConfig config) {
        Tables4LabelsProfile profile = new Tables4LabelsProfile();

        // Collect constraints from the schema
        Schema schema = tx.schema();

        for (ConstraintDefinition cd : schema.getConstraints()) {
            if (cd.isConstraintType(ConstraintType.NODE_PROPERTY_EXISTENCE)) {
                List<String> props = new ArrayList<>(10);
                if (ConstraintTracker.nodeConstraints.containsKey(cd.getLabel().name())) {
                    props = ConstraintTracker.nodeConstraints.get(cd.getLabel().name());
                }
                cd.getPropertyKeys().forEach(props::add);
                ConstraintTracker.nodeConstraints.put(cd.getLabel().name(), props);

            } else if (cd.isConstraintType(ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE)) {
                List<String> props = new ArrayList<>(10);
                if (ConstraintTracker.relConstraints.containsKey(
                        cd.getRelationshipType().name())) {
                    props = ConstraintTracker.relConstraints.get(
                            cd.getRelationshipType().name());
                }
                cd.getPropertyKeys().forEach(props::add);
                ConstraintTracker.relConstraints.put(cd.getRelationshipType().name(), props);
            }
        }

        // Get label counts from the label count store
        Map<String, Long> countStore = getLabelCountStore(transaction, kernelTx);

        Set<String> includeLabels = config.getIncludeLabels();
        Set<String> excludeLabels = config.getExcludeLabels();

        Set<String> includeRels = config.getIncludeRels();
        Set<String> excludeRels = config.getExcludeRels();

        // Iterate over labels in the graph
        for (Label label : tx.getAllLabelsInUse()) {
            String labelName = label.name();

            if (!excludeLabels.contains(labelName) && (includeLabels.isEmpty() || includeLabels.contains(labelName))) {
                // Skip if explicitly excluded or at least 1 include specified and not included

                long labelCount = countStore.get(labelName);
                long sample = getSampleForLabelCount(labelCount, config.getSample());

                try (ResourceIterator<Node> nodes = tx.findNodes(label)) {
                    int count = 1;
                    while (nodes.hasNext()) {
                        Node node = nodes.next();
                        if (count++ % sample == 0) {
                            final Set<Boolean> skips = StreamSupport
                                    // we analyze the node for each its relationship type
                                    .stream(node.getRelationshipTypes().spliterator(), false)
                                    .map(rel -> excludeRels.contains(
                                                    rel.name()) // we skip a node when the user said that must be
                                            // excluded
                                            // or when the user provided and inclusion list, but it's not in the
                                            // provided list
                                            || (!includeRels.isEmpty() && !includeRels.contains(rel.name())))
                                    .collect(Collectors.toSet());
                            // if the Set has just one element and is true we skip the node
                            // if there are two elements [true, false] we don't skip it as give it means that
                            // it have a relationship that satisfies the condition provided
                            // by the configuration
                            if (skips.size() == 1 && skips.iterator().next()) continue;
                            profile.observe(node, config);
                        }
                    }
                }
            }
        }

        return profile.finished();
    }

    // End new code
    /**
     * Collects metadata for generating a metadata map based on the provided subgraph and configuration. This method iterates
     * over the labels and relationships in the subgraph, collects various metadata information, and stores it in the
     * metadata map.
     */
    private Map<MetadataKey, Map<String, MetaItem>> collectMetaData(SubGraph graph, SampleMetaConfig config) {
        Map<MetadataKey, Map<String, MetaItem>> metaData = new LinkedHashMap<>(100);

        Set<RelationshipType> types = Iterables.asSet(graph.getAllRelationshipTypesInUse());
        Map<String, Iterable<ConstraintDefinition>> relConstraints = new HashMap<>(20);
        Map<String, Set<String>> relIndexes = new HashMap<>();
        for (RelationshipType type : graph.getAllRelationshipTypesInUse()) {
            metaData.put(new MetadataKey(Types.RELATIONSHIP, type.name()), new LinkedHashMap<>(10));
            relConstraints.put(type.name(), graph.getConstraints(type));
            relIndexes.put(type.name(), getIndexedProperties(graph.getIndexes(type)));
        }
        for (Label label : graph.getAllLabelsInUse()) {
            Map<String, MetaItem> nodeMeta = new LinkedHashMap<>(50);
            String labelName = label.name();
            // workaround in case of duplicated keys
            metaData.put(new MetadataKey(Types.NODE, labelName), nodeMeta);
            Iterable<ConstraintDefinition> constraints = graph.getConstraints(label);
            Set<String> indexed = getIndexedProperties(graph.getIndexes(label));
            long labelCount = graph.countsForNode(label);
            long sample = getSampleForLabelCount(labelCount, config.getSample());
            Iterator<Node> nodes = graph.findNodes(label);
            int count = 1;
            while (nodes.hasNext()) {
                Node node = nodes.next();
                if (count++ % sample == 0) {
                    addRelationships(metaData, nodeMeta, labelName, node, relConstraints, types, relIndexes);
                    addProperties(nodeMeta, labelName, constraints, indexed, node, node);
                }
            }
        }
        return metaData;
    }

    private Set<String> getIndexedProperties(Iterable<IndexDefinition> indexes) {
        return Iterables.stream(indexes)
                .map(IndexDefinition::getPropertyKeys)
                .flatMap(Iterables::stream)
                .collect(Collectors.toSet());
    }

    private static Map<String, Long> getLabelCountStore(Transaction tx, KernelTransaction kernelTx) {
        List<String> labels =
                Iterables.stream(tx.getAllLabelsInUse()).map(Label::name).collect(Collectors.toList());
        TokenRead tokenRead = kernelTx.tokenRead();
        return labels.stream()
                .collect(Collectors.toMap(e -> e, e -> kernelTx.dataRead().countsForNode(tokenRead.nodeLabel(e))));
    }

    public static long getSampleForLabelCount(long labelCount, long sample) {
        if (sample != -1L) {
            long skipCount = labelCount / sample;
            long min = (long) Math.floor(skipCount - (skipCount * 0.1D));
            long max = (long) Math.ceil(skipCount + (skipCount * 0.1D));
            if (min >= max) {
                return -1L;
            }
            long randomValue = ThreadLocalRandom.current().nextLong(min, max);
            return randomValue == 0L ? -1L : randomValue; // it can't return zero as it's used in % ops
        } else {
            return sample;
        }
    }

    private Map<String, Object> collectNodesMetaData(
            MetaStats metaStats, Map<MetadataKey, Map<String, MetaItem>> metaData, Map<String, Object> relationships) {
        Map<String, Object> nodes = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> startNodeNameToRelationshipsMap = new HashMap<>();
        for (MetadataKey metadataKey : metaData.keySet()) {
            Map<String, MetaItem> entityData = metaData.get(metadataKey);
            Map<String, Object> entityProperties = new LinkedHashMap<>();
            Map<String, Object> entityRelationships = new LinkedHashMap<>();
            List<String> labels = new LinkedList<>();
            boolean isNode = metaStats.labels.keySet().stream().anyMatch((label) -> metadataKey.key.equals(label));
            for (String entityDataKey : entityData.keySet()) {
                MetaItem metaItem = entityData.get(entityDataKey);
                if (metaItem.elementType.equals("relationship")) {
                    isNode = false;
                    break;
                } else {
                    if (metaItem.unique) labels = metaItem.otherLabels;
                    if (!metaItem.type.equals("RELATIONSHIP")) { // NODE PROPERTY
                        entityProperties.put(
                                entityDataKey,
                                MapUtil.map(
                                        "type",
                                        metaItem.type,
                                        "indexed",
                                        metaItem.index,
                                        "unique",
                                        metaItem.unique,
                                        "existence",
                                        metaItem.existence));
                    } else {
                        entityRelationships.put(
                                metaItem.property,
                                MapUtil.map(
                                        "direction",
                                        "out",
                                        "count",
                                        metaItem.rightCount,
                                        "labels",
                                        metaItem.other,
                                        "properties",
                                        ((Map<String, Object>) relationships.getOrDefault(metaItem.property, Map.of()))
                                                .get("properties")));
                        metaItem.other.forEach(o -> {
                            Map<String, Object> mirroredRelationship = new LinkedHashMap<>();
                            mirroredRelationship.put(
                                    metaItem.property,
                                    MapUtil.map(
                                            "direction",
                                            "in",
                                            "count",
                                            metaItem.leftCount,
                                            "labels",
                                            new LinkedList<>(Arrays.asList(metaItem.label)),
                                            "properties",
                                            ((Map<String, Object>)
                                                            relationships.getOrDefault(metaItem.property, Map.of()))
                                                    .get("properties")));

                            if (startNodeNameToRelationshipsMap.containsKey(o))
                                startNodeNameToRelationshipsMap.get(o).add(mirroredRelationship);
                            else {
                                List<Map<String, Object>> relList = new LinkedList<>();
                                relList.add(mirroredRelationship);
                                startNodeNameToRelationshipsMap.put(o, relList);
                            }
                        });
                    }
                }
            }
            if (isNode) {
                String key = metadataKey.key;
                nodes.put(
                        key,
                        MapUtil.map(
                                "type", "node",
                                "count", metaStats.labels.get(key),
                                "labels", labels,
                                "properties", entityProperties,
                                "relationships", entityRelationships));
            }
        }
        setIncomingRelationships(nodes, startNodeNameToRelationshipsMap);
        return nodes;
    }

    private void setIncomingRelationships(
            Map<String, Object> nodes, Map<String, List<Map<String, Object>>> nodeNameToRelationshipsMap) {
        nodes.keySet().forEach(k -> {
            if (nodeNameToRelationshipsMap.containsKey(k)) {
                Map<String, Object> node = (Map<String, Object>) nodes.get(k);
                List<Map<String, Object>> relationshipsToAddList = nodeNameToRelationshipsMap.get(k);
                relationshipsToAddList.forEach(relationshipNameToRelationshipMap -> {
                    Map<String, Object> actualRelationshipsList = (Map<String, Object>) node.get("relationships");
                    relationshipNameToRelationshipMap.keySet().forEach(relationshipName -> {
                        if (actualRelationshipsList.containsKey(relationshipName)) {
                            Map<String, Object> relToAdd =
                                    (Map<String, Object>) relationshipNameToRelationshipMap.get(relationshipName);
                            Map<String, Object> existingRel =
                                    (Map<String, Object>) actualRelationshipsList.get(relationshipName);
                            List<String> labels = (List<String>) existingRel.get("labels");
                            labels.addAll((List<String>) relToAdd.get("labels"));
                        } else
                            actualRelationshipsList.put(
                                    relationshipName, relationshipNameToRelationshipMap.get(relationshipName));
                    });
                });
            }
        });
    }

    private Map<String, Object> collectRelationshipsMetaData(
            MetaStats metaStats, Map<MetadataKey, Map<String, MetaItem>> metaData) {
        Map<String, Object> relationships = new LinkedHashMap<>();
        for (MetadataKey metadataKey : metaData.keySet()) {
            Map<String, MetaItem> entityData = metaData.get(metadataKey);
            Map<String, Object> entityProperties = new LinkedHashMap<>();
            boolean isRelationship =
                    metaStats.relTypesCount.keySet().stream().anyMatch((rel) -> metadataKey.key.equals(rel));
            for (String entityDataKey : entityData.keySet()) {
                MetaItem metaItem = entityData.get(entityDataKey);
                if (!metaItem.elementType.equals("relationship")) {
                    isRelationship = false;
                    break;
                }
                if (!metaItem.type.equals("RELATIONSHIP")) { // RELATIONSHIP PROPERTY
                    entityProperties.put(
                            entityDataKey,
                            MapUtil.map(
                                    "type", metaItem.type,
                                    "array", metaItem.array,
                                    "existence", metaItem.existence,
                                    "indexed", metaItem.index));
                }
            }
            if (isRelationship) {
                String key = metadataKey.key;
                relationships.put(
                        key,
                        MapUtil.map(
                                "type",
                                "relationship",
                                "count",
                                metaStats.relTypesCount.get(key),
                                "properties",
                                entityProperties));
            }
        }
        return relationships;
    }

    private void addProperties(
            Map<String, MetaItem> properties,
            String labelName,
            Iterable<ConstraintDefinition> constraints,
            Set<String> indexed,
            Entity pc,
            Node node) {
        for (String prop : pc.getPropertyKeys()) {
            if (properties.containsKey(prop)) continue;
            MetaItem res = metaResultForProp(pc, labelName, prop);
            res.elementType(Types.of(pc).name());
            addSchemaInfo(res, prop, constraints, indexed, node);
            properties.put(prop, res);
        }
    }

    private void addRelationships(
            Map<MetadataKey, Map<String, MetaItem>> metaData,
            Map<String, MetaItem> nodeMeta,
            String labelName,
            Node node,
            Map<String, Iterable<ConstraintDefinition>> relConstraints,
            Set<RelationshipType> types,
            Map<String, Set<String>> relIndexes) {
        StreamSupport.stream(node.getRelationshipTypes().spliterator(), false)
                .filter(type -> types.contains(type))
                .forEach(type -> {
                    int out = node.getDegree(type, Direction.OUTGOING);
                    if (out == 0) return;

                    String typeName = type.name();
                    // workaround in case of duplicated keys

                    Iterable<ConstraintDefinition> constraints = relConstraints.get(typeName);
                    Set<String> indexes = relIndexes.get(typeName);
                    if (!nodeMeta.containsKey(typeName)) nodeMeta.put(typeName, new MetaItem(labelName, typeName));
                    int in = node.getDegree(type, Direction.INCOMING);

                    Map<String, MetaItem> typeMeta = metaData.get(new MetadataKey(Types.RELATIONSHIP, typeName));
                    if (!typeMeta.containsKey(labelName)) typeMeta.put(labelName, new MetaItem(typeName, labelName));
                    MetaItem relMeta = nodeMeta.get(typeName);
                    addOtherNodeInfo(node, labelName, out, in, type, relMeta, typeMeta, constraints, indexes);
                });
    }

    private void addOtherNodeInfo(
            Node node,
            String labelName,
            int out,
            int in,
            RelationshipType type,
            MetaItem relMeta,
            Map<String, MetaItem> typeMeta,
            Iterable<ConstraintDefinition> relConstraints,
            Set<String> indexes) {
        MetaItem relNodeMeta = typeMeta.get(labelName);
        relMeta.elementType(Types.of(node).name());
        relMeta.inc().rel(out, in);
        relNodeMeta.inc().rel(out, in);
        for (Relationship rel : node.getRelationships(Direction.OUTGOING, type)) {
            Node endNode = rel.getEndNode();
            List<String> labels = toStrings(endNode.getLabels());
            relMeta.other(labels);
            relNodeMeta.other(labels);
            addProperties(typeMeta, type.name(), relConstraints, indexes, rel, node);
            relNodeMeta.elementType(Types.RELATIONSHIP.name());
        }
    }

    private void addSchemaInfo(
            MetaItem res, String prop, Iterable<ConstraintDefinition> constraints, Set<String> indexed, Node node) {

        if (indexed.contains(prop)) {
            res.index = true;
        }
        if (constraints == null) return;
        for (ConstraintDefinition constraint : constraints) {
            for (String key : constraint.getPropertyKeys()) {
                if (key.equals(prop)) {
                    switch (constraint.getConstraintType()) {
                        case UNIQUENESS -> {
                            res.unique = true;
                            node.getLabels().forEach(l -> {
                                if (res.label != l.name()) res.addLabel(l.name());
                            });
                        }
                        case RELATIONSHIP_UNIQUENESS -> res.unique = true;
                        case NODE_PROPERTY_EXISTENCE, RELATIONSHIP_PROPERTY_EXISTENCE -> res.existence = true;
                    }
                }
            }
        }
    }

    private MetaItem metaResultForProp(Entity pc, String labelName, String prop) {
        MetaItem res = new MetaItem(labelName, prop);
        Object value = pc.getProperty(prop);
        res.type(Types.of(value).name());
        res.elementType(Types.of(pc).name());
        if (value.getClass().isArray()) {
            res.array = true;
        }
        return res;
    }

    private List<String> toStrings(Iterable<Label> labels) {
        List<String> res = new ArrayList<>(10);
        for (Label label : labels) {
            String name = label.name();
            res.add(name);
        }
        return res;
    }

    /**
     * Represents a pattern used for matching relationships between nodes.
     */
    static class Pattern {
        private final String from;
        private final String type;
        private final String to;

        private Pattern(String from, String type, String to) {
            this.from = from;
            this.type = type;
            this.to = to;
        }

        public static Pattern of(String labelFrom, String type, String labelTo) {
            return new Pattern(labelFrom, type, labelTo);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Pattern) {
                Pattern pattern = (Pattern) o;
                return from.equals(pattern.from) && type.equals(pattern.type) && to.equals(pattern.to);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 31 * (31 * from.hashCode() + type.hashCode()) + to.hashCode();
        }

        public Label labelTo() {
            return Label.label(to);
        }

        public Label labelFrom() {
            return Label.label(from);
        }

        public RelationshipType relationshipType() {
            return RelationshipType.withName(type);
        }
    }

    @NotThreadSafe
    @Procedure("apoc.meta.graph")
    @Description("Examines the full graph and returns a meta-graph.")
    public Stream<GraphResult> graph(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        SampleMetaConfig metaConfig = new SampleMetaConfig(config, false);
        return metaGraph(new DatabaseSubGraph(transaction), null, null, true, metaConfig);
    }

    @NotThreadSafe
    @Procedure("apoc.meta.graph.of")
    @Description("Examines the given sub-graph and returns a meta-graph.")
    public Stream<GraphResult> graphOf(
            @Name(value = "graph", defaultValue = "{}") Object graph,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        MetaConfig metaConfig = new MetaConfig(config, false);
        final SubGraph subGraph;
        if (graph instanceof String) {
            Result result = tx.execute("CYPHER runtime=pipelined " + (String) graph);
            subGraph = CypherResultSubGraph.from(tx, result, metaConfig.isAddRelationshipsBetweenNodes());
        } else if (graph instanceof Map) {
            Map<String, Object> mGraph = (Map<String, Object>) graph;
            if (!mGraph.containsKey("nodes")) {
                throw new IllegalArgumentException(
                        "Graph Map must contains `nodes` field and `relationships` optionally");
            }
            subGraph = new NodesAndRelsSubGraph(
                    tx, (Collection<Node>) mGraph.get("nodes"), (Collection<Relationship>) mGraph.get("relationships"));
        } else if (graph instanceof VirtualGraph) {
            VirtualGraph vGraph = (VirtualGraph) graph;
            subGraph = new NodesAndRelsSubGraph(tx, vGraph.nodes(), vGraph.relationships());
        } else {
            throw new IllegalArgumentException("Supported inputs are String, VirtualGraph, Map");
        }
        return metaGraph(subGraph, null, null, true, metaConfig.getSampleMetaConfig());
    }

    /**
     * Generates a meta-graph based on the provided sub-graph and configuration.
     *
     * @param subGraph       The sub-graph to generate the meta-graph from.
     * @param labelNames     (optional) A collection of label names to consider. If not specified, all labels in the sub-graph will be used.
     * @param relTypeNames   (optional) A collection of relationship type names to consider. If not specified, all relationship types in the sub-graph will be used.
     * @param removeMissing  A boolean value indicating whether non-existing relationships should be filtered out from the meta-graph.
     * @param metaConfig     Configuration options for the meta-graph generation.
     * @return A stream of GraphResult objects representing the generated meta-graph.
     */
    private Stream<GraphResult> metaGraph(
            SubGraph subGraph,
            Collection<String> labelNames,
            Collection<String> relTypeNames,
            boolean removeMissing,
            SampleMetaConfig metaConfig) {
        TokenRead tokenRead = kernelTx.tokenRead();

        Map<String, Integer> typeMap = subGraph.relTypesInUse(tokenRead, relTypeNames);
        Iterable<Label> labels = CollectionUtils.isNotEmpty(labelNames)
                ? labelNames.stream().map(Label::label).collect(Collectors.toList())
                : subGraph.getAllLabelsInUse();
        Iterable<RelationshipType> types = CollectionUtils.isNotEmpty(relTypeNames)
                ? relTypeNames.stream().map(RelationshipType::withName).collect(Collectors.toList())
                : subGraph.getAllRelationshipTypesInUse();

        Map<String, Node> vNodes = new TreeMap<>();
        Map<Pattern, Relationship> vRels = new HashMap<>(typeMap.size() * 2);

        labels.forEach(label -> {
            long count = subGraph.countsForNode(label);
            if (count > 0) {
                mergeMetaNode(label, vNodes, count);
            }
        });
        types.forEach(type -> {
            labels.forEach(start -> {
                labels.forEach(end -> {
                    String startLabel = start.name();
                    String endLabel = end.name();
                    String relType = type.name();
                    if (vRels.containsKey(Pattern.of(startLabel, relType, endLabel))) return;
                    long relCountOut = subGraph.countsForRelationship(start, type);
                    if (relCountOut == 0) return;
                    long relCountIn = subGraph.countsForRelationship(type, end);
                    if (relCountIn > 0) {
                        Node startNode = vNodes.get(startLabel);
                        Node endNode = vNodes.get(endLabel);
                        long global = subGraph.countsForRelationship(type);
                        Relationship vRel = new VirtualRelationship(startNode, endNode, type)
                                .withProperties(
                                        map("type", relType, "out", relCountOut, "in", relCountIn, "count", global));
                        vRels.put(Pattern.of(startLabel, relType, endLabel), vRel);
                    }
                });
            });
        });

        if (removeMissing) filterNonExistingRelationships(vRels, metaConfig);
        GraphResult graphResult = new GraphResult(new ArrayList<>(vNodes.values()), new ArrayList<>(vRels.values()));
        return Stream.of(graphResult);
    }

    /**
     * Filters out non-existing relationships from the provided map of patterns to relationships in the meta-graph.
     */
    private void filterNonExistingRelationships(Map<Pattern, Relationship> vRels, SampleMetaConfig metaConfig) {
        Set<Pattern> rels = vRels.keySet();
        Map<Pair<String, String>, Set<Pattern>> aggregated = new HashMap<>();
        for (Pattern rel : rels) {
            combine(aggregated, Pair.of(rel.from, rel.type), rel);
            combine(aggregated, Pair.of(rel.type, rel.to), rel);
        }
        aggregated.values().stream()
                .filter(c -> c.size() > 1)
                .flatMap(Collection::stream)
                .filter(p -> !relationshipExistsWithDegreeCheck(p, vRels.get(p), metaConfig))
                .forEach(vRels::remove);
    }

    /**
     * Checks if a relationship exists in the database with a degree check.
     *
     * @param p             The pattern representing the relationship.
     * @param relationship  The relationship to check existence for.
     * @param metaConfig    Configuration options for the meta-graph generation.
     * @return A boolean value indicating whether the relationship exists in the database.
     */
    private boolean relationshipExistsWithDegreeCheck(
            Pattern p, Relationship relationship, SampleMetaConfig metaConfig) {
        if (relationship == null) return false;
        double degreeFrom = (double) (long) relationship.getProperty("out")
                / (long) relationship.getStartNode().getProperty("count");
        double degreeTo = (double) (long) relationship.getProperty("in")
                / (long) relationship.getEndNode().getProperty("count");

        if (degreeFrom < degreeTo) {
            return relationshipExists(
                    tx, p.labelFrom(), p.labelTo(), p.relationshipType(), Direction.OUTGOING, metaConfig);
        } else {
            return relationshipExists(
                    tx, p.labelTo(), p.labelFrom(), p.relationshipType(), Direction.INCOMING, metaConfig);
        }
    }

    /**
     * relationshipExists uses sampling to check if the relationships added in previous steps exist.
     * The sample count is the skip count; e.g. if set to 1000 this means every 1000th node will be checked.
     * A high sample count means that only one node will be checked each time.
     * Note; Each node is still fetched, but the relationships on that node will not be checked
     * if skipped, which should make it faster.
     */
    static boolean relationshipExists(
            Transaction tx,
            Label labelFromLabel,
            Label labelToLabel,
            RelationshipType relationshipType,
            Direction direction,
            SampleMetaConfig metaConfig) {
        try (ResourceIterator<Node> nodes = tx.findNodes(labelFromLabel)) {
            long count = 0L;
            // A sample size below or equal to 0 means we should check every node.
            long skipCount = metaConfig.getSample() > 0 ? metaConfig.getSample() : 1;
            while (nodes.hasNext()) {
                Node node = nodes.next();
                if (count % skipCount == 0) {
                    long maxRels = metaConfig.getMaxRels();
                    for (Relationship rel : node.getRelationships(direction, relationshipType)) {
                        Node otherNode = direction == Direction.OUTGOING ? rel.getEndNode() : rel.getStartNode();
                        // We have found the rel, we are confident the relationship exists.
                        if (otherNode.hasLabel(labelToLabel)) return true;
                        if (maxRels != -1 && maxRels-- == 0) break;
                    }
                }
                count++;
            }
        }
        // Our sampling (or full scan if skipCount == 1) did not find the relationship
        // So we assume it doesn't exist and remove it from the schema, may result in false negatives!
        return false;
    }

    private void combine(Map<Pair<String, String>, Set<Pattern>> aggregated, Pair<String, String> p, Pattern rel) {
        if (!aggregated.containsKey(p)) aggregated.put(p, new HashSet<>());
        aggregated.get(p).add(rel);
    }

    @NotThreadSafe
    @Procedure("apoc.meta.graphSample")
    @Description("Examines the full graph and returns a meta-graph.\n"
            + "Unlike `apoc.meta.graph`, this procedure does not filter away non-existing paths.")
    public Stream<GraphResult> graphSample(
            @Name(value = "config", defaultValue = "{}") @Deprecated Map<String, Object> config) {
        return metaGraph(new DatabaseSubGraph(transaction), null, null, false, new SampleMetaConfig(null));
    }

    @NotThreadSafe
    @Procedure("apoc.meta.subGraph")
    @Description("Examines the given sub-graph and returns a meta-graph.")
    public Stream<GraphResult> subGraph(@Name("config") Map<String, Object> config) {
        MetaConfig metaConfig = new MetaConfig(config, false);
        return filterResultStream(
                metaConfig.getExcludeLabels(),
                metaGraph(
                        new DatabaseSubGraph(transaction),
                        metaConfig.getIncludeLabels(),
                        metaConfig.getIncludeRels(),
                        true,
                        metaConfig.getSampleMetaConfig()));
    }

    private Stream<GraphResult> filterResultStream(Set<String> excludes, Stream<GraphResult> graphResultStream) {
        if (excludes == null || excludes.isEmpty()) return graphResultStream;
        return graphResultStream.map(gr -> {
            Iterator<Node> it = gr.nodes.iterator();
            while (it.hasNext()) {
                Node node = it.next();
                if (containsLabelName(excludes, node)) it.remove();
            }

            Iterator<Relationship> it2 = gr.relationships.iterator();
            while (it2.hasNext()) {
                Relationship relationship = it2.next();
                if (excludes.contains(relationship.getType().name())
                        || containsLabelName(excludes, relationship.getStartNode())
                        || containsLabelName(excludes, relationship.getEndNode())) {
                    it2.remove();
                }
            }

            return gr;
        });
    }

    private boolean containsLabelName(Set<String> excludes, Node node) {
        for (Label label : node.getLabels()) {
            if (excludes.contains(label.name())) return true;
        }
        return false;
    }

    private Node mergeMetaNode(Label label, Map<String, Node> labels, long increment) {
        String name = label.name();
        Node vNode = labels.get(name);
        if (vNode == null) {
            vNode = new VirtualNode(new Label[] {label}, Collections.singletonMap("name", name));
            labels.put(name, vNode);
        }
        if (increment > 0)
            vNode.setProperty("count", (((Number) vNode.getProperty("count", 0L)).longValue()) + increment);
        return vNode;
    }
}
