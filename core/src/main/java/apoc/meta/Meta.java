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

import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.VirtualGraph;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.cypher.export.CypherResultSubGraph;
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
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
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
    public Transaction transaction;

    @Context
    public Log log;

    @Context
    public ProcedureCallContext procedureCallContext;

    /**
     * Represents the result of a metadata operation.
     */
    public static class MetaResult {
        @Description("The label or type name.")
        public String label;

        @Description("The property name.")
        public String property;

        @Description("The count of seen values.")
        public long count;

        @Description("If all seen values are unique.")
        public boolean unique;

        @Description("If an index exists for this property.")
        public boolean index;

        @Description("If an existence constraint exists for this property.")
        public boolean existence;

        @Description("The type represented by this row.")
        public String type;

        @Description(
                "Indicates whether the property is an array. If the type column is \"RELATIONSHIP,\" this will be true if there is at least one node with two outgoing relationships of the type specified by the label or property column.")
        public boolean array;

        @Description("This is always null.")
        public List<Object> sample;

        @Description(
                "The ratio (rounded down) of the count of outgoing relationships for a specific label and relationship type relative to the total count of those patterns.")
        public long left; // 0,1,

        @Description(
                "The ratio (rounded down) of the count of incoming relationships for a specific label and relationship type relative to the total count of those patterns.")
        public long right; // 0,1,many

        @Description("The labels of connect nodes.")
        public List<String> other = new ArrayList<>();

        @Description(
                "For uniqueness constraints, this field shows other labels present on nodes that also contain the uniqueness constraint.")
        public List<String> otherLabels = new ArrayList<>();

        @Description("Whether this refers to a node or a relationship.")
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
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns true if the given value matches the given type.")
    public boolean isTypeCypherCypher5(
            @Name(value = "value", description = "An object to check the type of.") Object value,
            @Name(value = "type", description = "The verification type.") String type) {
        return type.equalsIgnoreCase(typeCypher(value));
    }

    @Deprecated
    @UserFunction(
            name = "apoc.meta.cypher.isType",
            deprecatedBy = "Cypher's type predicate expressions: `value IS :: <TYPE>`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns true if the given value matches the given type.")
    public boolean isTypeCypher(
            @Name(value = "value", description = "An object to check the type of.") Object value,
            @Name(value = "type", description = "The verification type.") String type) {
        return type.equalsIgnoreCase(typeCypher(value));
    }

    @UserFunction("apoc.meta.cypher.type")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the type name of the given value.")
    public String typeCypherCypher5(
            @Name(value = "value", description = "An object to get the type of.") Object value) {
        return typeCypher(value);
    }

    @Deprecated
    @UserFunction(name = "apoc.meta.cypher.type", deprecatedBy = "Cypher's `valueType()` function.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the type name of the given value.")
    public String typeCypher(@Name(value = "value", description = "An object to get the type of.") Object value) {
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
    public Map<String, Object> typesCypher(
            @Name(value = "props", description = "A relationship, node or map to get the property types from.")
                    Object target) {
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

    @NotThreadSafe
    @Procedure("apoc.meta.data.of")
    @Description("Examines the given sub-graph and returns a table of metadata.")
    public Stream<MetaResult> dataOf(
            @Name(value = "graph", description = "The graph to extract metadata from.") Object graph,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    "Number of nodes to sample, setting sample to `-1` will remove sampling; { sample = 1000 :: INTEGER }")
                    Map<String, Object> config) {
        MetaConfig metaConfig = new MetaConfig(config);
        final SubGraph subGraph;
        if (graph instanceof String) {
            Result result = tx.execute(Util.prefixQueryWithCheck(procedureCallContext, (String) graph));
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
}
