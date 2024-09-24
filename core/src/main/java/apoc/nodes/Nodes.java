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
package apoc.nodes;

import static apoc.path.RelationshipTypeAndDirections.format;
import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.refactor.util.RefactorUtil.copyProperties;
import static apoc.util.Util.map;

import apoc.Pools;
import apoc.create.Create;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.result.LongResult;
import apoc.result.NodeResult;
import apoc.result.PathResult;
import apoc.result.RelationshipResult;
import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualPathResult;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.storageengine.api.RelationshipSelection;

public class Nodes {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Context
    public Pools pools;

    @Procedure("apoc.nodes.cycles")
    @Description("Detects all `PATH` cycles in the given `LIST<NODE>`.\n"
            + "This procedure can be limited on `RELATIONSHIP` values as well.")
    public Stream<PathResult> cycles(
            @Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        NodesConfig conf = new NodesConfig(config);
        final List<String> types = conf.getRelTypes();
        Stream<Path> paths = nodes.stream().flatMap(start -> {
            boolean allRels = types.isEmpty();
            final RelationshipType[] relTypes =
                    types.stream().map(RelationshipType::withName).toArray(RelationshipType[]::new);
            final Iterable<Relationship> relationships = allRels
                    ? start.getRelationships(Direction.OUTGOING)
                    : start.getRelationships(Direction.OUTGOING, relTypes);

            PathExpanderBuilder expanderBuilder;
            if (allRels) {
                expanderBuilder = PathExpanderBuilder.allTypes(Direction.OUTGOING);
            } else {
                expanderBuilder = PathExpanderBuilder.empty();
                for (RelationshipType relType : relTypes) {
                    expanderBuilder = expanderBuilder.add(relType, Direction.OUTGOING);
                }
            }
            final PathExpander<Path> pathExpander = expanderBuilder.build();

            PathFinder<Path> finder =
                    GraphAlgoFactory.shortestPath(new BasicEvaluationContext(tx, db), pathExpander, conf.getMaxDepth());
            Map<String, List<String>> dups = new HashMap<>();
            return Iterables.stream(relationships)
                    // to prevent duplicated (start and end nodes with double-rels)
                    .filter(relationship -> {
                        final List<String> nodeDups = dups.computeIfAbsent(
                                relationship.getStartNode().getElementId(), (key) -> new ArrayList<>());
                        if (nodeDups.contains(relationship.getEndNode().getElementId())) {
                            return false;
                        }
                        nodeDups.add(relationship.getEndNode().getElementId());
                        return true;
                    })
                    .flatMap(relationship -> {
                        final Path path = finder.findSinglePath(relationship.getEndNode(), start);
                        if (path == null) return Stream.empty();
                        VirtualPath virtualPath = new VirtualPath(start);
                        virtualPath.addRel(relationship);
                        for (Relationship relPath : path.relationships()) {
                            virtualPath.addRel(relPath);
                        }
                        return Stream.of(virtualPath);
                    });
        });
        return paths.map(PathResult::new);
    }

    @Procedure(name = "apoc.nodes.link", mode = Mode.WRITE)
    @Description("Creates a linked list of the given `NODE` values connected by the given `RELATIONSHIP` type.")
    public void link(
            @Name("nodes") List<Node> nodes,
            @Name("type") String type,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Node> it = nodes.iterator();
        if (it.hasNext()) {
            RelationshipType relType = RelationshipType.withName(type);
            Node node = it.next();
            while (it.hasNext()) {
                Node next = it.next();
                final boolean createRelationship =
                        !conf.isAvoidDuplicates() || (conf.isAvoidDuplicates() && !connected(node, next, type));
                if (createRelationship) {
                    node.createRelationshipTo(next, relType);
                }
                node = next;
            }
        }
    }

    @Procedure("apoc.nodes.get")
    @Description("Returns all `NODE` values with the given ids.")
    public Stream<NodeResult> get(@Name("nodes") Object ids) {
        return Util.nodeStream((InternalTransaction) tx, ids).map(NodeResult::new);
    }

    @Procedure(name = "apoc.nodes.delete", mode = Mode.WRITE)
    @Description("Deletes all `NODE` values with the given ids.")
    public Stream<LongResult> delete(@Name("nodes") Object ids, @Name("batchSize") long batchSize) {
        Iterator<Node> it = Util.nodeStream((InternalTransaction) tx, ids).iterator();
        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int) batchSize);
            count += Util.inTx(db, pools, (txInThread) -> {
                txInThread
                        .execute("FOREACH (n in $nodes | DETACH DELETE n)", map("nodes", batch))
                        .close();
                return batch.size();
            });
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure("apoc.nodes.rels")
    @Description("Returns all `RELATIONSHIP` values with the given ids.")
    public Stream<RelationshipResult> rels(@Name("rels") Object ids) {
        return Util.relsStream((InternalTransaction) tx, ids).map(RelationshipResult::new);
    }

    @UserFunction("apoc.node.relationship.exists")
    @Description(
            "Returns a `BOOLEAN` based on whether the given `NODE` has a connecting `RELATIONSHIP` (or whether the given `NODE` has a connecting `RELATIONSHIP` of the given type and direction).")
    public boolean hasRelationship(
            @Name(value = "node", description = "The node to check for the specified relationship types.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "The relationship types to check for on the given node. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        if (types == null || types.isEmpty()) return node.hasRelationship();
        long id = ((InternalTransaction) tx).elementIdMapper().nodeId(node.getElementId());
        try (NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {

            ktx.dataRead().singleNode(id, nodeCursor);
            nodeCursor.next();
            TokenRead tokenRead = ktx.tokenRead();

            for (Pair<RelationshipType, Direction> pair : parse(types)) {
                int typeId = tokenRead.relationshipType(pair.getLeft().name());
                Direction direction = pair.getRight();

                int count =
                        switch (direction) {
                            case INCOMING -> org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming(
                                    nodeCursor, typeId);
                            case OUTGOING -> org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing(
                                    nodeCursor, typeId);
                            case BOTH -> org.neo4j.internal.kernel.api.helpers.Nodes.countAll(nodeCursor, typeId);
                        };
                if (count > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    @UserFunction("apoc.nodes.connected")
    @Description("Returns true when a given `NODE` is directly connected to another given `NODE`.\n"
            + "This function is optimized for dense nodes.")
    public boolean connected(
            @Name(
                            value = "startNode",
                            description = "The node to check if it is directly connected to the second node.")
                    Node start,
            @Name(value = "endNode", description = "The node to check if it is directly connected to the first node.")
                    Node end,
            @Name(
                            value = "types",
                            defaultValue = "",
                            description =
                                    "If not empty, provides an allow list of relationship types the nodes can be connected by. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        if (start == null || end == null) return false;
        if (start.equals(end)) return true;

        long startId = ((InternalTransaction) tx).elementIdMapper().nodeId(start.getElementId());
        long endId = ((InternalTransaction) tx).elementIdMapper().nodeId(end.getElementId());
        List<Pair<RelationshipType, Direction>> pairs = (types == null || types.isEmpty()) ? null : parse(types);

        Read dataRead = ktx.dataRead();
        TokenRead tokenRead = ktx.tokenRead();
        CursorFactory cursors = ktx.cursors();

        try (NodeCursor startNodeCursor = cursors.allocateNodeCursor(ktx.cursorContext());
                NodeCursor endNodeCursor = cursors.allocateNodeCursor(ktx.cursorContext())) {

            dataRead.singleNode(startId, startNodeCursor);
            if (!startNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + startId + " does not exist.");
            }

            dataRead.singleNode(endId, endNodeCursor);
            if (!endNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + endId + " does not exist.");
            }

            return connected(startNodeCursor, endId, typedDirections(tokenRead, pairs));
        }
    }

    @Procedure("apoc.nodes.collapse")
    @Description(
            "Merges `NODE` values together in the given `LIST<NODE>`.\n"
                    + "The `NODE` values are then combined to become one `NODE`, with all labels of the previous `NODE` values attached to it, and all `RELATIONSHIP` values pointing to it.")
    public Stream<VirtualPathResult> collapse(
            @Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        if (nodes.size() == 1) return Stream.of(new VirtualPathResult(nodes.get(0), null, null));
        Set<Node> nodeSet = new LinkedHashSet<>(nodes);
        RefactorConfig conf = new RefactorConfig(config);
        VirtualNode first = createVirtualNode(nodeSet, conf);
        if (first.getRelationships().iterator().hasNext()) {
            return StreamSupport.stream(first.getRelationships().spliterator(), false)
                    .map(relationship -> new VirtualPathResult(
                            relationship.getStartNode(), relationship, relationship.getEndNode()));
        } else {
            return Stream.of(new VirtualPathResult(first, null, null));
        }
    }

    private VirtualNode createVirtualNode(Set<Node> nodes, RefactorConfig conf) {
        Create create = new Create();
        Node first = nodes.iterator().next();
        List<String> labels = Util.labelStrings(first);
        if (conf.isCollapsedLabel()) {
            labels.add("Collapsed");
        }
        VirtualNode virtualNode = (VirtualNode) create.vNodeFunction(labels, first.getAllProperties());
        createVirtualRelationships(nodes, virtualNode, first, conf);
        nodes.stream().skip(1).forEach(node -> {
            virtualNode.addLabels(node.getLabels());
            PropertiesManager.mergeProperties(node.getAllProperties(), virtualNode, conf);
            createVirtualRelationships(nodes, virtualNode, node, conf);
        });
        if (conf.isCountMerge()) {
            virtualNode.setProperty("count", nodes.size());
        }
        return virtualNode;
    }

    private void createVirtualRelationships(
            Set<Node> nodes, VirtualNode virtualNode, Node node, RefactorConfig refactorConfig) {
        node.getRelationships().forEach(relationship -> {
            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();

            if (nodes.contains(startNode) && nodes.contains(endNode)) {
                if (refactorConfig.isSelfRel()) {
                    createOrMergeVirtualRelationship(
                            virtualNode, refactorConfig, relationship, virtualNode, Direction.OUTGOING);
                }
            } else {
                if (Objects.equals(startNode.getElementId(), node.getElementId())) {
                    createOrMergeVirtualRelationship(
                            virtualNode, refactorConfig, relationship, endNode, Direction.OUTGOING);
                } else {
                    createOrMergeVirtualRelationship(
                            virtualNode, refactorConfig, relationship, startNode, Direction.INCOMING);
                }
            }
        });
    }

    private void createOrMergeVirtualRelationship(
            VirtualNode virtualNode,
            RefactorConfig refactorConfig,
            Relationship source,
            Node node,
            Direction direction) {
        Iterable<Relationship> rels = virtualNode.getRelationships(direction, source.getType());
        Optional<Relationship> first = StreamSupport.stream(rels.spliterator(), false)
                .filter(relationship -> relationship.getOtherNode(virtualNode).equals(node))
                .findFirst();
        if (refactorConfig.isMergeVirtualRels() && first.isPresent()) {
            mergeRelationship(source, first.get(), refactorConfig);
        } else {
            if (direction == Direction.OUTGOING)
                copyProperties(source, virtualNode.createRelationshipTo(node, source.getType()));
            if (direction == Direction.INCOMING)
                copyProperties(source, virtualNode.createRelationshipFrom(node, source.getType()));
        }
    }

    private void mergeRelationship(Relationship source, Relationship target, RefactorConfig refactorConfig) {
        if (refactorConfig.isCountMerge()) {
            target.setProperty("count", (Integer) target.getProperty("count", 0) + 1);
        }
        PropertiesManager.mergeProperties(source.getAllProperties(), target, refactorConfig);
    }

    /**
     * TODO: be more efficient, in
     * @param start
     * @param end
     * @param typedDirections
     * @return
     */
    private boolean connected(NodeCursor start, long end, int[][] typedDirections) {
        try (RelationshipTraversalCursor relationship =
                ktx.cursors().allocateRelationshipTraversalCursor(ktx.cursorContext())) {
            start.relationships(relationship, RelationshipSelection.selection(Direction.BOTH));
            while (relationship.next()) {
                if (relationship.otherNodeReference() == end) {
                    if (typedDirections == null) {
                        return true;
                    } else {
                        int direction = relationship.targetNodeReference() == end ? 0 : 1;
                        int[] types = typedDirections[direction];
                        if (arrayContains(types, relationship.type())) return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean arrayContains(int[] array, int element) {
        for (int j : array) {
            if (j == element) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param ops
     * @param pairs
     * @return a int[][] where the first index is 0 for outgoing, 1 for incoming. second array contains rel type ids
     */
    private int[][] typedDirections(TokenRead ops, List<Pair<RelationshipType, Direction>> pairs) {
        if (pairs == null) return null;
        int from = 0;
        int to = 0;
        int[][] result = new int[2][pairs.size()];
        int outIdx = Direction.OUTGOING.ordinal();
        int inIdx = Direction.INCOMING.ordinal();
        for (Pair<RelationshipType, Direction> pair : pairs) {
            int type = ops.relationshipType(pair.getLeft().name());
            if (type == -1) continue;
            if (pair.getRight() != Direction.INCOMING) {
                result[outIdx][from++] = type;
            }
            if (pair.getRight() != Direction.OUTGOING) {
                result[inIdx][to++] = type;
            }
        }
        result[outIdx] = Arrays.copyOf(result[outIdx], from);
        result[inIdx] = Arrays.copyOf(result[inIdx], to);
        return result;
    }

    @UserFunction("apoc.node.labels")
    @Description("Returns the labels for the given virtual `NODE`.")
    public List<String> labels(@Name(value = "node", description = "The node to return labels from.") Node node) {
        if (node == null) return null;
        Iterator<Label> labels = node.getLabels().iterator();
        if (!labels.hasNext()) return Collections.emptyList();
        Label first = labels.next();
        if (!labels.hasNext()) return Collections.singletonList(first.name());
        List<String> result = new ArrayList<>();
        result.add(first.name());
        labels.forEachRemaining(l -> result.add(l.name()));
        return result;
    }

    @UserFunction("apoc.node.id")
    @Description("Returns the id for the given virtual `NODE`.")
    public Long id(@Name(value = "node", description = "The node to return the id from.") Node node) {
        return (node == null) ? null : node.getId();
    }

    @UserFunction("apoc.rel.id")
    @Description("Returns the id for the given virtual `RELATIONSHIP`.")
    public Long relId(@Name(value = "rel", description = "The relationship to get the id from.") Relationship rel) {
        return (rel == null) ? null : rel.getId();
    }

    @UserFunction("apoc.rel.startNode")
    @Description("Returns the start `NODE` for the given virtual `RELATIONSHIP`.")
    public Node startNode(
            @Name(value = "rel", description = "The relationship to get the start node from.") Relationship rel) {
        return (rel == null) ? null : rel.getStartNode();
    }

    @UserFunction("apoc.rel.endNode")
    @Description("Returns the end `NODE` for the given virtual `RELATIONSHIP`.")
    public Node endNode(
            @Name(value = "rel", description = "The relationship to get the end node from.") Relationship rel) {
        return (rel == null) ? null : rel.getEndNode();
    }

    @UserFunction("apoc.rel.type")
    @Description("Returns the type for the given virtual `RELATIONSHIP`.")
    public String type(@Name(value = "rel", description = "The relationship to get the type from.") Relationship rel) {
        return (rel == null) ? null : rel.getType().name();
    }

    @UserFunction("apoc.any.properties")
    @Description(
            "Returns all properties of the given object.\n"
                    + "The object can be a virtual `NODE`, a real `NODE`, a virtual `RELATIONSHIP`, a real `RELATIONSHIP`, or a `MAP`.")
    public Map<String, Object> properties(
            @Name(value = "object", description = "The object to return properties from.") Object thing,
            @Name(
                            value = "keys",
                            defaultValue = "null",
                            description =
                                    "The keys of the properties to be returned (if null then all keys are returned).")
                    List<String> keys) {
        if (thing == null) return null;
        if (thing instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) thing;
            if (keys != null) map.keySet().retainAll(keys);
            return map;
        }
        if (thing instanceof Entity) {
            if (keys == null) return ((Entity) thing).getAllProperties();
            return ((Entity) thing).getProperties(keys.toArray(new String[keys.size()]));
        }
        return null;
    }

    @UserFunction("apoc.any.property")
    @Description(
            "Returns the property for the given key from an object.\n"
                    + "The object can be a virtual `NODE`, a real `NODE`, a virtual `RELATIONSHIP`, a real `RELATIONSHIP`, or a `MAP`.")
    public Object property(
            @Name(value = "object", description = "The object to return a property from.") Object thing,
            @Name(value = "key", description = "The key of the property to return.") String key) {
        if (thing == null || key == null) return null;
        if (thing instanceof Map) {
            return ((Map<String, Object>) thing).get(key);
        }
        if (thing instanceof Entity) {
            return ((Entity) thing).getProperty(key, null);
        }
        return null;
    }

    @UserFunction("apoc.node.degree")
    @Description("Returns the total degrees of the given `NODE`.")
    public long degree(
            @Name(value = "node", description = "The node to count the total number of relationships on.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "The relationship types to restrict the count to. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        if (types == null || types.isEmpty()) return node.getDegree();
        long degree = 0;
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            degree += getDegreeSafe(node, pair.getLeft(), pair.getRight());
        }
        return degree;
    }

    @UserFunction("apoc.node.degree.in")
    @Description("Returns the total number of incoming `RELATIONSHIP` values connected to the given `NODE`.")
    public long degreeIn(
            @Name(
                            value = "node",
                            description = "The node for which to count the total number of incoming relationships.")
                    Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description = "The relationship type to restrict the count to.")
                    String type) {

        if (type == null || type.isEmpty()) {
            return node.getDegree(Direction.INCOMING);
        }

        return node.getDegree(RelationshipType.withName(type), Direction.INCOMING);
    }

    @UserFunction("apoc.node.degree.out")
    @Description("Returns the total number of outgoing `RELATIONSHIP` values from the given `NODE`.")
    public long degreeOut(
            @Name(
                            value = "node",
                            description = "The node for which to count the total number of outgoing relationships.")
                    Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description = "The relationship type to restrict the count to.")
                    String type) {

        if (type == null || type.isEmpty()) {
            return node.getDegree(Direction.OUTGOING);
        }

        return node.getDegree(RelationshipType.withName(type), Direction.OUTGOING);
    }

    @UserFunction("apoc.node.relationship.types")
    @Description("Returns a `LIST<STRING>` of distinct `RELATIONSHIP` types for the given `NODE`.")
    public List<String> relationshipTypes(
            @Name(value = "node", description = "The node to return the connected relationship types from.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "If not empty, provides an allow list of relationship types to be returned. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        if (node == null) return null;
        List<String> relTypes = Iterables.stream(node.getRelationshipTypes())
                .map(RelationshipType::name)
                .collect(Collectors.toList());
        if (types == null || types.isEmpty()) return relTypes;
        List<String> result = new ArrayList<>(relTypes.size());
        for (Pair<RelationshipType, Direction> p : parse(types)) {
            String name = p.getLeft().name();
            if (relTypes.contains(name) && node.hasRelationship(p.getRight(), p.getLeft())) {
                result.add(name);
            }
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationship.types")
    @Description("Returns a `LIST<STRING>` of distinct `RELATIONSHIP` types from the given `LIST<NODE>` values.")
    public List<Map<String, Object>> nodesRelationshipTypes(
            @Name(value = "nodes", description = "Nodes to return connected relationship types from.") Object ids,
            @Name(
                            value = "types",
                            defaultValue = "",
                            description =
                                    "If not empty, provides an allow list of relationship types to be returned. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        if (ids == null) return null;
        return Util.nodeStream((InternalTransaction) tx, ids)
                .map(node -> {
                    final List<String> relationshipTypes = relationshipTypes(node, types);
                    if (relationshipTypes == null) {
                        // in order to avoid possible NullPointerException because we'll use Collectors#toMap which uses
                        // Map#merge
                        return null;
                    }
                    return map("node", node, "types", relationshipTypes);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @UserFunction("apoc.node.relationships.exist")
    @Description(
            "Returns a `BOOLEAN` based on whether the given `NODE` has connecting `RELATIONSHIP` values (or whether the given `NODE` has connecting `RELATIONSHIP` values of the given type and direction).")
    public Map<String, Boolean> relationshipExists(
            @Name(value = "node", description = "The node to check for the specified relationship types.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "The relationship types to check for on the given node. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|....")
                    String types) {
        if (node == null || types == null || types.isEmpty()) return null;
        List<String> relTypes = Iterables.stream(node.getRelationshipTypes())
                .map(RelationshipType::name)
                .toList();
        Map<String, Boolean> result = new HashMap<>();
        for (Pair<RelationshipType, Direction> p : parse(types)) {
            String name = p.getLeft().name();
            boolean hasRelationship = relTypes.contains(name) && node.hasRelationship(p.getRight(), p.getLeft());
            result.put(format(p), hasRelationship);
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationships.exist")
    @Description(
            "Returns a `BOOLEAN` based on whether or not the given `NODE` values have the given `RELATIONSHIP` values.")
    public List<Map<String, Object>> nodesRelationshipExists(
            @Name(value = "nodes", description = "Nodes to check for the specified relationship types.") Object ids,
            @Name(
                            value = "types",
                            defaultValue = "",
                            description =
                                    "The relationship types to check for on the given nodes. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        if (ids == null) return null;
        return Util.nodeStream((InternalTransaction) tx, ids)
                .map(node -> {
                    final Map<String, Boolean> existsMap = relationshipExists(node, types);
                    if (existsMap == null) {
                        // in order to avoid possible NullPointerException because we'll use Collectors#toMap which uses
                        // Map#merge
                        return null;
                    }
                    return map("node", node, "exists", existsMap);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @UserFunction("apoc.nodes.isDense")
    @Description("Returns true if the given `NODE` is a dense node.")
    public boolean isDense(@Name(value = "node", description = "The node to check for being dense or not.") Node node) {
        try (NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {
            final long id = ((InternalTransaction) tx).elementIdMapper().nodeId(node.getElementId());
            ktx.dataRead().singleNode(id, nodeCursor);
            if (nodeCursor.next()) {
                return nodeCursor.supportsFastDegreeLookup();
            } else {
                throw new IllegalArgumentException("node with id " + id + " does not exist.");
            }
        }
    }

    @NotThreadSafe
    @UserFunction("apoc.any.isDeleted")
    @Description("Returns true if the given `NODE` or `RELATIONSHIP` no longer exists.")
    public boolean isDeleted(
            @Name(value = "object", description = "The node or relationship to check the non-existence of.")
                    Object object) {
        if (object == null) return true;
        final String query;
        if (object instanceof Node) {
            query = "MATCH (n) WHERE elementId(n) = $id RETURN COUNT(n) = 1 AS exists";
        } else if (object instanceof Relationship) {
            query = "MATCH ()-[r]->() WHERE elementId(r) = $id RETURN COUNT(r) = 1 AS exists";
        } else {
            throw new IllegalArgumentException(
                    "expected Node or Relationship but was " + object.getClass().getSimpleName());
        }
        return !(boolean) tx.execute(query, Map.of("id", ((Entity) object).getElementId()))
                .next()
                .get("exists");
    }

    // works in cases when relType is null
    private int getDegreeSafe(Node node, RelationshipType relType, Direction direction) {
        if (relType == null) {
            return node.getDegree(direction);
        }

        return node.getDegree(relType, direction);
    }
}
