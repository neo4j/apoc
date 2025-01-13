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
package apoc.refactor;

import static apoc.refactor.util.PropertiesManager.mergeProperties;
import static apoc.refactor.util.RefactorConfig.RelationshipSelectionStrategy.MERGE;
import static apoc.refactor.util.RefactorUtil.*;
import static apoc.util.Util.withTransactionAndRebind;
import static java.util.stream.StreamSupport.stream;

import apoc.Pools;
import apoc.algo.Cover;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class GraphRefactoring {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Pools pools;

    @Procedure(name = "apoc.refactor.extractNode", mode = Mode.WRITE)
    @Description("Expands the given `RELATIONSHIP` VALUES into intermediate `NODE` VALUES.\n"
            + "The intermediate `NODE` values are connected by the given `outType` and `inType`.")
    public Stream<NodeRefactorResult> extractNode(
            @Name(
                            value = "rels",
                            description =
                                    "The relationships to turn into new nodes. Relationships can be of type `STRING` (elementId()), `INTEGER` (id()), `RELATIONSHIP`, or `LIST<STRING | INTEGER | RELATIONSHIP>`.")
                    Object rels,
            @Name(value = "labels", description = "The labels to be added to the new nodes.") List<String> labels,
            @Name(value = "outType", description = "The type of the outgoing relationship.") String outType,
            @Name(value = "inType", description = "The type of the ingoing relationship.") String inType) {
        return Util.relsStream((InternalTransaction) tx, rels).map((rel) -> {
            NodeRefactorResult result = new NodeRefactorResult(rel.getId());
            try {
                Node copy = withTransactionAndRebind(db, tx, transaction -> {
                    Node copyNode = copyProperties(rel, transaction.createNode(Util.labels(labels)));
                    copyNode.createRelationshipTo(rel.getEndNode(), RelationshipType.withName(outType));
                    return copyNode;
                });
                rel.getStartNode().createRelationshipTo(copy, RelationshipType.withName(inType));
                rel.delete();
                return result.withOther(copy);
            } catch (Exception e) {
                return result.withError(e);
            }
        });
    }

    @Procedure(name = "apoc.refactor.collapseNode", mode = Mode.WRITE)
    @Description("Collapses the given `NODE` and replaces it with a `RELATIONSHIP` of the given type.")
    public Stream<UpdatedRelationshipResult> collapseNode(
            @Name(
                            value = "nodes",
                            description =
                                    "The nodes to collapse. Nodes can be of type `STRING` (elementId()), `INTEGER` (id()), `NODE`, or `LIST<STRING | INTEGER | NODE>`.")
                    Object nodes,
            @Name(value = "relType", description = "The name of the resulting relationship type.") String type) {
        return Util.nodeStream((InternalTransaction) tx, nodes).map((node) -> {
            UpdatedRelationshipResult result = new UpdatedRelationshipResult(node.getId());
            try {
                Iterable<Relationship> outRels = node.getRelationships(Direction.OUTGOING);
                Iterable<Relationship> inRels = node.getRelationships(Direction.INCOMING);
                if (node.getDegree(Direction.OUTGOING) == 1 && node.getDegree(Direction.INCOMING) == 1) {
                    Relationship outRel = outRels.iterator().next();
                    Relationship inRel = inRels.iterator().next();
                    Relationship newRel = inRel.getStartNode()
                            .createRelationshipTo(outRel.getEndNode(), RelationshipType.withName(type));
                    newRel = copyProperties(node, copyProperties(inRel, copyProperties(outRel, newRel)));

                    for (Relationship r : inRels) r.delete();
                    for (Relationship r : outRels) r.delete();
                    node.delete();

                    return result.withOther(newRel);
                } else {
                    return result.withError(String.format(
                            "Node %d has more that 1 outgoing %d or incoming %d relationships",
                            node.getId(), node.getDegree(Direction.OUTGOING), node.getDegree(Direction.INCOMING)));
                }
            } catch (Exception e) {
                return result.withError(e);
            }
        });
    }

    /**
     * this procedure takes a list of nodes and clones them with their labels and properties
     */
    @Procedure(name = "apoc.refactor.cloneNodes", mode = Mode.WRITE)
    @Description(
            "Clones the given `NODE` values with their labels and properties.\n"
                    + "It is possible to skip any `NODE` properties using skipProperties (note: this only skips properties on `NODE` values and not their `RELATIONSHIP` values).")
    public Stream<NodeRefactorResult> cloneNodes(
            @Name(value = "nodes", description = "The nodes to be cloned.") List<Node> nodes,
            @Name(
                            value = "withRelationships",
                            defaultValue = "false",
                            description = "Whether or not the connected relationships should also be cloned.")
                    boolean withRelationships,
            @Name(
                            value = "skipProperties",
                            defaultValue = "[]",
                            description = "Whether or not to skip the node properties when cloning.")
                    List<String> skipProperties) {
        if (nodes == null) return Stream.empty();
        return nodes.stream().map(node -> {
            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            Node newNode = tx.createNode(Util.getLabelsArray(node));
            Map<String, Object> properties = node.getAllProperties();
            if (skipProperties != null && !skipProperties.isEmpty()) {
                for (String skip : skipProperties) properties.remove(skip);
            }
            try {
                copyProperties(properties, newNode);
                if (withRelationships) {
                    copyRelationships(node, newNode, false, true);
                }
            } catch (Exception e) {
                // If there was an error, the procedure still passes, but this node + its rels should not
                // be created. Instead, an error is returned to the user in the output.
                if (withRelationships) {
                    for (Relationship rel : newNode.getRelationships()) {
                        rel.delete();
                    }
                }
                newNode.delete();
                return result.withError(e);
            }
            return result.withOther(newNode);
        });
    }

    /**
     * this procedure clones a subgraph defined by a list of nodes and relationships. The resulting clone is a disconnected subgraph,
     * with no relationships connecting with the original nodes, nor with any other node outside the subgraph clone.
     * This can be overridden by supplying a list of node pairings in the `standinNodes` config property, so any relationships that went to the old node, when cloned, will instead be redirected to the standin node.
     * This is useful when instead of cloning a certain node or set of nodes, you want to instead redirect relationships in the resulting clone
     * such that they point to some existing node in the graph.
     *
     * For example, this could be used to clone a branch from a tree structure (with none of the new relationships going
     * to the original nodes) and to redirect any relationships from an old root node (which will not be cloned) to a different existing root node, which acts as the standin.
     *
     */
    @Procedure(name = "apoc.refactor.cloneSubgraphFromPaths", mode = Mode.WRITE)
    @Description(
            "Clones a sub-graph defined by the given `LIST<PATH>` values.\n"
                    + "It is possible to skip any `NODE` properties using the `skipProperties` `LIST<STRING>` via the config `MAP`.")
    public Stream<NodeRefactorResult> cloneSubgraphFromPaths(
            @Name(value = "paths", description = "The paths to be cloned.") List<Path> paths,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        standinNodes :: LIST<LIST<NODE>>,
                        skipProperties :: LIST<STRING>
                    }
                    """)
                    Map<String, Object> config) {

        if (paths == null || paths.isEmpty()) return Stream.empty();

        Set<Node> nodes = new HashSet<>();
        Set<Relationship> rels = new HashSet<>();

        for (Path path : paths) {
            for (Relationship rel : path.relationships()) {
                rels.add(rel);
            }

            for (Node node : path.nodes()) {
                nodes.add(node);
            }
        }

        List<Node> nodesList = new ArrayList<>(nodes);
        List<Relationship> relsList = new ArrayList<>(rels);

        return cloneSubgraph(nodesList, relsList, config);
    }

    /**
     * this procedure clones a subgraph defined by a list of nodes and relationships. The resulting clone is a disconnected subgraph,
     * with no relationships connecting with the original nodes, nor with any other node outside the subgraph clone.
     * This can be overridden by supplying a list of node pairings in the `standinNodes` config property, so any relationships that went to the old node, when cloned, will instead be redirected to the standin node.
     * This is useful when instead of cloning a certain node or set of nodes, you want to instead redirect relationships in the resulting clone
     * such that they point to some existing node in the graph.
     *
     * For example, this could be used to clone a branch from a tree structure (with none of the new relationships going
     * to the original nodes) and to redirect any relationships from an old root node (which will not be cloned) to a different existing root node, which acts as the standin.
     *
     */
    @Procedure(name = "apoc.refactor.cloneSubgraph", mode = Mode.WRITE)
    @Description(
            "Clones the given `NODE` values with their labels and properties (optionally skipping any properties in the `skipProperties` `LIST<STRING>` via the config `MAP`), and clones the given `RELATIONSHIP` values.\n"
                    + "If no `RELATIONSHIP` values are provided, all existing `RELATIONSHIP` values between the given `NODE` values will be cloned.")
    public Stream<NodeRefactorResult> cloneSubgraph(
            @Name(value = "nodes", description = "The nodes to be cloned.") List<Node> nodes,
            @Name(
                            value = "rels",
                            defaultValue = "[]",
                            description =
                                    "The relationships to be cloned. If left empty all relationships between the given nodes will be cloned.")
                    List<Relationship> rels,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        standinNodes :: LIST<LIST<NODE>>,
                        skipProperties :: LIST<STRING>,
                        createNodesInNewTransactions = false :: BOOLEAN
                    }
                    """)
                    Map<String, Object> config) {

        if (nodes == null || nodes.isEmpty()) return Stream.empty();

        final var newNodeByOldNode = new HashMap<Node, Node>(nodes.size());
        final var resultStream = new ArrayList<NodeRefactorResult>();

        final var standinMap = asNodePairs(config.get("standinNodes"));
        final var skipProps = asStringSet(config.get("skipProperties"));
        final var createNodesInInnerTx =
                Boolean.TRUE.equals(config.getOrDefault("createNodesInNewTransactions", false));

        // clone nodes and populate copy map
        for (final var oldNode : nodes) {

            // standinNodes will NOT be cloned
            if (oldNode == null || standinMap.containsKey(oldNode)) continue;

            final var result = new NodeRefactorResult(oldNode.getId());
            try {
                final Node newNode;
                if (!createNodesInInnerTx) newNode = cloneNode(tx, oldNode, skipProps);
                else newNode = withTransactionAndRebind(db, tx, innerTx -> cloneNode(innerTx, oldNode, skipProps));
                resultStream.add(result.withOther(newNode));
                newNodeByOldNode.put(oldNode, newNode);
            } catch (Exception e) {
                resultStream.add(result.withError(e));
            }
        }

        final Iterator<Relationship> relsIterator;
        // empty or missing rels list means get all rels between nodes
        if (rels == null || rels.isEmpty())
            relsIterator = Cover.coverNodes(nodes).iterator();
        else relsIterator = rels.iterator();

        // clone relationships, will be between cloned nodes and/or standins
        while (relsIterator.hasNext()) {
            final var rel = relsIterator.next();
            if (rel == null) continue;

            Node oldStart = rel.getStartNode();
            Node newStart = standinMap.getOrDefault(oldStart, newNodeByOldNode.get(oldStart));

            Node oldEnd = rel.getEndNode();
            Node newEnd = standinMap.getOrDefault(oldEnd, newNodeByOldNode.get(oldEnd));

            if (newStart != null && newEnd != null) cloneRel(rel, newStart, newEnd, skipProps);
        }

        return resultStream.stream();
    }

    private static Node cloneNode(final Transaction tx, final Node node, final Set<String> skipProps) {
        final var newNode =
                tx.createNode(stream(node.getLabels().spliterator(), false).toArray(Label[]::new));
        try {
            node.getAllProperties().forEach((k, v) -> {
                if (skipProps.isEmpty() || !skipProps.contains(k)) newNode.setProperty(k, v);
            });
        } catch (Exception e) {
            newNode.delete();
            throw e;
        }
        return newNode;
    }

    private static void cloneRel(Relationship base, Node from, Node to, final Set<String> skipProps) {
        final var rel = from.createRelationshipTo(to, base.getType());
        base.getAllProperties().forEach((k, v) -> {
            if (skipProps.isEmpty() || !skipProps.contains(k)) rel.setProperty(k, v);
        });
    }

    private Map<Node, Node> asNodePairs(Object o) {
        if (o == null) return Collections.emptyMap();
        else if (o instanceof List<?> list) {
            return list.stream()
                    .filter(Objects::nonNull)
                    .map(GraphRefactoring::castNodePair)
                    .collect(Collectors.toUnmodifiableMap(l -> l.get(0), l -> l.get(1)));
        } else {
            throw new IllegalArgumentException("Expected a list of node pairs but got " + o);
        }
    }

    private static Set<String> asStringSet(Object o) {
        if (o == null) return Collections.emptySet();
        else if (o instanceof Collection<?> c && c.stream().allMatch(i -> i instanceof String)) {
            return c.stream().map(Object::toString).collect(Collectors.toSet());
        } else throw new IllegalArgumentException("Expected a list of string parameter keys but got " + o);
    }

    private static List<Node> castNodePair(Object o) {
        if (o instanceof List<?> l && l.size() == 2 && l.get(0) instanceof Node && l.get(1) instanceof Node) {
            //noinspection unchecked
            return (List<Node>) l;
        } else {
            throw new IllegalArgumentException("Expected pair of nodes but got " + o);
        }
    }

    public record MergedNodeResult(@Description("The merged node.") Node node) {}

    /**
     * Merges the nodes onto the first node.
     * The other nodes are deleted and their relationships moved onto that first node.
     */
    @Procedure(name = "apoc.refactor.mergeNodes", mode = Mode.WRITE, eager = true)
    @Description("Merges the given `LIST<NODE>` onto the first `NODE` in the `LIST<NODE>`.\n"
            + "All `RELATIONSHIP` values are merged onto that `NODE` as well.")
    public Stream<MergedNodeResult> mergeNodes(
            @Name(value = "nodes", description = "The nodes to be merged onto the first node.") List<Node> nodes,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        mergeRels :: BOOLEAN,
                        selfRef :: BOOLEAN,
                        produceSelfRef = true :: BOOLEAN,
                        preserveExistingSelfRels = true :: BOOLEAN,
                        countMerge = true :: BOOLEAN,
                        collapsedLabel :: BOOLEAN,
                        singleElementAsArray = false :: BOOLEAN,
                        avoidDuplicates = false :: BOOLEAN,
                        relationshipSelectionStrategy = "incoming" :: ["incoming", "outgoing", "merge"]
                        properties :: ["overwrite", ""discard", "combine"]
                    }
                    """)
                    Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        RefactorConfig conf = new RefactorConfig(config);
        Set<Node> nodesSet = new LinkedHashSet<>(nodes);
        // grab write locks upfront consistently ordered
        nodesSet.stream().sorted(Comparator.comparing(Node::getElementId)).forEach(tx::acquireWriteLock);

        final Node first = nodes.get(0);
        final List<String> existingSelfRelIds = conf.isPreservingExistingSelfRels()
                ? stream(first.getRelationships().spliterator(), false)
                        .filter(Util::isSelfRel)
                        .map(Entity::getElementId)
                        .collect(Collectors.toList())
                : Collections.emptyList();

        nodesSet.stream().skip(1).forEach(node -> mergeNodes(node, first, conf, existingSelfRelIds));
        return Stream.of(new MergedNodeResult(first));
    }

    public record MergedRelationshipResult(@Description("The merged relationship.") Relationship rel) {}

    /**
     * Merges the relationships onto the first relationship and delete them.
     * All relationships must have the same starting node and ending node.
     */
    @Procedure(name = "apoc.refactor.mergeRelationships", mode = Mode.WRITE)
    @Description("Merges the given `LIST<RELATIONSHIP>` onto the first `RELATIONSHIP` in the `LIST<RELATIONSHIP>`.")
    public Stream<MergedRelationshipResult> mergeRelationships(
            @Name(value = "rels", description = "The relationships to be merged onto the first relationship.")
                    List<Relationship> relationships,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        mergeRels :: BOOLEAN,
                        selfRef :: BOOLEAN,
                        produceSelfRef = true :: BOOLEAN,
                        preserveExistingSelfRels = true :: BOOLEAN,
                        countMerge = true :: BOOLEAN,
                        collapsedLabel :: BOOLEAN,
                        singleElementAsArray = false :: BOOLEAN,
                        avoidDuplicates = false :: BOOLEAN,
                        relationshipSelectionStrategy = "incoming" :: ["incoming", "outgoing", "merge"]
                        properties :: ["overwrite", "discard", "combine"]
                    }
                    """)
                    Map<String, Object> config) {
        if (relationships == null || relationships.isEmpty()) return Stream.empty();
        Set<Relationship> relationshipsSet = new LinkedHashSet<>(relationships);
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Relationship> it = relationshipsSet.iterator();
        Relationship first = it.next();
        while (it.hasNext()) {
            Relationship other = it.next();
            if (first.getStartNode().equals(other.getStartNode())
                    && first.getEndNode().equals(other.getEndNode())) mergeRels(other, first, true, conf);
            else throw new RuntimeException("All Relationships must have the same start and end nodes.");
        }
        return Stream.of(new MergedRelationshipResult(first));
    }

    /**
     * Changes the relationship-type of a relationship by creating a new one between the two nodes
     * and deleting the old.
     */
    @Procedure(name = "apoc.refactor.setType", mode = Mode.WRITE)
    @Description("Changes the type of the given `RELATIONSHIP`.")
    public Stream<UpdatedRelationshipResult> setType(
            @Name(value = "rel", description = "The relationship to change the type of.") Relationship rel,
            @Name(value = "newType", description = "The new type for the relationship.") String newType) {
        if (rel == null) return Stream.empty();
        UpdatedRelationshipResult result = new UpdatedRelationshipResult(rel.getId());
        try {
            Relationship newRel =
                    rel.getStartNode().createRelationshipTo(rel.getEndNode(), RelationshipType.withName(newType));
            copyProperties(rel, newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    /**
     * Redirects a relationships to a new target node.
     */
    @Procedure(name = "apoc.refactor.to", mode = Mode.WRITE, eager = true)
    @Description("Redirects the given `RELATIONSHIP` to the given end `NODE`.")
    public Stream<UpdatedRelationshipResult> to(
            @Name(value = "rel", description = "The relationship to redirect.") Relationship rel,
            @Name(value = "endNode", description = "The new end node the relationship should point to.") Node newNode,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                {
                    failOnErrors = false :: BOOLEAN
                }
                """)
                    Map<String, Object> config) {
        if (config == null) config = Map.of();
        if (rel == null || newNode == null) return Stream.empty();

        final var failOnErrors = Boolean.TRUE.equals(config.getOrDefault("failOnErrors", false));
        final var result = new UpdatedRelationshipResult(rel.getId());

        try {
            final var type = rel.getType();
            final var properties = rel.getAllProperties();
            final var startNode = rel.getStartNode();

            // Delete first to not break constraints.
            rel.delete();

            final var newRel = startNode.createRelationshipTo(newNode, type);
            properties.forEach(newRel::setProperty);

            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            if (failOnErrors) {
                throw e;
            } else {
                // Note! We might now have half applied the changes,
                // not sure why we would ever want to do this instead of just failing.
                // I guess it's up to the user to explicitly rollback at this point ¯\_(ツ)_/¯.
                return Stream.of(result.withError(e));
            }
        }
    }

    @Procedure(name = "apoc.refactor.invert", mode = Mode.WRITE, eager = true)
    @Description("Inverts the direction of the given `RELATIONSHIP`.")
    public Stream<RefactorRelationshipResult> invert(
            @Name(value = "rel", description = "The relationship to reverse.") Relationship rel,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        failOnErrors = false :: BOOLEAN
                    }
                    """)
                    Map<String, Object> config) {
        if (config == null) config = Map.of();
        if (rel == null) return Stream.empty();

        final var failOnErrors = Boolean.TRUE.equals(config.getOrDefault("failOnErrors", false));
        final var result = new RefactorRelationshipResult(rel.getId());

        try {
            final var type = rel.getType();
            final var properties = rel.getAllProperties();
            final var startNode = rel.getStartNode();
            final var endNode = rel.getEndNode();

            // Delete first to not break constraints.
            rel.delete();

            final var newRel = endNode.createRelationshipTo(startNode, type);
            properties.forEach(newRel::setProperty);

            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            if (failOnErrors) {
                throw e;
            } else {
                // Note! We might now have half applied the changes,
                // not sure why we would ever want to do this instead of just failing.
                // I guess it's up to the user to explicitly rollback at this point ¯\_(ツ)_/¯.
                return Stream.of(result.withError(e));
            }
        }
    }

    /**
     * Redirects a relationships to a new target node.
     */
    @Procedure(name = "apoc.refactor.from", mode = Mode.WRITE, eager = true)
    @Description("Redirects the given `RELATIONSHIP` to the given start `NODE`.")
    public Stream<RefactorRelationshipResult> from(
            @Name(value = "rel", description = "The relationship to redirect.") Relationship rel,
            @Name(value = "newNode", description = "The node to redirect the given relationship to.") Node newNode,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        failOnErrors = false :: BOOLEAN
                    }
                    """)
                    Map<String, Object> config) {
        if (config == null) config = Map.of();
        if (rel == null || newNode == null) return Stream.empty();

        final var result = new RefactorRelationshipResult(rel.getId());
        final var failOnErrors = Boolean.TRUE.equals(config.getOrDefault("failOnErrors", false));

        try {
            final var type = rel.getType();
            final var properties = rel.getAllProperties();
            final var endNode = rel.getEndNode();

            // Delete before setting properties to not break constraints.
            rel.delete();

            final var newRel = newNode.createRelationshipTo(endNode, type);
            properties.forEach(newRel::setProperty);

            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            if (failOnErrors) {
                throw e;
            } else {
                // Note! We might now have half applied the changes,
                // not sure why we would ever want to do this instead of just failing.
                // I guess it's up to the user to explicitly rollback at this point ¯\_(ツ)_/¯.
                return Stream.of(result.withError(e));
            }
        }
    }

    /**
     * Make properties boolean
     */
    @Procedure(name = "apoc.refactor.normalizeAsBoolean", mode = Mode.WRITE)
    @Description("Refactors the given property to a `BOOLEAN`.")
    public void normalizeAsBoolean(
            @Name(
                            value = "entity",
                            description = "The node or relationship whose properties will be normalized to booleans.")
                    Object entity,
            @Name(value = "propertyKey", description = "The name of the property key to normalize.") String propertyKey,
            @Name(value = "trueValues", description = "The possible representations of true values.")
                    List<Object> trueValues,
            @Name(value = "falseValues", description = "The possible representations of false values.")
                    List<Object> falseValues) {
        if (entity instanceof Entity pc) {
            Object value = pc.getProperty(propertyKey, null);
            if (value != null) {
                boolean isTrue = trueValues.contains(value);
                boolean isFalse = falseValues.contains(value);
                if (isTrue && !isFalse) {
                    pc.setProperty(propertyKey, true);
                }
                if (!isTrue && isFalse) {
                    pc.setProperty(propertyKey, false);
                }
                if (!isTrue && !isFalse) {
                    pc.removeProperty(propertyKey);
                }
            }
        }
    }

    /**
     * Create category nodes from unique property values
     */
    @Procedure(name = "apoc.refactor.categorize", mode = Mode.WRITE)
    @Description(
            "Creates new category `NODE` values from `NODE` values in the graph with the specified `sourceKey` as one of its property keys.\n"
                    + "The new category `NODE` values are then connected to the original `NODE` values with a `RELATIONSHIP` of the given type.")
    public void categorize(
            @Name(value = "sourceKey", description = "The property key to add to the on the new node.")
                    String sourceKey,
            @Name(value = "type", description = "The relationship type to connect to the new node.")
                    String relationshipType,
            @Name(value = "outgoing", description = "Whether the relationship should be outgoing or not.")
                    Boolean outgoing,
            @Name(value = "label", description = "The label of the new node.") String label,
            @Name(
                            value = "targetKey",
                            description = "The name by which the source key value will be referenced on the new node.")
                    String targetKey,
            @Name(
                            value = "copiedKeys",
                            description = "A list of additional property keys to be copied to the new node.")
                    List<String> copiedKeys,
            @Name(value = "batchSize", description = "The max size of each batch.") long batchSize)
            throws ExecutionException {
        // Verify and adjust arguments
        if (sourceKey == null) throw new IllegalArgumentException("Invalid (null) sourceKey");

        if (targetKey == null) throw new IllegalArgumentException("Invalid (null) targetKey");

        copiedKeys.remove(targetKey); // Just to be sure

        if (!isUniqueConstraintDefinedFor(label, targetKey)) {
            throw new IllegalArgumentException(
                    "Before execute this procedure you must define an unique constraint for the label and the targetKey:\n"
                            + String.format(
                                    "CREATE CONSTRAINT FOR (n:`%s`) REQUIRE n.`%s` IS UNIQUE", label, targetKey));
        }

        // Create batches of nodes
        List<Node> batch = null;
        List<Future<Void>> futures = new ArrayList<>();
        for (Node node : tx.getAllNodes()) {
            if (batch == null) {
                batch = new ArrayList<>((int) batchSize);
            }
            batch.add(node);
            if (batch.size() == batchSize) {
                futures.add(
                        categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys));
                batch = null;
            }
        }
        if (batch != null) {
            futures.add(categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys));
        }

        // Await processing of node batches
        for (Future<Void> future : futures) {
            Pools.force(future);
        }
    }

    public record RefactorGraphResult(
            @Description("The remaining nodes.") List<Node> nodes,
            @Description("The new connecting relationships.") List<Relationship> relationships) {}

    @Procedure(
            name = "apoc.refactor.deleteAndReconnect",
            mode = Mode.WRITE,
            deprecatedBy =
                    "Deprecated for removal without a direct replacement, use plain Cypher or create a custom procedure.")
    @Description(
            """
            Removes the given `NODE` values from the `PATH` (and graph, including all of its relationships) and reconnects the remaining `NODE` values.
            Note, undefined behaviour for paths that visits the same node multiple times.
            Note, nodes that are not connected in the same direction as the path will not be reconnected, for example `MATCH p=(:A)-->(b:B)<--(:C) CALL apoc.refactor.deleteAndReconnect(p, [b]) ...` will not reconnect the :A and :C nodes.""")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public Stream<RefactorGraphResult> deleteAndReconnectCypher25(
            @Name(
                            value = "path",
                            description =
                                    "The path containing the nodes to delete and the remaining nodes to reconnect.")
                    Path path,
            @Name(value = "nodes", description = "The nodes to delete.") List<Node> nodesToRemove,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
            {
                relationshipSelectionStrategy = "incoming" :: ["incoming", "outgoing", "merge"]
                properties :: ["overwrite", "discard", "combine"]
            }
            """)
                    Map<String, Object> config) {
        return deleteAndReconnectCypher5(path, nodesToRemove, config);
    }

    @Procedure(name = "apoc.refactor.deleteAndReconnect", mode = Mode.WRITE)
    @Description(
            """
            Removes the given `NODE` values from the `PATH` (and graph, including all of its relationships) and reconnects the remaining `NODE` values.
            Note, undefined behaviour for paths that visits the same node multiple times.
            Note, nodes that are not connected in the same direction as the path will not be reconnected, for example `MATCH p=(:A)-->(b:B)<--(:C) CALL apoc.refactor.deleteAndReconnect(p, [b]) ...` will not reconnect the :A and :C nodes.""")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    public Stream<RefactorGraphResult> deleteAndReconnectCypher5(
            @Name(
                            value = "path",
                            description =
                                    "The path containing the nodes to delete and the remaining nodes to reconnect.")
                    Path path,
            @Name(value = "nodes", description = "The nodes to delete.") List<Node> nodesToRemove,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        relationshipSelectionStrategy = "incoming" :: ["incoming", "outgoing", "merge"]
                        properties :: ["overwrite", "discard", "combine"]
                    }
                    """)
                    Map<String, Object> config) {

        RefactorConfig refactorConfig = new RefactorConfig(config);
        List<Node> nodes = new ArrayList<>();
        path.nodes().forEach(nodes::add);
        Set<Relationship> rels = Iterables.asSet(path.relationships());

        if (!nodes.containsAll(nodesToRemove)) {
            return Stream.empty();
        }

        BiFunction<Node, Direction, Relationship> filterRel =
                (node, direction) -> stream(node.getRelationships(direction).spliterator(), false)
                        .filter(rels::contains)
                        .findFirst()
                        .orElse(null);

        nodesToRemove.forEach(node -> {
            Relationship relationshipIn = filterRel.apply(node, Direction.INCOMING);
            Relationship relationshipOut = filterRel.apply(node, Direction.OUTGOING);

            // if initial or terminal node
            if (relationshipIn == null || relationshipOut == null) {
                rels.remove(relationshipIn == null ? relationshipOut : relationshipIn);

            } else {
                Node nodeIncoming = relationshipIn.getStartNode();
                Node nodeOutgoing = relationshipOut.getEndNode();

                RelationshipType newRelType;
                Map<String, Object> newRelProps = new HashMap<>();

                final RefactorConfig.RelationshipSelectionStrategy strategy =
                        refactorConfig.getRelationshipSelectionStrategy();
                switch (strategy) {
                    case INCOMING -> {
                        newRelType = relationshipIn.getType();
                        newRelProps.putAll(relationshipIn.getAllProperties());
                    }
                    case OUTGOING -> {
                        newRelType = relationshipOut.getType();
                        newRelProps.putAll(relationshipOut.getAllProperties());
                    }
                    default -> {
                        newRelType =
                                RelationshipType.withName(relationshipIn.getType() + "_" + relationshipOut.getType());
                        newRelProps.putAll(relationshipIn.getAllProperties());
                    }
                }

                Relationship relCreated = nodeIncoming.createRelationshipTo(nodeOutgoing, newRelType);
                newRelProps.forEach(relCreated::setProperty);

                if (strategy == MERGE) {
                    mergeProperties(relationshipOut.getAllProperties(), relCreated, refactorConfig);
                }

                rels.add(relCreated);
                rels.removeAll(List.of(relationshipIn, relationshipOut));
            }

            tx.execute("WITH $node as n DETACH DELETE n", Map.of("node", node));
            nodes.remove(node);
        });

        return Stream.of(new RefactorGraphResult(nodes, List.copyOf(rels)));
    }

    private boolean isUniqueConstraintDefinedFor(String label, String key) {
        return stream(tx.schema().getConstraints(Label.label(label)).spliterator(), false)
                .anyMatch(c -> {
                    if (!c.isConstraintType(ConstraintType.UNIQUENESS)) {
                        return false;
                    }
                    return stream(c.getPropertyKeys().spliterator(), false).allMatch(k -> k.equals(key));
                });
    }

    private Future<Void> categorizeNodes(
            List<Node> batch,
            String sourceKey,
            String relationshipType,
            Boolean outgoing,
            String label,
            String targetKey,
            List<String> copiedKeys) {

        return pools.processBatch(batch, db, (innerTx, node) -> {
            node = Util.rebind(innerTx, node);
            Object value = node.getProperty(sourceKey, null);
            if (value != null) {
                String nodeLabel = Util.sanitize(label);
                String key = Util.sanitize(targetKey);
                String relType = Util.sanitize(relationshipType);
                String q = "WITH $node AS n " + "MERGE (cat:`"
                        + nodeLabel + "` {`" + key + "`: $value}) "
                        + (outgoing
                                ? "MERGE (n)-[:`" + relType + "`]->(cat) "
                                : "MERGE (n)<-[:`" + relType + "`]-(cat) ")
                        + "RETURN cat";
                Map<String, Object> params = new HashMap<>(2);
                params.put("node", node);
                params.put("value", value);
                Result result = innerTx.execute(q, params);
                if (result.hasNext()) {
                    Node cat = (Node) result.next().get("cat");
                    for (String copiedKey : copiedKeys) {
                        Object copiedValue = node.getProperty(copiedKey, null);
                        if (copiedValue != null) {
                            Object catValue = cat.getProperty(copiedKey, null);
                            if (catValue == null) {
                                cat.setProperty(copiedKey, copiedValue);
                                node.removeProperty(copiedKey);
                            } else if (copiedValue.equals(catValue)) {
                                node.removeProperty(copiedKey);
                            }
                        }
                    }
                }
                assert (!result.hasNext());
                result.close();
                node.removeProperty(sourceKey);
            }
        });
    }

    private void mergeNodes(Node source, Node target, RefactorConfig conf, List<String> excludeRelIds) {
        try {
            Map<String, Object> properties = source.getAllProperties();
            final Iterable<Label> labels = source.getLabels();

            copyRelationships(source, target, true, conf.isCreatingNewSelfRel());
            if (conf.getMergeRelsAllowed()) {
                mergeRelationshipsWithSameTypeAndDirection(target, conf, Direction.OUTGOING, excludeRelIds);
                mergeRelationshipsWithSameTypeAndDirection(target, conf, Direction.INCOMING, excludeRelIds);
            }
            source.delete();
            labels.forEach(target::addLabel);
            PropertiesManager.mergeProperties(properties, target, conf);
        } catch (NotFoundException e) {
            log.warn("skipping a node for merging: " + e.getCause().getMessage());
        }
    }

    private void copyRelationships(Node source, Node target, boolean delete, boolean createNewSelfRel) {
        for (Relationship rel : source.getRelationships()) {
            var startNode = rel.getStartNode();
            var endNode = rel.getEndNode();

            if (!createNewSelfRel && startNode.getElementId().equals(endNode.getElementId()))  {
                if (delete) rel.delete();
            } else {
                if (startNode.getElementId().equals(source.getElementId())) startNode = target;
                if (endNode.getElementId().equals(source.getElementId())) endNode = target;

                final var type = rel.getType();
                final var properties = rel.getAllProperties();

                // Delete first to avoid breaking constraints.
                if (delete) rel.delete();

                final var newRel = startNode.createRelationshipTo(endNode, type);
                properties.forEach(newRel::setProperty);
            }
        }
    }
}
