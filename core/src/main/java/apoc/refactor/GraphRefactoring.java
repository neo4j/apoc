package apoc.refactor;

import apoc.Pools;
import apoc.algo.Cover;
import apoc.refactor.util.PropertiesManager;
import apoc.refactor.util.RefactorConfig;
import apoc.result.GraphResult;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.commons.collections4.IterableUtils;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.refactor.util.PropertiesManager.mergeProperties;
import static apoc.refactor.util.RefactorConfig.RelationshipSelectionStrategy.MERGE;
import static apoc.refactor.util.RefactorUtil.*;
import static apoc.util.Util.withTransactionAndRebind;

public class GraphRefactoring {
    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Pools pools;

    private Stream<NodeRefactorResult> doCloneNodes(@Name("nodes") List<Node> nodes, @Name("withRelationships") boolean withRelationships, List<String> skipProperties) {
        if (nodes == null) return Stream.empty();
        return nodes.stream().map(node -> Util.rebind(tx, node)).map(node -> {
            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            try {
                Node copy = withTransactionAndRebind(db, tx, transaction -> {
                    Node newNode = copyLabels(node, transaction.createNode());

                    Map<String, Object> properties = node.getAllProperties();
                    if (skipProperties != null && !skipProperties.isEmpty())
                        for (String skip : skipProperties) properties.remove(skip);

                    newNode = copyProperties(properties, newNode);
                    copyLabels(node, newNode);
                    return newNode;
                });
                if (withRelationships) {
                    copyRelationships(node, copy, false, true);
                }
                return result.withOther(copy);
            } catch (Exception e) {
                return result.withError(e);
            }
        });
    }

    @Procedure(name = "apoc.refactor.extractNode", mode = Mode.WRITE)
    @Description("Expands the given relationships into intermediate nodes.\n" +
            "The intermediate nodes are connected by the given 'OUT' and 'IN' types.")
    public Stream<NodeRefactorResult> extractNode(@Name("rels") Object rels, @Name("labels") List<String> labels, @Name("outType") String outType, @Name("inType") String inType) {
        return Util.relsStream(tx, rels).map((rel) -> {
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
    @Description("Collapses the given node and replaces it with a relationship of the given type.")
    public Stream<RelationshipRefactorResult> collapseNode(@Name("nodes") Object nodes, @Name("relType") String type) {
        return Util.nodeStream(tx, nodes).map((node) -> {
            RelationshipRefactorResult result = new RelationshipRefactorResult(node.getId());
            try {
                Iterable<Relationship> outRels = node.getRelationships(Direction.OUTGOING);
                Iterable<Relationship> inRels = node.getRelationships(Direction.INCOMING);
                if (node.getDegree(Direction.OUTGOING) == 1 && node.getDegree(Direction.INCOMING) == 1) {
                    Relationship outRel = outRels.iterator().next();
                    Relationship inRel = inRels.iterator().next();
                    Relationship newRel = inRel.getStartNode().createRelationshipTo(outRel.getEndNode(), RelationshipType.withName(type));
                    newRel = copyProperties(node, copyProperties(inRel, copyProperties(outRel, newRel)));

                    for (Relationship r : inRels) r.delete();
                    for (Relationship r : outRels) r.delete();
                    node.delete();

                    return result.withOther(newRel);
                } else {
                    return result.withError(String.format("Node %d has more that 1 outgoing %d or incoming %d relationships", node.getId(), node.getDegree(Direction.OUTGOING), node.getDegree(Direction.INCOMING)));
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
    @Description("Clones the given nodes with their labels and properties.\n" +
            "It is possible to skip any node properties using skipProperties (note: this only skips properties on nodes and not their relationships).")
    public Stream<NodeRefactorResult> cloneNodes(@Name("nodes") List<Node> nodes,
                                                 @Name(value = "withRelationships", defaultValue = "false") boolean withRelationships,
                                                 @Name(value = "skipProperties", defaultValue = "[]") List<String> skipProperties) {
        return doCloneNodes(nodes, withRelationships, skipProperties);
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
    @Description("Clones a sub-graph defined by the given list of paths.\n" +
            "It is possible to skip any node properties using the skipProperties list via the config map.")
    public Stream<NodeRefactorResult> cloneSubgraphFromPaths(@Name("paths") List<Path> paths,
                                                             @Name(value="config", defaultValue = "{}") Map<String, Object> config) {

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
    @Description("Clones the given nodes with their labels and properties (optionally skipping any properties in the skipProperties list via the config map), and clones the given relationships.\n" +
            "If no relationships are provided, all existing relationships between the given nodes will be cloned.")
    public Stream<NodeRefactorResult> cloneSubgraph(@Name("nodes") List<Node> nodes,
                                                    @Name(value="rels", defaultValue = "[]") List<Relationship> rels,
                                                    @Name(value="config", defaultValue = "{}") Map<String, Object> config) {

        if (nodes == null || nodes.isEmpty()) return Stream.empty();

        // empty or missing rels list means get all rels between nodes
        if (rels == null || rels.isEmpty()) {
            rels = Cover.coverNodes(nodes).collect(Collectors.toList());
        }

        Map<Node, Node> copyMap = new HashMap<>(nodes.size());
        List<NodeRefactorResult> resultStream = new ArrayList<>();

        Map<Node, Node> standinMap = generateStandinMap((List<List<Node>>) config.getOrDefault("standinNodes", Collections.emptyList()));
        List<String> skipProperties = (List<String>) config.getOrDefault("skipProperties", Collections.emptyList());

        // clone nodes and populate copy map
        for (Node node : nodes) {
            if (node == null || standinMap.containsKey(node)) continue;
            // standinNodes will NOT be cloned

            NodeRefactorResult result = new NodeRefactorResult(node.getId());
            try {
                Node copy = withTransactionAndRebind(db, tx, transaction -> {
                    Node copyTemp = transaction.createNode();
                    Map<String, Object> properties = node.getAllProperties();
                    if (skipProperties != null && !skipProperties.isEmpty()) {
                        for (String skip : skipProperties) properties.remove(skip);
                    }
                    copyProperties(properties, copyTemp);
                    copyLabels(node, copyTemp);
                    return copyTemp;
                });
                resultStream.add(result.withOther(copy));
                copyMap.put(node, copy);
            } catch (Exception e) {
                resultStream.add(result.withError(e));
            }
        }

        // clone relationships, will be between cloned nodes and/or standins
        for (Relationship rel : rels) {
            if (rel == null) continue;

            Node oldStart = rel.getStartNode();
            Node newStart = standinMap.getOrDefault(oldStart, copyMap.get(oldStart));

            Node oldEnd = rel.getEndNode();
            Node newEnd = standinMap.getOrDefault(oldEnd, copyMap.get(oldEnd));

            if (newStart != null && newEnd != null) {
                Relationship newrel = newStart.createRelationshipTo(newEnd, rel.getType());
                Map<String, Object> properties = rel.getAllProperties();
                if (skipProperties != null && !skipProperties.isEmpty()) {
                    for (String skip : skipProperties) properties.remove(skip);
                }
                copyProperties(properties, newrel);            }
        }

        return resultStream.stream();
    }

    private Map<Node, Node> generateStandinMap(List<List<Node>> standins) {
        Map<Node, Node> standinMap = standins.isEmpty() ? Collections.emptyMap() : new HashMap<>(standins.size());

        for (List<Node> pairing : standins) {
            if (pairing == null) continue;

            if (pairing.size() != 2) {
                throw new IllegalArgumentException("\'standinNodes\' must be a list of node pairs");
            }

            Node from = pairing.get(0);
            Node to = pairing.get(1);

            if (from == null || to == null) {
                throw new IllegalArgumentException("\'standinNodes\' must be a list of node pairs");
            }

            standinMap.put(from, to);
        }

        return standinMap;
    }

    /**
     * Merges the nodes onto the first node.
     * The other nodes are deleted and their relationships moved onto that first node.
     */
    @Procedure(name = "apoc.refactor.mergeNodes", mode = Mode.WRITE,eager = true)
    @Description("Merges the given list of nodes onto the first node in the list.\n" +
            "All relationships are merged onto that node as well.")
    public Stream<NodeResult> mergeNodes(@Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        RefactorConfig conf = new RefactorConfig(config);
        Set<Node> nodesSet = new LinkedHashSet<>(nodes);
        // grab write locks upfront consistently ordered
        nodesSet.stream().sorted(Comparator.comparingLong(Node::getId)).forEach(tx::acquireWriteLock);

        final Node first = nodes.get(0);
        final List<String> existingSelfRelIds = conf.isPreservingExistingSelfRels()
                ? StreamSupport.stream(first.getRelationships().spliterator(), false).filter(Util::isSelfRel)
                    .map(Entity::getElementId)
                    .collect(Collectors.toList())
                : Collections.emptyList();

        nodesSet.stream().skip(1).forEach(node -> mergeNodes(node, first, conf, existingSelfRelIds));
        return Stream.of(new NodeResult(first));
    }

    /**
     * Merges the relationships onto the first relationship and delete them.
     * All relationships must have the same starting node and ending node.
     */
    @Procedure(name = "apoc.refactor.mergeRelationships", mode = Mode.WRITE)
    @Description("Merges the given list of relationships onto the first relationship in the list.")
    public Stream<RelationshipResult> mergeRelationships(@Name("rels") List<Relationship> relationships, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (relationships == null || relationships.isEmpty()) return Stream.empty();
        Set<Relationship> relationshipsSet = new LinkedHashSet<>(relationships);
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Relationship> it = relationshipsSet.iterator();
        Relationship first = it.next();
        while (it.hasNext()) {
            Relationship other = it.next();
            if (first.getStartNode().equals(other.getStartNode()) && first.getEndNode().equals(other.getEndNode()))
                mergeRels(other, first, true, conf);
            else
                throw new RuntimeException("All Relationships must have the same start and end nodes.");
        }
        return Stream.of(new RelationshipResult(first));
    }

    /**
     * Changes the relationship-type of a relationship by creating a new one between the two nodes
     * and deleting the old.
     */
    @Procedure(name = "apoc.refactor.setType", mode = Mode.WRITE)
    @Description("Changes the type of the given relationship.")
    public Stream<RelationshipRefactorResult> setType(@Name("rel") Relationship rel, @Name("newType") String newType) {
        if (rel == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getStartNode().createRelationshipTo(rel.getEndNode(), RelationshipType.withName(newType));
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
    @Procedure(name = "apoc.refactor.to", mode = Mode.WRITE,eager = true)
    @Description("Redirects the given relationship to the given end node.")
    public Stream<RelationshipRefactorResult> to(@Name("rel") Relationship rel, @Name("endNode") Node newNode) {
        if (rel == null || newNode == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getStartNode().createRelationshipTo(newNode, rel.getType());
            copyProperties(rel, newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    @Procedure(name = "apoc.refactor.invert", mode = Mode.WRITE,eager = true)
    @Description("Inverts the direction of the given relationship.")
    public Stream<RelationshipRefactorResult> invert(@Name("rel") Relationship rel) {
        if (rel == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = rel.getEndNode().createRelationshipTo(rel.getStartNode(), rel.getType());
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
    @Procedure(name = "apoc.refactor.from", mode = Mode.WRITE, eager = true)
    @Description("Redirects the given relationship to the given start node.")
    public Stream<RelationshipRefactorResult> from(@Name("rel") Relationship rel, @Name("newNode") Node newNode) {
        if (rel == null || newNode == null) return Stream.empty();
        RelationshipRefactorResult result = new RelationshipRefactorResult(rel.getId());
        try {
            Relationship newRel = newNode.createRelationshipTo(rel.getEndNode(), rel.getType());
            copyProperties(rel, newRel);
            rel.delete();
            return Stream.of(result.withOther(newRel));
        } catch (Exception e) {
            return Stream.of(result.withError(e));
        }
    }

    /**
     * Make properties boolean
     */
    @Procedure(name = "apoc.refactor.normalizeAsBoolean", mode = Mode.WRITE)
    @Description("Refactors the given property to a boolean.")
    public void normalizeAsBoolean(
            @Name("entity") Object entity,
            @Name("propertyKey") String propertyKey,
            @Name("trueValues") List<Object> trueValues,
            @Name("falseValues") List<Object> falseValues) {
        if (entity instanceof Entity) {
            Entity pc = (Entity) entity;
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
    @Description("Creates new category nodes from nodes in the graph with the specified sourceKey as one of its property keys.\n" +
            "The new category nodes are then connected to the original nodes with a relationship of the given type.")
    public void categorize(
            @Name("sourceKey") String sourceKey,
            @Name("type") String relationshipType,
            @Name("outgoing") Boolean outgoing,
            @Name("label") String label,
            @Name("targetKey") String targetKey,
            @Name("copiedKeys") List<String> copiedKeys,
            @Name("batchSize") long batchSize
    ) throws ExecutionException {
        // Verify and adjust arguments
        if (sourceKey == null)
            throw new IllegalArgumentException("Invalid (null) sourceKey");

        if (targetKey == null)
            throw new IllegalArgumentException("Invalid (null) targetKey");

        copiedKeys.remove(targetKey); // Just to be sure

        if (!isUniqueConstraintDefinedFor(label, targetKey)) {
            throw new IllegalArgumentException("Before execute this procedure you must define an unique constraint for the label and the targetKey:\n"
                    + String.format("CREATE CONSTRAINT FOR (n:`%s`) REQUIRE n.`%s` IS UNIQUE", label, targetKey));
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
                futures.add(categorizeNodes(batch, sourceKey, relationshipType, outgoing, label, targetKey, copiedKeys));
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

    @Procedure(name = "apoc.refactor.deleteAndReconnect", mode = Mode.WRITE)
    @Description("Removes the given nodes from the path and reconnects the remaining nodes.")
    public Stream<GraphResult> deleteAndReconnect(@Name("path") Path path, @Name("nodes") List<Node> nodesToRemove, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {

        RefactorConfig refactorConfig = new RefactorConfig(config);

        List<Node> nodes = IterableUtils.toList(path.nodes());
        Set<Relationship> rels = Iterables.asSet(path.relationships());

        if (!nodes.containsAll(nodesToRemove)) {
            return Stream.empty();
        }

        BiFunction<Node, Direction, Relationship> filterRel = (node, direction) -> StreamSupport
                .stream(node.getRelationships(direction).spliterator(), false)
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

                final RefactorConfig.RelationshipSelectionStrategy strategy = refactorConfig.getRelationshipSelectionStrategy();
                switch (strategy) {
                    case INCOMING:
                        newRelType = relationshipIn.getType();
                        newRelProps.putAll(relationshipIn.getAllProperties());
                        break;

                    case OUTGOING:
                        newRelType = relationshipOut.getType();
                        newRelProps.putAll(relationshipOut.getAllProperties());
                        break;

                    default:
                        newRelType = RelationshipType.withName(relationshipIn.getType() + "_" + relationshipOut.getType());
                        newRelProps.putAll(relationshipIn.getAllProperties());
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

        return Stream.of(new GraphResult(nodes, List.copyOf(rels)));
    }

    private boolean isUniqueConstraintDefinedFor(String label, String key) {
        return StreamSupport.stream(tx.schema().getConstraints(Label.label(label)).spliterator(), false)
                .anyMatch(c ->  {
                    if (!c.isConstraintType(ConstraintType.UNIQUENESS)) {
                        return false;
                    }
                    return StreamSupport.stream(c.getPropertyKeys().spliterator(), false)
                            .allMatch(k -> k.equals(key));
                });
    }

    private Future<Void> categorizeNodes(List<Node> batch, String sourceKey, String relationshipType, Boolean outgoing, String label, String targetKey, List<String> copiedKeys) {

        return pools.processBatch(batch, db, (innerTx, node) -> {
            node = Util.rebind(innerTx, node);
            Object value = node.getProperty(sourceKey, null);
            if (value != null) {
                String nodeLabel = Util.sanitize(label);
                String key = Util.sanitize(targetKey);
                String relType = Util.sanitize(relationshipType);
                String q =
                        "WITH $node AS n " +
                                "MERGE (cat:`" + nodeLabel + "` {`" + key + "`: $value}) " +
                                (outgoing ? "MERGE (n)-[:`" + relType + "`]->(cat) "
                                        : "MERGE (n)<-[:`" + relType + "`]-(cat) ") +
                                "RETURN cat";
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

            copyRelationships(source, copyLabels(source, target), true, conf.isCreatingNewSelfRel());
            if (conf.getMergeRelsAllowed()) {
                mergeRelationshipsWithSameTypeAndDirection(target, conf, Direction.OUTGOING, excludeRelIds);
                mergeRelationshipsWithSameTypeAndDirection(target, conf, Direction.INCOMING, excludeRelIds);
            }
            source.delete();
            PropertiesManager.mergeProperties(properties, target, conf);
        } catch (NotFoundException e) {
            log.warn("skipping a node for merging: " + e.getCause().getMessage());
        }
    }

    private void copyRelationships(Node source, Node target, boolean delete, boolean createNewSelfRel) {
        for (Relationship rel : source.getRelationships()) {
            copyRelationship(rel, source, target, createNewSelfRel);
            if (delete) rel.delete();
        }
    }

    private Node copyLabels(Node source, Node target) {
        for (Label label : source.getLabels()) {
            if (!target.hasLabel(label)) {
                target.addLabel(label);
            }
        }
        return target;
    }

    private void copyRelationship(Relationship rel, Node source, Node target, boolean createNewSelfRelf) {
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();

        if (startNode.getId() == endNode.getId() && !createNewSelfRelf) {
            return;
        }

        if (startNode.getId() == source.getId()) {
            startNode = target;
        }

        if (endNode.getId() == source.getId()) {
            endNode = target;
        }

        Relationship newrel = startNode.createRelationshipTo(endNode, rel.getType());
        copyProperties(rel, newrel);
    }

}
