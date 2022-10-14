package apoc.nodes;

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
import apoc.util.collection.Iterables;
import apoc.util.Util;
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
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.storageengine.api.RelationshipSelection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.path.RelationshipTypeAndDirections.format;
import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.refactor.util.RefactorUtil.copyProperties;
import static apoc.util.Util.map;

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
    @Description("Detects all path cycles in the given node list.\n" +
            "This procedure can be limited on relationships as well.")
    public Stream<PathResult> cycles(@Name("nodes") List<Node> nodes, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        NodesConfig conf = new NodesConfig(config);
        final List<String> types = conf.getRelTypes();
        Stream<Path> paths = nodes.stream().flatMap(start -> {
            boolean allRels = types.isEmpty();
            final RelationshipType[] relTypes = types.stream().map(RelationshipType::withName).toArray(RelationshipType[]::new);
            final Iterable<Relationship> relationships = allRels
                    ? start.getRelationships(Direction.OUTGOING)
                    : start.getRelationships(Direction.OUTGOING, relTypes);

            PathExpanderBuilder expanderBuilder;
            if (allRels) {
                expanderBuilder = PathExpanderBuilder.allTypes(Direction.OUTGOING);
            } else {
                expanderBuilder = PathExpanderBuilder.empty();
                for (RelationshipType relType: relTypes) {
                    expanderBuilder = expanderBuilder.add(relType, Direction.OUTGOING);
                }
            }
            final PathExpander<Path> pathExpander = expanderBuilder.build();

            PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                    new BasicEvaluationContext(tx, db),
                    pathExpander,
                    conf.getMaxDepth());
            Map<Long, List<Long>> dups = new HashMap<>();
            return Iterables.stream(relationships)
                    // to prevent duplicated (start and end nodes with double-rels)
                    .filter(relationship -> {
                        final List<Long> nodeDups = dups.computeIfAbsent(relationship.getStartNodeId(), (key) -> new ArrayList<>());
                        if (nodeDups.contains(relationship.getEndNodeId())) {
                            return false;
                        }
                        nodeDups.add(relationship.getEndNodeId());
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
    @Description("Creates a linked list of the given nodes connected by the given relationship type.")
    public void link(@Name("nodes") List<Node> nodes, @Name("type") String type, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        RefactorConfig conf = new RefactorConfig(config);
        Iterator<Node> it = nodes.iterator();
        if (it.hasNext()) {
            RelationshipType relType = RelationshipType.withName(type);
            Node node = it.next();
            while (it.hasNext()) {
                Node next = it.next();
                final boolean createRelationship = !conf.isAvoidDuplicates() || (conf.isAvoidDuplicates() && !connected(node, next, type));
                if (createRelationship) {
                    node.createRelationshipTo(next, relType);
                }
                node = next;
            }
        }
    }

    @Procedure("apoc.nodes.get")
    @Description("Returns all nodes with the given ids.")
    public Stream<NodeResult> get(@Name("nodes") Object ids) {
        return Util.nodeStream(tx, ids).map(NodeResult::new);
    }

    @Procedure(name = "apoc.nodes.delete", mode = Mode.WRITE)
    @Description("Deletes all nodes with the given ids.")
    public Stream<LongResult> delete(@Name("nodes") Object ids, @Name("batchSize") long batchSize) {
        Iterator<Node> it = Util.nodeStream(tx, ids).iterator();
        long count = 0;
        while (it.hasNext()) {
            final List<Node> batch = Util.take(it, (int)batchSize);
//            count += Util.inTx(api,() -> batch.stream().peek( n -> {n.getRelationships().forEach(Relationship::delete);n.delete();}).count());
            count += Util.inTx(db, pools, (txInThread) -> {txInThread.execute("FOREACH (n in $nodes | DETACH DELETE n)",map("nodes",batch)).close();return batch.size();});
        }
        return Stream.of(new LongResult(count));
    }

    @Procedure("apoc.get.rels")
    @Description("Returns all relationships with the given ids.")
    public Stream<RelationshipResult> rels(@Name("rels") Object ids) {
        return Util.relsStream(tx, ids).map(RelationshipResult::new);
    }

    @UserFunction("apoc.node.relationship.exists")
    @Description("Returns a boolean based on whether the given node has a relationship (or whether the given node has a relationship of the given type and direction).")
    public boolean hasRelationship(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types) {
        if (types == null || types.isEmpty()) return node.hasRelationship();
        long id = node.getId();
        try ( NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {

            ktx.dataRead().singleNode(id, nodeCursor);
            nodeCursor.next();
            TokenRead tokenRead = ktx.tokenRead();

            for (Pair<RelationshipType, Direction> pair : parse(types)) {
                int typeId = tokenRead.relationshipType(pair.getLeft().name());
                Direction direction = pair.getRight();

                int count;
                switch (direction) {
                    case INCOMING:
                        count = org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming(nodeCursor, typeId);
                        break;
                    case OUTGOING:
                        count = org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing(nodeCursor, typeId);
                        break;
                    case BOTH:
                        count = org.neo4j.internal.kernel.api.helpers.Nodes.countAll(nodeCursor, typeId);
                        break;
                    default:
                        throw new UnsupportedOperationException("invalid direction " + direction);
                }
                if (count > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    @UserFunction("apoc.nodes.connected")
    @Description("Returns true when a given node is directly connected to another given node.\n" +
            "This function is optimized for dense nodes.")
    public boolean connected(@Name("startNode") Node start, @Name("endNode") Node end, @Name(value = "types", defaultValue = "") String types)  {
        if (start == null || end == null) return false;
        if (start.equals(end)) return true;

        long startId = start.getId();
        long endId = end.getId();
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

//            boolean startDense = startNodeCursor.supportsFastDegreeLookup();
            dataRead.singleNode(endId, endNodeCursor);
            if (!endNodeCursor.next()) {
                throw new IllegalArgumentException("node with id " + endId + " does not exist.");
            }
//            boolean endDense = endNodeCursor.supportsFastDegreeLookup();

            return connected(startNodeCursor, endId, typedDirections(tokenRead, pairs, true));


//            if (!startDense) return connected(startNodeCursor, endId, typedDirections(tokenRead, pairs, true));
//            if (!endDense) return connected(endNodeCursor, startId, typedDirections(tokenRead, pairs, false));
//            return connectedDense(startNodeCursor, endNodeCursor, typedDirections(tokenRead, pairs, true));
        }
    }

    @Procedure("apoc.nodes.collapse")
    @Description("Merges nodes together in the given list.\n" +
            "The nodes are then combined to become one node, with all labels of the previous nodes attached to it, and all relationships pointing to it.")
    public Stream<VirtualPathResult> collapse(@Name("nodes") List<Node> nodes, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (nodes == null || nodes.isEmpty()) return Stream.empty();
        if (nodes.size() == 1) return Stream.of(new VirtualPathResult(nodes.get(0), null, null));
        Set<Node> nodeSet = new LinkedHashSet<>(nodes);
        RefactorConfig conf = new RefactorConfig(config);
        VirtualNode first = createVirtualNode(nodeSet, conf);
        if (first.getRelationships().iterator().hasNext()) {
            return StreamSupport.stream(first.getRelationships().spliterator(), false)
                    .map(relationship -> new VirtualPathResult(relationship.getStartNode(), relationship, relationship.getEndNode()));
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

    private void createVirtualRelationships(Set<Node> nodes, VirtualNode virtualNode, Node node, RefactorConfig refactorConfig) {
        node.getRelationships().forEach(relationship -> {
            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();

            if (nodes.contains(startNode) && nodes.contains(endNode)) {
                if (refactorConfig.isSelfRel()) {
                    createOrMergeVirtualRelationship(virtualNode, refactorConfig, relationship, virtualNode,  Direction.OUTGOING);
                }
            } else {
                if (startNode.getId() == node.getId()) {
                    createOrMergeVirtualRelationship(virtualNode, refactorConfig, relationship, endNode,  Direction.OUTGOING);
                } else {
                    createOrMergeVirtualRelationship(virtualNode, refactorConfig, relationship, startNode,  Direction.INCOMING);
                }
            }
        });
    }

    private void createOrMergeVirtualRelationship(VirtualNode virtualNode, RefactorConfig refactorConfig, Relationship source, Node node, Direction direction) {
        Iterable<Relationship> rels = virtualNode.getRelationships(direction, source.getType());
        Optional<Relationship> first = StreamSupport.stream(rels.spliterator(), false).filter(relationship -> relationship.getOtherNode(virtualNode).equals(node)).findFirst();
        if (refactorConfig.isMergeVirtualRels() && first.isPresent()) {
            mergeRelationship(source, first.get(), refactorConfig);
        } else {
            if (direction==Direction.OUTGOING)
               copyProperties(source, virtualNode.createRelationshipTo(node, source.getType()));
            if (direction==Direction.INCOMING) 
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
        try (RelationshipTraversalCursor relationship = ktx.cursors().allocateRelationshipTraversalCursor(ktx.cursorContext())) {
            start.relationships(relationship, RelationshipSelection.selection(Direction.BOTH));
            while (relationship.next()) {
                if (relationship.otherNodeReference() ==end) {
                    if (typedDirections==null) {
                        return true;
                    } else {
                        int direction = relationship.targetNodeReference() == end ? 0 : 1 ;
                        int[] types = typedDirections[direction];
                        if (arrayContains(types, relationship.type())) return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean arrayContains(int[] array, int element) {
        for (int i=0; i<array.length; i++) {
            if (array[i]==element) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param ops
     * @param pairs
     * @param outgoing
     * @return a int[][] where the first index is 0 for outgoing, 1 for incoming. second array contains rel type ids
     */
    private int[][] typedDirections(TokenRead ops, List<Pair<RelationshipType, Direction>> pairs, boolean outgoing) {
        if (pairs==null) return null;
        int from=0;int to=0;
        int[][] result = new int[2][pairs.size()];
        int outIdx = Direction.OUTGOING.ordinal();
        int inIdx = Direction.INCOMING.ordinal();
        for (Pair<RelationshipType, Direction> pair : pairs) {
            int type = ops.relationshipType(pair.getLeft().name());
            if (type == -1) continue;
            if (pair.getRight() != Direction.INCOMING) {
                result[outIdx][from++]= type;
            }
            if (pair.getRight() != Direction.OUTGOING) {
                result[inIdx][to++]= type;
            }
        }
        result[outIdx] = Arrays.copyOf(result[outIdx], from);
        result[inIdx] = Arrays.copyOf(result[inIdx], to);
        if (!outgoing) {
            int[] tmp = result[outIdx];
            result[outIdx] = result[inIdx];
            result[inIdx] = tmp;
        }
        return result;
    }

    static class Degree implements Comparable<Degree> {
        public final long node;
        private final long group;
        public final int degree;
        public final long other;

        public Degree(long node, long group, int degree, long other) {
            this.node = node;
            this.group = group;
            this.degree = degree;
            this.other = other;
        }

        @Override
        public int compareTo(Degree o) {
            return Integer.compare(degree, o.degree);
        }

        public boolean isConnected(Read read, RelationshipTraversalCursor relationship) {
            read.relationships(node, group, RelationshipSelection.ALL_RELATIONSHIPS, relationship);
            while (relationship.next()) {
                if (relationship.otherNodeReference()==other) {
                    return true;
                }
            }
            return false;
        }
    }

//    private boolean connectedDense(NodeCursor start, NodeCursor end, int[][] typedDirections) {
//        List<Degree> degrees = new ArrayList<>(32);
//
//        Read read = ktx.dataRead();
//
//        try (RelationshipGroupCursor relationshipGroup = ktx.cursors().allocateRelationshipGroupCursor()) {
//            addDegreesForNode(read, start, end, degrees, relationshipGroup, typedDirections);
//            addDegreesForNode(read, end, start, degrees, relationshipGroup, typedDirections);
//        }
//
//
//        Collections.sort(degrees);
//        try (RelationshipTraversalCursor relationship = ktx.cursors().allocateRelationshipTraversalCursor()) {
//            for (Degree degree : degrees) {
//                if (degree.isConnected(ktx.dataRead(), relationship)) return true;
//            }
//            return false;
//        }
//    }
//
//    private void addDegreesForNode(Read dataRead, NodeCursor node, NodeCursor other, List<Degree> degrees, RelationshipGroupCursor relationshipGroup, int[][] typedDirections) {
//        long nodeId = node.nodeReference();
//        long otherId = other.nodeReference();
//
//        dataRead.relationshipGroups(nodeId, node.relationshipGroupReference(), relationshipGroup);
//        while (relationshipGroup.next()) {
//            int type = relationshipGroup.type();
//            if ((typedDirections==null) || (arrayContains(typedDirections[0], type))) {
//                addDegreeWithDirection(degrees, relationshipGroup.outgoingReference(), relationshipGroup.outgoingCount(), nodeId, otherId);
//            }
//
//            if ((typedDirections==null) || (arrayContains(typedDirections[1], type))) {
//                addDegreeWithDirection(degrees, relationshipGroup.incomingReference(), relationshipGroup.incomingCount(), nodeId, otherId);
//            }
//        }
//    }

    private void addDegreeWithDirection(List<Degree> degrees, long relationshipGroup, int degree, long nodeId, long otherId) {
        if (degree > 0 ) {
            degrees.add(new Degree(nodeId, relationshipGroup, degree, otherId));
        }
    }

    @UserFunction("apoc.node.labels")
    @Description("returns labels for (virtual) nodes")
    public List<String> labels(@Name("node") Node node) {
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
    @Description("Returns the labels for the given virtual node.")
    public Long id(@Name("node") Node node) {
        return (node == null) ? null : node.getId();
    }

    @UserFunction("apoc.rel.id")
    @Description("Returns the id for the given virtual relationship.")
    public Long relId(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getId();
    }

    @UserFunction("apoc.rel.startNode")
    @Description("Returns the start node for the given virtual relationship.")
    public Node startNode(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getStartNode();
    }

    @UserFunction("apoc.rel.endNode")
    @Description("Returns the end node for the given virtual relationship.")
    public Node endNode(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getEndNode();
    }

    @UserFunction("apoc.rel.type")
    @Description("Returns the type for the given virtual relationship.")
    public String type(@Name("rel") Relationship rel) {
        return (rel == null) ? null : rel.getType().name();
    }

    @UserFunction("apoc.any.properties")
    @Description("Returns all properties of the given object.\n" +
            "The object can be a virtual node, a real node, a virtual relationship, a real relationship, or a map.")
    public Map<String,Object> properties(@Name("object") Object thing, @Name(value = "keys", defaultValue = "null") List<String> keys) {
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
    @Description("Returns the property for the given key from an object.\n" +
            "The object can be a virtual node, a real node, a virtual relationship, a real relationship, or a map.")
    public Object property(@Name("object") Object thing, @Name(value = "key") String key) {
        if (thing == null || key == null) return null;
        if (thing instanceof Map) {
            return ((Map<String, Object>) thing).get(key);
        }
        if (thing instanceof Entity) {
            return ((Entity) thing).getProperty(key,null);
        }
        return null;
    }

    @UserFunction("apoc.node.degree")
    @Description("Returns the total degrees for the given node.")
    public long degree(@Name("node") Node node, @Name(value = "relTypes",defaultValue = "") String types) {
        if (types==null || types.isEmpty()) return node.getDegree();
        long degree = 0;
        for (Pair<RelationshipType, Direction> pair : parse(types)) {
            degree += getDegreeSafe(node, pair.getLeft(), pair.getRight());
        }
        return degree;
    }

    @UserFunction("apoc.node.degree.in")
    @Description("Returns the total number of incoming relationships to the given node.")
    public long degreeIn(@Name("node") Node node, @Name(value = "relTypes",defaultValue = "") String type) {

        if (type==null || type.isEmpty()) {
            return node.getDegree(Direction.INCOMING);
        }

        return node.getDegree(RelationshipType.withName(type), Direction.INCOMING);

    }

    @UserFunction("apoc.node.degree.out")
    @Description("Returns the total number of outgoing relationships from the given node.")
    public long degreeOut(@Name("node") Node node, @Name(value = "relTypes",defaultValue = "") String type) {

        if (type==null || type.isEmpty()) {
            return node.getDegree(Direction.OUTGOING);
        }

        return node.getDegree(RelationshipType.withName(type), Direction.OUTGOING);

    }


    @UserFunction("apoc.node.relationship.types")
    @Description("Returns a list of distinct relationship types for the given node.")
    public List<String> relationshipTypes(@Name("node") Node node, @Name(value = "relTypes",defaultValue = "") String types) {
        if (node==null) return null;
        List<String> relTypes = Iterables.stream(node.getRelationshipTypes()).map(RelationshipType::name).collect(Collectors.toList());
        if (types == null || types.isEmpty()) return relTypes;
        List<String> result = new ArrayList<>(relTypes.size());
        for (Pair<RelationshipType, Direction> p : parse(types)) {
            String name = p.getLeft().name();
            if (relTypes.contains(name) && node.hasRelationship(p.getRight(),p.getLeft())) {
                result.add(name);
            }
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationship.types")
    @Description("Returns a list of distinct relationship types from the given list of nodes.")
    public List<Map<String, Object>> nodesRelationshipTypes(@Name("nodes") Object ids, @Name(value = "types",defaultValue = "") String types) {
        if (ids == null) return null;
        return Util.nodeStream(tx, ids)
                .map(node -> {
                    final List<String> relationshipTypes = relationshipTypes(node, types);
                    if (relationshipTypes == null) {
                        // in order to avoid possible NullPointerException because we'll use Collectors#toMap which uses Map#merge
                        return null;
                    }
                    return map("node", node, "types", relationshipTypes);
                })
                .filter(e -> e != null)
                .collect(Collectors.toList());
    }

    @UserFunction("apoc.node.relationships.exist")
    @Description("Returns a boolean based on whether the given node has relationships (or whether the given nodes has relationships of the given type and direction).")
    public Map<String,Boolean> relationshipExists(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types) {
        if (node == null || types == null || types.isEmpty()) return null;
        List<String> relTypes = Iterables.stream(node.getRelationshipTypes()).map(RelationshipType::name).toList();
        Map<String,Boolean> result =  new HashMap<>();
        for (Pair<RelationshipType, Direction> p : parse(types)) {
            String name = p.getLeft().name();
            boolean hasRelationship = relTypes.contains(name) && node.hasRelationship(p.getRight(), p.getLeft());
            result.put(format(p), hasRelationship);
        }
        return result;
    }

    @UserFunction("apoc.nodes.relationships.exist")
    @Description("Returns a boolean based on whether or not the given nodes have the given relationships.")
    public List<Map<String, Object>> nodesRelationshipExists(@Name("nodes") Object ids, @Name(value = "types", defaultValue = "") String types) {
        if (ids == null) return null;
        return Util.nodeStream(tx, ids)
                .map(node -> {
                    final Map<String, Boolean> existsMap = relationshipExists(node, types);
                    if (existsMap == null) {
                        // in order to avoid possible NullPointerException because we'll use Collectors#toMap which uses Map#merge
                        return null;
                    }
                    return map("node", node, "exists", existsMap);
                })
                .filter(e -> e != null)
                .collect(Collectors.toList());
    }

    @UserFunction("apoc.nodes.isDense")
    @Description("Returns true if the given node is a dense node.")
    public boolean isDense(@Name("node") Node node) {
        try (NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {
            final long id = node.getId();
            ktx.dataRead().singleNode(id, nodeCursor);
            if (nodeCursor.next()) {
                return nodeCursor.supportsFastDegreeLookup();
            } else {
                throw new IllegalArgumentException("node with id " + id + " does not exist.");
            }
        }
    }

    @UserFunction("apoc.any.isDeleted")
    @Description("returns boolean value for nodes and rele existance")
    public boolean isDeleted(@Name("thing") Object thing) {
        if (thing == null) return true;
        final String query;
        if (thing instanceof Node) {
            query = "MATCH (n) WHERE ID(n) = $id RETURN COUNT(n) = 1 AS exists";
        }
        else if (thing instanceof Relationship){
            query = "MATCH ()-[r]->() WHERE ID(r) = $id RETURN COUNT(r) = 1 AS exists";
        }
        else {
            throw new IllegalArgumentException("expected Node or Relationship but was " + thing.getClass().getSimpleName());
        }
        return !(boolean) tx.execute(query, Map.of("id",((Entity)thing).getId())).next().get("exists");
    }

    // works in cases when relType is null
    private int getDegreeSafe(Node node, RelationshipType relType, Direction direction) {
        if (relType == null) {
            return node.getDegree(direction);
        }

        return node.getDegree(relType, direction);
    }

}
