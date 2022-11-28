package apoc.neighbors;

import apoc.result.ListResult;
import apoc.result.LongResult;
import apoc.result.NodeListResult;
import apoc.result.NodeResult;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.path.RelationshipTypeAndDirections.parse;

public class Neighbors {

    @Context
    public Transaction tx;

    private Iterable<Relationship> getRelationshipsByTypeAndDirection(Node node, Pair<RelationshipType, Direction> typesAndDirection) {
        // as policy if both elements in the pair are null we return an empty result
        if (typesAndDirection.getLeft() == null) {
            return typesAndDirection.getRight() == null ? Collections.emptyList() : node.getRelationships(typesAndDirection.getRight());
        }
        if (typesAndDirection.getRight() == null) {
            return typesAndDirection.getLeft() == null ? Collections.emptyList() : node.getRelationships(typesAndDirection.getLeft());
        }
        return node.getRelationships(typesAndDirection.getRight(), typesAndDirection.getLeft());
    }

    private long getNodeId(String elementId) {
        return ((InternalTransaction) tx).elementIdMapper().nodeId(elementId);
    }
    private String getNodeElementId(long id) {
        return ((InternalTransaction) tx).elementIdMapper().nodeElementId(id);
    }

    @Procedure("apoc.neighbors.tohop")
    @Description("Returns all nodes connected by the given relationship types within the specified distance.\n" +
            "Nodes are returned individually for each row.")
    public Stream<NodeResult> neighbors(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        final long startNodeId = getNodeId(node.getElementId());

        // Initialize bitmaps for iteration
        Roaring64NavigableMap seen = new Roaring64NavigableMap();
        Roaring64NavigableMap nextA = new Roaring64NavigableMap();
        Roaring64NavigableMap nextB = new Roaring64NavigableMap();
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId(nodeElementId);
        seen.addLong(nodeId);
        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);

        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                nextB.addLong(getNodeId(r.getOtherNode(node).getElementId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            // next even Hop
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();
            iterator = nextB.iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId(iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        nextA.add(getNodeId(r.getOtherNode(node).getElementId()));
                    }
                }
            }

            i++;
            if (i < distance) {
                // next odd Hop
                nextA.andNot(seen);
                seen.or(nextA);
                nextB.clear();
                iterator = nextA.iterator();
                while (iterator.hasNext()) {
                    nodeElementId = getNodeElementId(iterator.next());
                    node = tx.getNodeByElementId(nodeElementId);
                    for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                        for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                            nextB.add(getNodeId(r.getOtherNode(node).getElementId()));
                        }
                    }
                }
            }
        }
        if((distance % 2) == 0) {
            seen.or(nextA);
        } else {
            seen.or(nextB);
        }
        // remove starting node
        seen.removeLong(startNodeId);

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(seen.iterator(), Spliterator.SORTED), false)
                .map(x -> new NodeResult(tx.getNodeByElementId(getNodeElementId(x))));
    }

    @Procedure("apoc.neighbors.tohop.count")
    @Description("Returns the count of all nodes connected by the given relationships in the pattern within the specified distance.")
    public Stream<LongResult> neighborsCount(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        final long startNodeId = getNodeId(node.getElementId());

        // Initialize bitmaps for iteration
        Roaring64NavigableMap seen = new Roaring64NavigableMap();
        Roaring64NavigableMap nextA = new Roaring64NavigableMap();
        Roaring64NavigableMap nextB = new Roaring64NavigableMap();
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId(nodeElementId);
        seen.add(nodeId);
        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                nextB.add(getNodeId(r.getOtherNode(node).getElementId()));

            }
        }

        for(int i = 1; i < distance; i++) {
            // next even Hop
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();
            iterator = nextB.iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId(iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        nextA.add(getNodeId(r.getOtherNode(node).getElementId()));
                    }
                }
            }

            i++;
            if (i < distance) {
                // next odd Hop
                nextA.andNot(seen);
                seen.or(nextA);
                nextB.clear();
                iterator = nextA.iterator();
                while (iterator.hasNext()) {
                    nodeElementId = getNodeElementId(iterator.next());
                    node = tx.getNodeByElementId(nodeElementId);
                    for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                        for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                            nextB.add(getNodeId(r.getOtherNode(node).getElementId()));
                        }
                    }
                }
            }
        }
        if((distance % 2) == 0) {
            seen.or(nextA);
        } else {
            seen.or(nextB);
        }
        // remove starting node
        seen.removeLong(startNodeId);

        return Stream.of(new LongResult(seen.getLongCardinality()));
    }

    @Procedure("apoc.neighbors.byhop")
    @Description("Returns all nodes connected by the given relationship types within the specified distance.")
    public Stream<NodeListResult> neighborsByHop(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId(nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(getNodeId(r.getOtherNode(node).getElementId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId(iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return Arrays.stream(seen).map(x -> new NodeListResult(
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(x.iterator(), Spliterator.SORTED), false)
                        .map(y -> tx.getNodeByElementId(getNodeElementId(y)))
                        .collect(Collectors.toList())));
    }

    @Procedure("apoc.neighbors.byhop.count")
    @Description("Returns the count of all nodes connected by the given relationship types within the specified distance.")
    public Stream<ListResult> neighborsByHopCount(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId(nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(getNodeId(r.getOtherNode(node).getElementId()));

            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId(iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        ArrayList counts = new ArrayList<Long>();
        for(int i = 0; i < distance; i++) {
            counts.add(seen[i].getLongCardinality());
        }

        return Stream.of(new ListResult(counts));
    }

    @Procedure("apoc.neighbors.athop")
    @Description("Returns all nodes connected by the given relationship types at the specified distance.")
    public Stream<NodeResult> neighborsAtHop(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types==null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for(int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId(nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(getNodeId(r.getOtherNode(node).getElementId()));
            }
        }

        for(int i = 1; i < distance; i++) {
            iterator = seen[i-1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId(iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for(int j = 0; j < i; j++){
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(seen[distance.intValue() - 1].iterator(), Spliterator.SORTED), false)
                .map(y -> new NodeResult(tx.getNodeByElementId(getNodeElementId(y))));
    }

    @Procedure("apoc.neighbors.athop.count")
    @Description("Returns the count of all nodes connected by the given relationship types at the specified distance.")
    public Stream<LongResult> neighborsAtHopCount(@Name("node") Node node, @Name(value = "relTypes", defaultValue = "") String types, @Name(value="distance", defaultValue = "1") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for (int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId(nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(getNodeId(r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            iterator = seen[i - 1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId(iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for (int j = 0; j < i; j++) {
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return Stream.of(new LongResult(seen[distance.intValue() - 1].getLongCardinality()));
    }
}
