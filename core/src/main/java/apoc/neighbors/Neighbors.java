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
package apoc.neighbors;

import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.util.Util.getNodeElementId;
import static apoc.util.Util.getNodeId;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class Neighbors {

    @Context
    public Transaction tx;

    private Iterable<Relationship> getRelationshipsByTypeAndDirection(
            Node node, Pair<RelationshipType, Direction> typesAndDirection) {
        // as policy if both elements in the pair are null we return an empty result
        if (typesAndDirection.getLeft() == null) {
            return typesAndDirection.getRight() == null
                    ? Collections.emptyList()
                    : node.getRelationships(typesAndDirection.getRight());
        }
        if (typesAndDirection.getRight() == null) {
            return typesAndDirection.getLeft() == null
                    ? Collections.emptyList()
                    : node.getRelationships(typesAndDirection.getLeft());
        }
        return node.getRelationships(typesAndDirection.getRight(), typesAndDirection.getLeft());
    }

    public record NeighborNodeResult(
            @Description("A neighboring node.") Node node) {}

    @Procedure("apoc.neighbors.tohop")
    @Description(
            "Returns all `NODE` values connected by the given `RELATIONSHIP` types within the specified distance.\n"
                    + "`NODE` values are returned individually for each row.")
    public Stream<NeighborNodeResult> neighbors(
            @Name(value = "node", description = "The starting node for the algorithm.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "A list of relationship types to follow. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types,
            @Name(value = "distance", defaultValue = "1", description = "The max number of hops to take.")
                    Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        final long startNodeId = getNodeId((InternalTransaction) tx, node.getElementId());

        // Initialize bitmaps for iteration
        Roaring64NavigableMap seen = new Roaring64NavigableMap();
        Roaring64NavigableMap nextA = new Roaring64NavigableMap();
        Roaring64NavigableMap nextB = new Roaring64NavigableMap();
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId((InternalTransaction) tx, nodeElementId);
        seen.addLong(nodeId);
        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);

        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                nextB.addLong(
                        getNodeId((InternalTransaction) tx, r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            // next even Hop
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();
            iterator = nextB.iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        nextA.add(getNodeId(
                                (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
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
                    nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                    node = tx.getNodeByElementId(nodeElementId);
                    for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                        for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                            nextB.add(getNodeId(
                                    (InternalTransaction) tx,
                                    r.getOtherNode(node).getElementId()));
                        }
                    }
                }
            }
        }
        if ((distance % 2) == 0) {
            seen.or(nextA);
        } else {
            seen.or(nextB);
        }
        // remove starting node
        seen.removeLong(startNodeId);

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(seen.iterator(), Spliterator.SORTED), false)
                .map(x -> new NeighborNodeResult(tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, x))));
    }

    public record NeighborLongResult(
            @Description("The total count of neighboring nodes within the given hop distance.")
            Long value) {}

    @Procedure("apoc.neighbors.tohop.count")
    @Description(
            "Returns the count of all `NODE` values connected by the given `RELATIONSHIP` values in the pattern within the specified distance.")
    public Stream<NeighborLongResult> neighborsCount(
            @Name(value = "node", description = "The starting node for the algorithm.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "A list of relationship types to follow. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types,
            @Name(value = "distance", defaultValue = "1", description = "The max number of hops to take.")
                    Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        final long startNodeId = getNodeId((InternalTransaction) tx, node.getElementId());

        // Initialize bitmaps for iteration
        Roaring64NavigableMap seen = new Roaring64NavigableMap();
        Roaring64NavigableMap nextA = new Roaring64NavigableMap();
        Roaring64NavigableMap nextB = new Roaring64NavigableMap();
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId((InternalTransaction) tx, nodeElementId);
        seen.add(nodeId);
        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                nextB.add(
                        getNodeId((InternalTransaction) tx, r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            // next even Hop
            nextB.andNot(seen);
            seen.or(nextB);
            nextA.clear();
            iterator = nextB.iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        nextA.add(getNodeId(
                                (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
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
                    nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                    node = tx.getNodeByElementId(nodeElementId);
                    for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                        for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                            nextB.add(getNodeId(
                                    (InternalTransaction) tx,
                                    r.getOtherNode(node).getElementId()));
                        }
                    }
                }
            }
        }
        if ((distance % 2) == 0) {
            seen.or(nextA);
        } else {
            seen.or(nextB);
        }
        // remove starting node
        seen.removeLong(startNodeId);

        return Stream.of(new NeighborLongResult(seen.getLongCardinality()));
    }

    public static class NeighbouringNodeListResult {
        @Description("A list of neighboring nodes at a distinct hop distance.")
        public final List<Node> nodes;

        public NeighbouringNodeListResult(List<Node> value) {
            this.nodes = value;
        }
    }

    @Procedure("apoc.neighbors.byhop")
    @Description(
            "Returns all `NODE` values connected by the given `RELATIONSHIP` types within the specified distance. Returns `LIST<NODE>` values, where each `PATH` of `NODE` values represents one row of the `LIST<NODE>` values.")
    public Stream<NeighbouringNodeListResult> neighborsByHop(
            @Name(value = "node", description = "The starting node for the algorithm.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "A list of relationship types to follow. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types,
            @Name(value = "distance", defaultValue = "1", description = "The max number of hops to take.")
                    Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for (int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId((InternalTransaction) tx, nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(
                        getNodeId((InternalTransaction) tx, r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            iterator = seen[i - 1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(
                                (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for (int j = 0; j < i; j++) {
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return Arrays.stream(seen)
                .map(x -> new NeighbouringNodeListResult(StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(x.iterator(), Spliterator.SORTED), false)
                        .map(y -> tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, y)))
                        .collect(Collectors.toList())));
    }

    public record NeighborListResult(
            @Description("A list of neighbor counts for each distinct hop distance.")
            List<Object> value) {}

    @Procedure("apoc.neighbors.byhop.count")
    @Description(
            "Returns the count of all `NODE` values connected by the given `RELATIONSHIP` types within the specified distance.")
    public Stream<NeighborListResult> neighborsByHopCount(
            @Name(value = "node", description = "The starting node for the algorithm.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "A list of relationship types to follow. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types,
            @Name(value = "distance", defaultValue = "1", description = "The max number of hops to take.")
                    Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for (int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId((InternalTransaction) tx, nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(
                        getNodeId((InternalTransaction) tx, r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            iterator = seen[i - 1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(
                                (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for (int j = 0; j < i; j++) {
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        ArrayList counts = new ArrayList<Long>();
        for (int i = 0; i < distance; i++) {
            counts.add(seen[i].getLongCardinality());
        }

        return Stream.of(new NeighborListResult(counts));
    }

    public record NeighboringNodeResult(
            @Description("A neighboring node.") Node node) {}

    @Procedure("apoc.neighbors.athop")
    @Description("Returns all `NODE` values connected by the given `RELATIONSHIP` types at the specified distance.")
    public Stream<NeighboringNodeResult> neighborsAtHop(
            @Name(value = "node", description = "The starting node for the algorithm.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "A list of relationship types to follow. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types,
            @Name(value = "distance", defaultValue = "1", description = "The number of hops to take.") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for (int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId((InternalTransaction) tx, nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(
                        getNodeId((InternalTransaction) tx, r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            iterator = seen[i - 1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(
                                (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for (int j = 0; j < i; j++) {
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                seen[distance.intValue() - 1].iterator(), Spliterator.SORTED),
                        false)
                .map(y -> new NeighboringNodeResult(
                        tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, y))));
    }

    public record NeighboursLongResult(
            @Description("The total count of neighboring nodes at the given hop distance.")
            Long value) {}

    @Procedure("apoc.neighbors.athop.count")
    @Description(
            "Returns the count of all `NODE` values connected by the given `RELATIONSHIP` types at the specified distance.")
    public Stream<NeighboursLongResult> neighborsAtHopCount(
            @Name(value = "node", description = "The starting node for the algorithm.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "A list of relationship types to follow. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types,
            @Name(value = "distance", defaultValue = "1", description = "The number of hops to take.") Long distance) {
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();

        // Initialize bitmaps for iteration
        Roaring64NavigableMap[] seen = new Roaring64NavigableMap[distance.intValue()];
        for (int i = 0; i < distance; i++) {
            seen[i] = new Roaring64NavigableMap();
        }
        String nodeElementId = node.getElementId();
        long nodeId = getNodeId((InternalTransaction) tx, nodeElementId);

        Iterator<Long> iterator;

        List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);
        // First Hop
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                seen[0].add(
                        getNodeId((InternalTransaction) tx, r.getOtherNode(node).getElementId()));
            }
        }

        for (int i = 1; i < distance; i++) {
            iterator = seen[i - 1].iterator();
            while (iterator.hasNext()) {
                nodeElementId = getNodeElementId((InternalTransaction) tx, iterator.next());
                node = tx.getNodeByElementId(nodeElementId);
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    for (Relationship r : getRelationshipsByTypeAndDirection(node, pair)) {
                        seen[i].add(getNodeId(
                                (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
                    }
                }
            }
            for (int j = 0; j < i; j++) {
                seen[i].andNot(seen[j]);
                seen[i].removeLong(nodeId);
            }
        }

        return Stream.of(new NeighboursLongResult(seen[distance.intValue() - 1].getLongCardinality()));
    }
}
