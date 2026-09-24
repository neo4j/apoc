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

import static apoc.ApocConfig.APOC_MAX_HOPS;
import static apoc.ApocConfig.DEFAULT_MAX_HOPS;
import static apoc.ApocConfig.apocConfig;
import static apoc.path.RelationshipTypeAndDirections.parse;
import static apoc.util.Util.getNodeElementId;
import static apoc.util.Util.getNodeId;

import apoc.util.collection.Iterables;
import java.util.*;
import java.util.function.Function;
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
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.procedure.memory.ProcedureMemory;
import org.neo4j.procedure.memory.ProcedureMemoryTracker;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class Neighbors {

    @Context
    public Transaction tx;

    @Context
    public ProcedureMemory procedureMemory;

    @Context
    public TerminationGuard terminationGuard;

    private ResourceIterable<Relationship> getRelationshipsByTypeAndDirection(
            Node node, Pair<RelationshipType, Direction> typesAndDirection) {
        // as policy if both elements in the pair are null we return an empty result
        if (typesAndDirection.getLeft() == null) {
            return typesAndDirection.getRight() == null
                    ? Iterables.asResourceIterable(Collections.<Relationship>emptyList())
                    : node.getRelationships(typesAndDirection.getRight());
        }
        if (typesAndDirection.getRight() == null) {
            return typesAndDirection.getLeft() == null
                    ? Iterables.asResourceIterable(Collections.<Relationship>emptyList())
                    : node.getRelationships(typesAndDirection.getLeft());
        }
        return node.getRelationships(typesAndDirection.getRight(), typesAndDirection.getLeft());
    }

    public record NeighborNodeResult(@Description("A neighboring node.") Node node) {}

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
        requireDistance(distance);
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();
        requireNode(node);

        return tracked(tracker -> StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                withinHops(node, types, distance, tracker).iterator(), Spliterator.SORTED),
                        false)
                .map(x ->
                        new NeighborNodeResult(tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, x)))));
    }

    public record NeighborLongResult(
            @Description("The total count of neighboring nodes within the given hop distance.") Long value) {}

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
        requireDistance(distance);
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();
        requireNode(node);

        return tracked(tracker -> Stream.of(new NeighborLongResult(
                withinHops(node, types, distance, tracker).getLongCardinality())));
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
        requireDistance(distance);
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();
        requireNode(node);
        requireWithinMaxHops(distance);

        return tracked(tracker -> {
            List<Roaring64NavigableMap> seen = hops(node, types, distance, tracker);
            // The contract is exactly `distance` rows; hops past the point where the frontier emptied are empty, so
            // they are generated here rather than stored.
            return Stream.concat(
                            seen.stream().map(this::toNodes),
                            Stream.generate(() -> List.<Node>of()).limit(distance - seen.size()))
                    .map(NeighbouringNodeListResult::new);
        });
    }

    public record NeighborListResult(
            @Description("A list of neighbor counts for each distinct hop distance.") List<Object> value) {}

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
        requireDistance(distance);
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();
        requireNode(node);
        requireWithinMaxHops(distance);

        return tracked(tracker -> {
            List<Roaring64NavigableMap> seen = hops(node, types, distance, tracker);
            int size = Math.toIntExact(distance);
            final var estimator = procedureMemory.heapEstimator();
            tracker.allocateHeap(estimator.shallowSizeOfInstance(ArrayList.class)
                    + estimator.shallowSizeOfObjectArray(size)
                    + size * estimator.shallowSizeOfInstance(Long.class));
            List<Object> counts = new ArrayList<>(size);
            for (Roaring64NavigableMap hop : seen) {
                counts.add(hop.getLongCardinality());
            }
            while (counts.size() < size) {
                counts.add(0L);
            }
            return Stream.of(new NeighborListResult(counts));
        });
    }

    public record NeighboringNodeResult(@Description("A neighboring node.") Node node) {}

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
        requireDistance(distance);
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();
        requireNode(node);

        return tracked(tracker -> StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                atHop(node, types, distance, tracker).iterator(), Spliterator.SORTED),
                        false)
                .map(y -> new NeighboringNodeResult(
                        tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, y)))));
    }

    public record NeighboursLongResult(
            @Description("The total count of neighboring nodes at the given hop distance.") Long value) {}

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
        requireDistance(distance);
        if (distance < 1) return Stream.empty();
        if (types == null || types.isEmpty()) return Stream.empty();
        requireNode(node);

        return tracked(tracker -> Stream.of(
                new NeighboursLongResult(atHop(node, types, distance, tracker).getLongCardinality())));
    }

    private static void requireNode(Node node) {
        if (node == null) {
            throw new IllegalArgumentException("'node' must not be null");
        }
    }

    // Checked after the early returns above, which never touch the node or the graph: those calls returned an empty
    // result before and still do. Only `distance` is checked first, as the early return itself unboxes it.
    private static void requireDistance(Long distance) {
        if (distance == null) {
            throw new IllegalArgumentException("'distance' must not be null");
        }
    }

    /**
     * byhop and byhop.count produce output proportional to `distance` rather than to the graph, and streamed rows
     * are never seen by the memory tracker, so they alone are bounded by a hop count.
     */
    private static void requireWithinMaxHops(long distance) {
        int maxHops = apocConfig().getInt(APOC_MAX_HOPS, DEFAULT_MAX_HOPS);
        if (distance > maxHops) {
            throw new IllegalArgumentException("'distance' must not exceed " + maxHops + ", the value of "
                    + APOC_MAX_HOPS + ", but was " + distance);
        }
    }

    /**
     * Runs `body` with a fresh memory tracker whose charge lives as long as the returned stream. Try-with-resources
     * would release it when the procedure returns, before the stream is consumed.
     */
    private <T> Stream<T> tracked(Function<ProcedureMemoryTracker, Stream<T>> body) {
        ProcedureMemoryTracker tracker = procedureMemory.newTracker();
        try {
            return body.apply(tracker).onClose(tracker::close);
        } catch (RuntimeException e) {
            tracker.close();
            throw e;
        }
    }

    /** Every node within `distance` hops, excluding the start node. */
    private Roaring64NavigableMap withinHops(Node node, String types, long distance, ProcedureMemoryTracker tracker) {
        Roaring64NavigableMap all = new Roaring64NavigableMap();
        for (Roaring64NavigableMap hop : hops(node, types, distance, tracker)) {
            all.or(hop);
        }
        tracker.allocateHeap(all.getLongSizeInBytes());
        all.removeLong(getNodeId((InternalTransaction) tx, node.getElementId()));
        return all;
    }

    /** The nodes first reached at exactly `distance` hops; empty when the search ran out of nodes before that. */
    private Roaring64NavigableMap atHop(Node node, String types, long distance, ProcedureMemoryTracker tracker) {
        List<Roaring64NavigableMap> seen = hops(node, types, distance, tracker);
        return seen.size() < distance ? new Roaring64NavigableMap() : seen.get(seen.size() - 1);
    }

    /**
     * The nodes first reached at each hop, one bitmap per hop actually executed. Stops as soon as a hop reaches no
     * new node, so a large `distance` on a shallow graph costs nothing; every bitmap is charged to `tracker`.
     */
    private List<Roaring64NavigableMap> hops(Node node, String types, long distance, ProcedureMemoryTracker tracker) {
        final var estimator = procedureMemory.heapEstimator();
        final long bitmapOverhead = estimator.shallowSizeOfInstance(Roaring64NavigableMap.class);
        final long nodeId = getNodeId((InternalTransaction) tx, node.getElementId());
        final List<Pair<RelationshipType, Direction>> typesAndDirections = parse(types);

        List<Roaring64NavigableMap> seen = new ArrayList<>();
        // The start node plus every node reached at an earlier hop. Duplicates the ids held in `seen`, roughly doubling
        // the memory, in exchange for one andNot per hop instead of one against every earlier hop.
        Roaring64NavigableMap visited = new Roaring64NavigableMap();
        long seenBytes = 0L;
        long charged = 0L;

        // First Hop
        Roaring64NavigableMap hop = new Roaring64NavigableMap();
        for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
            try (ResourceIterable<Relationship> rels = getRelationshipsByTypeAndDirection(node, pair)) {
                for (Relationship r : rels) {
                    hop.add(getNodeId(
                            (InternalTransaction) tx, r.getOtherNode(node).getElementId()));
                }
            }
        }

        while (true) {
            seen.add(hop);
            visited.or(hop);
            visited.addLong(nodeId);
            // A hop's size is only known once it is built, so this charges after the fact: the limit can be overshot by
            // one hop before the query is refused. A finished hop never changes again, so only the newest one and
            // `visited` need measuring.
            seenBytes += bitmapOverhead + hop.getLongSizeInBytes();
            long size = estimator.shallowSizeOfObjectArray(seen.size())
                    + seenBytes
                    + bitmapOverhead
                    + visited.getLongSizeInBytes();
            if (size > charged) {
                tracker.allocateHeap(size - charged);
                charged = size;
            }

            if (seen.size() >= distance || hop.isEmpty()) break;

            Roaring64NavigableMap previous = hop;
            hop = new Roaring64NavigableMap();
            Iterator<Long> iterator = previous.iterator();
            while (iterator.hasNext()) {
                // Per expanded node rather than per hop, so a single hop over a huge frontier is still killable
                terminationGuard.check();
                Node current = tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, iterator.next()));
                for (Pair<RelationshipType, Direction> pair : typesAndDirections) {
                    try (ResourceIterable<Relationship> rels = getRelationshipsByTypeAndDirection(current, pair)) {
                        for (Relationship r : rels) {
                            hop.add(getNodeId(
                                    (InternalTransaction) tx,
                                    r.getOtherNode(current).getElementId()));
                        }
                    }
                }
            }
            hop.andNot(visited);
        }
        return seen;
    }

    private List<Node> toNodes(Roaring64NavigableMap ids) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(ids.iterator(), Spliterator.SORTED), false)
                .map(y -> tx.getNodeByElementId(getNodeElementId((InternalTransaction) tx, y)))
                .collect(Collectors.toList());
    }
}
