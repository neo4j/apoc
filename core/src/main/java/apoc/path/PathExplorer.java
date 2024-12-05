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
package apoc.path;

import static apoc.path.PathExplorer.NodeFilter.*;

import apoc.algo.Cover;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;

public class PathExplorer {
    public static final Uniqueness UNIQUENESS = Uniqueness.RELATIONSHIP_PATH;
    public static final boolean BFS = true;

    @Context
    public Transaction tx;

    public static class ExpandedPathResult {
        @Description("The expanded path.")
        public Path path;

        public ExpandedPathResult(Path path) {
            this.path = path;
        }
    }

    @NotThreadSafe
    @Procedure("apoc.path.expand")
    @Description(
            "Returns `PATH` values expanded from the start `NODE` following the given `RELATIONSHIP` types from min-depth to max-depth.")
    public Stream<ExpandedPathResult> explorePath(
            @Name(
                            value = "startNode",
                            description =
                                    "The node to start the algorithm from. `startNode` can be of type `STRING` (elementId()), `INTEGER` (id()), `NODE`, or `LIST<STRING | INTEGER | NODE>`.")
                    Object start,
            @Name(value = "relFilter", description = "An allow list of types allowed on the returned relationships.")
                    String pathFilter,
            @Name(value = "labelFilter", description = "An allow list of labels allowed on the returned nodes.")
                    String labelFilter,
            @Name(value = "minDepth", description = "The minimum number of hops allowed in the returned paths.")
                    long minLevel,
            @Name(value = "maxDepth", description = "The maximum number of hops allowed in the returned paths.")
                    long maxLevel) {
        List<Node> nodes = Util.nodeList((InternalTransaction) tx, start);
        return explorePathPrivate(
                        nodes,
                        pathFilter,
                        labelFilter,
                        minLevel,
                        maxLevel,
                        BFS,
                        UNIQUENESS,
                        false,
                        -1,
                        null,
                        null,
                        true)
                .map(ExpandedPathResult::new);
    }

    @NotThreadSafe
    @Procedure("apoc.path.expandConfig")
    @Description(
            "Returns `PATH` values expanded from the start `NODE` with the given `RELATIONSHIP` types from min-depth to max-depth.")
    public Stream<ExpandedPathResult> expandConfig(
            @Name(
                            value = "startNode",
                            description =
                                    "The node to start the algorithm from. `startNode` can be of type `STRING` (elementId()), `INTEGER` (id()), `NODE`, or `LIST<STRING | INTEGER | NODE>.")
                    Object start,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        minLevel = -1 :: INTEGER,
                        maxLevel = -1 :: INTEGER,
                        relationshipFilter :: STRING,
                        labelFilter :: STRING,
                        beginSequenceAtStart = true :: BOOLEAN,
                        uniqueness = "RELATIONSHIP_PATH" :: STRING,
                        bfs = true :: BOOLEAN,
                        filterStartNode = false :: BOOLEAN,
                        limit = -1 :: INTEGER,
                        optional = false :: BOOLEAN,
                        endNodes :: LIST<NODES>,
                        terminatorNodes:: LIST<NODES>,
                        allowlistNodes:: LIST<NODES>,
                        denylistNodes:: LIST<NODES>
                    }
                    """)
                    Map<String, Object> config) {
        return expandConfigPrivate(start, config).map(ExpandedPathResult::new);
    }

    public record SubgraphNodeResult(@Description("Nodes part of the returned subgraph.") Node node) {}

    @NotThreadSafe
    @Procedure("apoc.path.subgraphNodes")
    @Description(
            "Returns the `NODE` values in the sub-graph reachable from the start `NODE` following the given `RELATIONSHIP` types to max-depth.")
    public Stream<SubgraphNodeResult> subgraphNodes(
            @Name(
                            value = "startNode",
                            description =
                                    "The node to start the algorithm from. `startNode` can be of type `STRING` (elementId()), `INTEGER` (id()), `NODE`, or `LIST<STRING | INTEGER | NODE>`.")
                    Object start,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        minLevel = -1 :: INTEGER,
                        maxLevel = -1 :: INTEGER,
                        relationshipFilter :: STRING,
                        labelFilter :: STRING,
                        beginSequenceAtStart = true :: BOOLEAN,
                        uniqueness = "RELATIONSHIP_PATH" :: STRING,
                        bfs = true :: BOOLEAN,
                        filterStartNode = false :: BOOLEAN,
                        limit = -1 :: INTEGER,
                        optional = false :: BOOLEAN,
                        endNodes :: LIST<NODES>,
                        terminatorNodes:: LIST<NODES>,
                        allowlistNodes:: LIST<NODES>,
                        denylistNodes:: LIST<NODES>
                    }
                    """)
                    Map<String, Object> config) {
        Map<String, Object> configMap = new HashMap<>(config);
        configMap.put("uniqueness", "NODE_GLOBAL");

        if (config.containsKey("minLevel")
                && !config.get("minLevel").equals(0L)
                && !config.get("minLevel").equals(1L)) {
            throw new IllegalArgumentException("minLevel can only be 0 or 1 in subgraphNodes()");
        }

        return expandConfigPrivate(start, configMap)
                .map(path -> path == null ? new SubgraphNodeResult(null) : new SubgraphNodeResult(path.endNode()));
    }

    public record SubgraphGraphResult(
            @Description("Nodes part of the returned subgraph.") List<Node> nodes,
            @Description("Relationships part of the returned subgraph.") List<Relationship> relationships) {}

    @NotThreadSafe
    @Procedure("apoc.path.subgraphAll")
    @Description(
            "Returns the sub-graph reachable from the start `NODE` following the given `RELATIONSHIP` types to max-depth.")
    public Stream<SubgraphGraphResult> subgraphAll(
            @Name(
                            value = "startNode",
                            description =
                                    "The node to start the algorithm from. `startNode` can be of type `STRING` (elementId()), `INTEGER` (id()), `NODE`, or `LIST<STRING | INTEGER | NODE>.")
                    Object start,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        minLevel = -1 :: INTEGER,
                        maxLevel = -1 :: INTEGER,
                        relationshipFilter :: STRING,
                        labelFilter :: STRING,
                        beginSequenceAtStart = true :: BOOLEAN,
                        uniqueness = "RELATIONSHIP_PATH" :: STRING,
                        bfs = true :: BOOLEAN,
                        filterStartNode = false :: BOOLEAN,
                        limit = -1 :: INTEGER,
                        optional = false :: BOOLEAN,
                        endNodes :: LIST<NODES>,
                        terminatorNodes:: LIST<NODES>,
                        allowlistNodes:: LIST<NODES>,
                        denylistNodes:: LIST<NODES>
                    }
                    """)
                    Map<String, Object> config) {
        Map<String, Object> configMap = new HashMap<>(config);
        configMap.remove("optional"); // not needed, will return empty collections anyway if no results
        configMap.put("uniqueness", "NODE_GLOBAL");

        if (config.containsKey("minLevel")
                && !config.get("minLevel").equals(0L)
                && !config.get("minLevel").equals(1L)) {
            throw new IllegalArgumentException("minLevel can only be 0 or 1 in subgraphAll()");
        }

        List<Node> subgraphNodes =
                expandConfigPrivate(start, configMap).map(Path::endNode).collect(Collectors.toList());
        List<Relationship> subgraphRels = Cover.coverNodes(subgraphNodes).collect(Collectors.toList());

        return Stream.of(new SubgraphGraphResult(subgraphNodes, subgraphRels));
    }

    public static class SpanningPathResult {
        @Description("A spanning tree path.")
        public Path path;

        public SpanningPathResult(Path path) {
            this.path = path;
        }
    }

    @NotThreadSafe
    @Procedure("apoc.path.spanningTree")
    @Description(
            "Returns spanning tree `PATH` values expanded from the start `NODE` following the given `RELATIONSHIP` types to max-depth.")
    public Stream<SpanningPathResult> spanningTree(
            @Name(
                            value = "startNode",
                            description =
                                    "The node to start the algorithm from. `startNode` can be of type `STRING` (elementId()), `INTEGER` (id()), `NODE`, or `LIST<STRING | INTEGER | NODE>.")
                    Object start,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        minLevel = -1 :: INTEGER,
                        maxLevel = -1 :: INTEGER,
                        relationshipFilter :: STRING,
                        labelFilter :: STRING,
                        beginSequenceAtStart = true :: BOOLEAN,
                        uniqueness = "RELATIONSHIP_PATH" :: STRING,
                        bfs = true :: BOOLEAN,
                        filterStartNode = false :: BOOLEAN,
                        limit = -1 :: INTEGER,
                        optional = false :: BOOLEAN,
                        endNodes :: LIST<NODES>,
                        terminatorNodes:: LIST<NODES>,
                        allowlistNodes:: LIST<NODES>,
                        denylistNodes:: LIST<NODES>
                    }
                    """)
                    Map<String, Object> config) {
        Map<String, Object> configMap = new HashMap<>(config);
        configMap.put("uniqueness", "NODE_GLOBAL");

        if (config.containsKey("minLevel")
                && !config.get("minLevel").equals(0L)
                && !config.get("minLevel").equals(1L)) {
            throw new IllegalArgumentException("minLevel can only be 0 or 1 in spanningTree()");
        }

        return expandConfigPrivate(start, configMap).map(SpanningPathResult::new);
    }

    private Uniqueness getUniqueness(String uniqueness) {
        for (Uniqueness u : Uniqueness.values()) {
            if (u.name().equalsIgnoreCase(uniqueness)) return u;
        }
        throw new RuntimeException("Invalid uniqueness: '" + uniqueness + "'. Must be one of: "
                + String.join(
                        ", ",
                        java.util.Arrays.stream(Uniqueness.values())
                                .map(Enum::name)
                                .toArray(String[]::new))
                + ".");
    }

    private Stream<Path> expandConfigPrivate(@Name("start") Object start, @Name("config") Map<String, Object> config) {
        List<Node> nodes = Util.nodeList((InternalTransaction) tx, start);

        String uniqueness = (String) config.getOrDefault("uniqueness", UNIQUENESS.name());
        String relationshipFilter = (String) config.getOrDefault("relationshipFilter", null);
        String labelFilter = (String) config.getOrDefault("labelFilter", null);
        long minLevel = Util.toLong(config.getOrDefault("minLevel", "-1"));
        long maxLevel = Util.toLong(config.getOrDefault("maxLevel", "-1"));
        boolean bfs = Util.toBoolean(config.getOrDefault("bfs", true));
        boolean filterStartNode = Util.toBoolean(config.getOrDefault("filterStartNode", false));
        long limit = Util.toLong(config.getOrDefault("limit", "-1"));
        boolean optional = Util.toBoolean(config.getOrDefault("optional", false));
        String sequence = (String) config.getOrDefault("sequence", null);
        boolean beginSequenceAtStart = Util.toBoolean(config.getOrDefault("beginSequenceAtStart", true));

        List<Node> endNodes = Util.nodeList((InternalTransaction) tx, config.get("endNodes"));
        List<Node> terminatorNodes = Util.nodeList((InternalTransaction) tx, config.get("terminatorNodes"));
        List<Node> whitelistNodes =
                Util.nodeList((InternalTransaction) tx, config.get("whitelistNodes")); // DEPRECATED REMOVE 6.0
        List<Node> blacklistNodes =
                Util.nodeList((InternalTransaction) tx, config.get("blacklistNodes")); // DEPRECATED REMOVE 6.0
        List<Node> allowlistNodes = Util.nodeList((InternalTransaction) tx, config.get("allowlistNodes"));
        List<Node> denylistNodes = Util.nodeList((InternalTransaction) tx, config.get("denylistNodes"));
        EnumMap<NodeFilter, List<Node>> nodeFilter = new EnumMap<>(NodeFilter.class);

        if (endNodes != null && !endNodes.isEmpty()) {
            nodeFilter.put(END_NODES, endNodes);
        }

        if (terminatorNodes != null && !terminatorNodes.isEmpty()) {
            nodeFilter.put(TERMINATOR_NODES, terminatorNodes);
        }

        // If allowlist/denylist is specified use that (and only that)
        // Else check for the *deprecated* config items
        if (allowlistNodes != null && !allowlistNodes.isEmpty()) {
            nodeFilter.put(ALLOWLIST_NODES, allowlistNodes);
        } else if (whitelistNodes != null && !whitelistNodes.isEmpty()) {
            nodeFilter.put(ALLOWLIST_NODES, whitelistNodes);
        }

        if (denylistNodes != null && !denylistNodes.isEmpty()) {
            nodeFilter.put(DENYLIST_NODES, denylistNodes);
        } else if (blacklistNodes != null && !blacklistNodes.isEmpty()) {
            nodeFilter.put(DENYLIST_NODES, blacklistNodes);
        }

        Stream<Path> results = explorePathPrivate(
                nodes,
                relationshipFilter,
                labelFilter,
                minLevel,
                maxLevel,
                bfs,
                getUniqueness(uniqueness),
                filterStartNode,
                limit,
                nodeFilter,
                sequence,
                beginSequenceAtStart);

        if (optional) {
            return optionalStream(results);
        } else {
            return results;
        }
    }

    private Stream<Path> explorePathPrivate(
            Iterable<Node> startNodes,
            String pathFilter,
            String labelFilter,
            long minLevel,
            long maxLevel,
            boolean bfs,
            Uniqueness uniqueness,
            boolean filterStartNode,
            long limit,
            EnumMap<NodeFilter, List<Node>> nodeFilter,
            String sequence,
            boolean beginSequenceAtStart) {

        Traverser traverser = traverse(
                tx.traversalDescription(),
                startNodes,
                pathFilter,
                labelFilter,
                minLevel,
                maxLevel,
                uniqueness,
                bfs,
                filterStartNode,
                nodeFilter,
                sequence,
                beginSequenceAtStart);

        if (limit == -1) {
            return Iterables.stream(traverser);
        } else {
            return Iterables.stream(traverser).limit(limit);
        }
    }

    /**
     * If the stream is empty, returns a stream of a single null value, otherwise returns the equivalent of the input stream
     * @param stream the input stream
     * @return a stream of a single null value if the input stream is empty, otherwise returns the equivalent of the input stream
     */
    private Stream<Path> optionalStream(Stream<Path> stream) {
        Stream<Path> optionalStream;
        Iterator<Path> itr = stream.iterator();
        if (itr.hasNext()) {
            optionalStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(itr, 0), false);
        } else {
            List<Path> listOfNull = new ArrayList<>();
            listOfNull.add(null);
            optionalStream = listOfNull.stream();
        }

        return optionalStream;
    }

    public static Traverser traverse(
            TraversalDescription td,
            Iterable<Node> startNodes,
            String pathFilter,
            String labelFilter,
            long minLevel,
            long maxLevel,
            Uniqueness uniqueness,
            boolean bfs,
            boolean filterStartNode,
            EnumMap<NodeFilter, List<Node>> nodeFilter,
            String sequence,
            boolean beginSequenceAtStart) {
        // based on the pathFilter definition now the possible relationships and directions must be shown

        td = bfs ? td.breadthFirst() : td.depthFirst();

        // if `sequence` is present, it overrides `labelFilter` and `relationshipFilter`
        if (sequence != null && !sequence.trim().isEmpty()) {
            String[] sequenceSteps = sequence.split(",");
            List<String> labelSequenceList = new ArrayList<>();
            List<String> relSequenceList = new ArrayList<>();

            for (int index = 0; index < sequenceSteps.length; index++) {
                List<String> seq =
                        (beginSequenceAtStart ? index : index - 1) % 2 == 0 ? labelSequenceList : relSequenceList;
                seq.add(sequenceSteps[index]);
            }

            td = td.expand(new RelationshipSequenceExpander(relSequenceList, beginSequenceAtStart));
            td = td.evaluator(new LabelSequenceEvaluator(
                    labelSequenceList, filterStartNode, beginSequenceAtStart, (int) minLevel));
        } else {
            if (pathFilter != null && !pathFilter.trim().isEmpty()) {
                td = td.expand(new RelationshipSequenceExpander(pathFilter.trim(), beginSequenceAtStart));
            }

            if (labelFilter != null && sequence == null && !labelFilter.trim().isEmpty()) {
                td = td.evaluator(new LabelSequenceEvaluator(
                        labelFilter.trim(), filterStartNode, beginSequenceAtStart, (int) minLevel));
            }
        }

        if (minLevel != -1) td = td.evaluator(Evaluators.fromDepth((int) minLevel));
        if (maxLevel != -1) td = td.evaluator(Evaluators.toDepth((int) maxLevel));

        if (nodeFilter != null && !nodeFilter.isEmpty()) {
            List<Node> endNodes = nodeFilter.getOrDefault(END_NODES, Collections.EMPTY_LIST);
            List<Node> terminatorNodes = nodeFilter.getOrDefault(TERMINATOR_NODES, Collections.EMPTY_LIST);
            List<Node> denylistNodes = nodeFilter.getOrDefault(DENYLIST_NODES, Collections.EMPTY_LIST);
            List<Node> allowlistNodes;

            if (nodeFilter.containsKey(ALLOWLIST_NODES)) {
                // need to add to new list since we may need to add to it later
                // encounter "can't add to abstractList" error if we don't do this
                allowlistNodes = new ArrayList<>(nodeFilter.get(ALLOWLIST_NODES));
            } else {
                allowlistNodes = Collections.EMPTY_LIST;
            }

            if (!denylistNodes.isEmpty()) {
                td = td.evaluator(NodeEvaluators.denylistNodeEvaluator(filterStartNode, denylistNodes));
            }

            Evaluator endAndTerminatorNodeEvaluator = NodeEvaluators.endAndTerminatorNodeEvaluator(
                    filterStartNode, (int) minLevel, endNodes, terminatorNodes);
            if (endAndTerminatorNodeEvaluator != null) {
                td = td.evaluator(endAndTerminatorNodeEvaluator);
            }

            if (!allowlistNodes.isEmpty()) {
                // ensure endNodes and terminatorNodes are allowlisted
                allowlistNodes.addAll(endNodes);
                allowlistNodes.addAll(terminatorNodes);
                td = td.evaluator(NodeEvaluators.allowlistNodeEvaluator(filterStartNode, allowlistNodes));
            }
        }

        td = td.uniqueness(uniqueness); // this is how Cypher works !! Uniqueness.RELATIONSHIP_PATH
        // uniqueness should be set as last on the TraversalDescription
        return td.traverse(startNodes);
    }

    // keys to node filter map
    enum NodeFilter {
        ALLOWLIST_NODES,
        DENYLIST_NODES,
        END_NODES,
        TERMINATOR_NODES
    }
}
