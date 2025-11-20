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

import static apoc.path.RelationshipTypeAndDirections.parse;

import apoc.Pools;
import apoc.refactor.util.RefactorConfig;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.token.api.TokenConstants;

public class NodesRestricted {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Context
    public Pools pools;

    @Context
    public ProcedureCallContext procedureCallContext;

    @Procedure(name = "apoc.nodes.link", mode = Mode.WRITE)
    @Description("Creates a linked list of the given `NODE` values connected by the given `RELATIONSHIP` type.")
    public void link(
            @Name(value = "nodes", description = "The list of nodes to be linked.") List<Node> nodes,
            @Name(value = "type", description = "The relationship type name to link the nodes with.") String type,
            @Name(value = "config", defaultValue = "{}", description = "{ avoidDuplicates = false :: BOOLEAN }")
                    Map<String, Object> config) {
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

    @UserFunction("apoc.node.relationship.exists")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Returns a `BOOLEAN` based on whether the given `NODE` has a connecting `RELATIONSHIP` (or whether the given `NODE` has a connecting `RELATIONSHIP` of the given type and direction).")
    public boolean hasRelationshipCypher5(
            @Name(value = "node", description = "The node to check for the specified relationship types.") Node node,
            @Name(
                            value = "relTypes",
                            defaultValue = "",
                            description =
                                    "The relationship types to check for on the given node. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String types) {
        return hasRelationship(node, types);
    }

    @Deprecated
    @UserFunction(name = "apoc.node.relationship.exists", deprecatedBy = "Cypher's `EXISTS {}` expression.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
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

    @UserFunction("apoc.nodes.isDense")
    @Description("Returns true if the given `NODE` is a dense node.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    public boolean isDenseCypher5(
            @Name(value = "node", description = "The node to check for being dense or not.") Node node) {
        return isDense(node, "");
    }

    @UserFunction("apoc.nodes.isDense")
    @Description("Returns true if the given `NODE` is a dense node.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    public boolean isDense(
            @Name(value = "node", description = "The node to check for being dense or not.") Node node,
            @Name(
                            value = "relationshipType",
                            description = "The type of the relationship to check the density of.",
                            defaultValue = "")
                    String relationshipType) {
        try (NodeCursor nodeCursor = ktx.cursors().allocateNodeCursor(ktx.cursorContext())) {
            final long id = ((InternalTransaction) tx).elementIdMapper().nodeId(node.getElementId());
            ktx.dataRead().singleNode(id, nodeCursor);
            if (nodeCursor.next()) {
                var supportsFastDegreeLookup = nodeCursor.supportsFastDegreeLookup();
                if (supportsFastDegreeLookup && !relationshipType.isBlank()) {
                    int denseThreshold = 50;
                    TokenRead tokenRead = ktx.tokenRead();
                    int relTypeId = tokenRead.relationshipType(relationshipType);
                    if (relTypeId != TokenConstants.NO_TOKEN) {
                        return nodeCursor.degreeWithMax(
                                        denseThreshold, RelationshipSelection.selection(relTypeId, Direction.BOTH))
                                >= denseThreshold;
                    }
                }
                return supportsFastDegreeLookup;
            } else {
                throw new IllegalArgumentException("node with id " + id + " does not exist.");
            }
        }
    }
}
