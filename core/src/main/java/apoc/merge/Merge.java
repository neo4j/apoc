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
package apoc.merge;

import static java.util.Collections.emptyMap;

import apoc.cypher.Cypher;
import apoc.result.NodeResultWithStats;
import apoc.result.RelationshipResultWithStats;
import apoc.result.UpdatedNodeResult;
import apoc.result.UpdatedRelationshipResult;
import apoc.util.Util;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.*;

public class Merge {

    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    @Procedure(value = "apoc.merge.node.eager", mode = Mode.WRITE, eager = true)
    @Description("Merges the given `NODE` values with the given dynamic labels eagerly.")
    public Stream<UpdatedNodeResult> nodesEager(
            @Name(value = "labels", description = "The list of labels used for the generated MERGE statement.")
                    List<String> labelNames,
            @Name(value = "identProps", description = "Properties on the node that are always merged.")
                    Map<String, Object> identProps,
            @Name(
                            value = "onCreateProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is created.")
                    Map<String, Object> onCreateProps,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is matched.")
                    Map<String, Object> onMatchProps) {
        return nodes(labelNames, identProps, onCreateProps, onMatchProps);
    }

    @Procedure(value = "apoc.merge.node", mode = Mode.WRITE)
    @Description("Merges the given `NODE` values with the given dynamic labels.")
    public Stream<UpdatedNodeResult> nodes(
            @Name(value = "labels", description = "The list of labels used for the generated MERGE statement.")
                    List<String> labelNames,
            @Name(value = "identProps", description = "Properties on the node that are always merged.")
                    Map<String, Object> identProps,
            @Name(
                            value = "onCreateProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is created.")
                    Map<String, Object> onCreateProps,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is matched.")
                    Map<String, Object> onMatchProps) {
        final Result nodeResult = getNodeResult(labelNames, identProps, onCreateProps, onMatchProps);
        return nodeResult.columnAs("n").stream().map(node -> new UpdatedNodeResult((Node) node));
    }

    @Procedure(value = "apoc.merge.nodeWithStats.eager", mode = Mode.WRITE, eager = true)
    @Description(
            "Merges the given `NODE` values with the given dynamic labels eagerly. Provides queryStatistics in the result.")
    public Stream<NodeResultWithStats> nodeWithStatsEager(
            @Name(value = "labels", description = "The list of labels used for the generated MERGE statement.")
                    List<String> labelNames,
            @Name(value = "identProps", description = "Properties on the node that are always merged.")
                    Map<String, Object> identProps,
            @Name(
                            value = "onCreateProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is created.")
                    Map<String, Object> onCreateProps,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is matched.")
                    Map<String, Object> onMatchProps) {
        return nodeWithStats(labelNames, identProps, onCreateProps, onMatchProps);
    }

    @Procedure(value = "apoc.merge.nodeWithStats", mode = Mode.WRITE)
    @Description(
            "Merges the given `NODE` values with the given dynamic labels. Provides queryStatistics in the result.")
    public Stream<NodeResultWithStats> nodeWithStats(
            @Name(value = "labels", description = "The list of labels used for the generated MERGE statement.")
                    List<String> labelNames,
            @Name(value = "identProps", description = "Properties on the node that are always merged.")
                    Map<String, Object> identProps,
            @Name(
                            value = "onCreateProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is created.")
                    Map<String, Object> onCreateProps,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a node is matched.")
                    Map<String, Object> onMatchProps) {
        final Result nodeResult = getNodeResult(labelNames, identProps, onCreateProps, onMatchProps);
        return nodeResult.columnAs("n").stream()
                .map(node -> new NodeResultWithStats((Node) node, Cypher.toMap(nodeResult.getQueryStatistics())));
    }

    private Result getNodeResult(
            List<String> labelNames,
            Map<String, Object> identProps,
            Map<String, Object> onCreateProps,
            Map<String, Object> onMatchProps) {
        if (identProps == null || identProps.isEmpty()) {
            throw new IllegalArgumentException("you need to supply at least one identifying property for a merge");
        }

        if (labelNames != null && (labelNames.contains(null) || labelNames.contains(""))) {
            throw new IllegalArgumentException(
                    "The list of label names may not contain any `NULL` or empty `STRING` values. If you wish to merge a `NODE` without a label, pass an empty list instead.");
        }

        String labels;
        if (labelNames == null || labelNames.isEmpty()) {
            labels = "";
        } else {
            labels = ":" + labelNames.stream().map(Util::quote).collect(Collectors.joining(":"));
        }

        Map<String, Object> params =
                Util.map("identProps", identProps, "onCreateProps", onCreateProps, "onMatchProps", onMatchProps);
        String identPropsString = buildIdentPropsString(identProps);

        final String cypher = Util.prefixQuery(
                procedureCallContext,
                "MERGE (n" + labels + "{" + identPropsString
                        + "}) ON CREATE SET n += $onCreateProps ON MATCH SET n += $onMatchProps RETURN n");
        return tx.execute(cypher, params);
    }

    @Procedure(value = "apoc.merge.relationship", mode = Mode.WRITE)
    @Description("Merges the given `RELATIONSHIP` values with the given dynamic types/properties.")
    public Stream<UpdatedRelationshipResult> relationship(
            @Name(value = "startNode", description = "The start node of the relationship.") Node startNode,
            @Name(value = "relType", description = "The type of the relationship.") String relType,
            @Name(value = "identProps", description = "Properties on the relationship that are always merged.")
                    Map<String, Object> identProps,
            @Name(value = "onCreateProps", description = "Properties that are merged when a relationship is created.")
                    Map<String, Object> onCreateProps,
            @Name(value = "endNode", description = "The end node of the relationship.") Node endNode,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a relationship is matched.")
                    Map<String, Object> onMatchProps) {
        final Result execute = getRelResult(startNode, relType, identProps, onCreateProps, endNode, onMatchProps);
        return execute.columnAs("r").stream().map(rel -> new UpdatedRelationshipResult((Relationship) rel));
    }

    @Procedure(value = "apoc.merge.relationshipWithStats", mode = Mode.WRITE)
    @Description(
            "Merges the given `RELATIONSHIP` values with the given dynamic types/properties. Provides queryStatistics in the result.")
    public Stream<RelationshipResultWithStats> relationshipWithStats(
            @Name(value = "startNode", description = "The start node of the relationship.") Node startNode,
            @Name(value = "relType", description = "The type of the relationship.") String relType,
            @Name(value = "identProps", description = "Properties on the relationship that are always merged.")
                    Map<String, Object> identProps,
            @Name(value = "onCreateProps", description = "Properties that are merged when a relationship is created.")
                    Map<String, Object> onCreateProps,
            @Name(value = "endNode", description = "The end node of the relationship.") Node endNode,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a relationship is matched.")
                    Map<String, Object> onMatchProps) {
        final Result relResult = getRelResult(startNode, relType, identProps, onCreateProps, endNode, onMatchProps);
        return relResult.columnAs("r").stream()
                .map(rel -> new RelationshipResultWithStats(
                        (Relationship) rel, Cypher.toMap(relResult.getQueryStatistics())));
    }

    private Result getRelResult(
            Node startNode,
            String relType,
            Map<String, Object> identProps,
            Map<String, Object> onCreateProps,
            Node endNode,
            Map<String, Object> onMatchProps) {
        String identPropsString = buildIdentPropsString(identProps);

        if (relType == null || relType.isEmpty()) {
            throw new IllegalArgumentException(
                    "It is not possible to merge a `RELATIONSHIP` without a `RELATIONSHIP` type.");
        }

        Map<String, Object> params = Util.map(
                "identProps",
                identProps,
                "onCreateProps",
                onCreateProps == null ? emptyMap() : onCreateProps,
                "onMatchProps",
                onMatchProps == null ? emptyMap() : onMatchProps,
                "startNode",
                startNode,
                "endNode",
                endNode);

        final String cypher = "WITH $startNode as startNode, $endNode as endNode " + "MERGE (startNode)-[r:"
                + Util.quote(relType) + "{" + identPropsString + "}]->(endNode) " + "ON CREATE SET r+= $onCreateProps "
                + "ON MATCH SET r+= $onMatchProps "
                + "RETURN r";
        return tx.execute(Util.prefixQuery(procedureCallContext, cypher), params);
    }

    @Procedure(value = "apoc.merge.relationship.eager", mode = Mode.WRITE, eager = true)
    @Description("Merges the given `RELATIONSHIP` values with the given dynamic types/properties eagerly.")
    public Stream<UpdatedRelationshipResult> relationshipEager(
            @Name(value = "startNode", description = "The start node of the relationship.") Node startNode,
            @Name(value = "relType", description = "The type of the relationship.") String relType,
            @Name(value = "identProps", description = "Properties on the relationship that are always merged.")
                    Map<String, Object> identProps,
            @Name(value = "onCreateProps", description = "Properties that are merged when a relationship is created.")
                    Map<String, Object> onCreateProps,
            @Name(value = "endNode", description = "The end node of the relationship.") Node endNode,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a relationship is matched.")
                    Map<String, Object> onMatchProps) {
        return relationship(startNode, relType, identProps, onCreateProps, endNode, onMatchProps);
    }

    @Procedure(value = "apoc.merge.relationshipWithStats.eager", mode = Mode.WRITE, eager = true)
    @Description(
            "Merges the given `RELATIONSHIP` values with the given dynamic types/properties eagerly. Provides queryStatistics in the result.")
    public Stream<RelationshipResultWithStats> relationshipWithStatsEager(
            @Name(value = "startNode", description = "The start node of the relationship.") Node startNode,
            @Name(value = "relType", description = "The type of the relationship.") String relType,
            @Name(value = "identProps", description = "Properties on the relationship that are always merged.")
                    Map<String, Object> identProps,
            @Name(value = "onCreateProps", description = "Properties that are merged when a relationship is created.")
                    Map<String, Object> onCreateProps,
            @Name(value = "endNode", description = "The end node of the relationship.") Node endNode,
            @Name(
                            value = "onMatchProps",
                            defaultValue = "{}",
                            description = "Properties that are merged when a relationship is matched.")
                    Map<String, Object> onMatchProps) {
        return relationshipWithStats(startNode, relType, identProps, onCreateProps, endNode, onMatchProps);
    }

    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps == null) return "";
        return identProps.keySet().stream()
                .map(Util::quote)
                .map(s -> s + ":$identProps." + s)
                .collect(Collectors.joining(","));
    }
}
