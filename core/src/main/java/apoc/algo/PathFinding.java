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
package apoc.algo;

import static apoc.algo.PathFindingUtils.buildPathExpander;

import apoc.result.PathResult;
import apoc.result.WeightedPathResult;
import apoc.util.Util;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphalgo.*;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;

public class PathFinding {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure("apoc.algo.aStar")
    @Description(
            "Runs the A* search algorithm to find the optimal path between two `NODE` values, using the given `RELATIONSHIP` property name for the cost function.")
    public Stream<WeightedPathResult> aStar(
            @Name(value = "startNode", description = "The node to start the search from.") Node startNode,
            @Name(value = "endNode", description = "The node to end the search on.") Node endNode,
            @Name(
                            value = "relTypesAndDirections",
                            description =
                                    "The relationship types to restrict the algorithm to. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String relTypesAndDirs,
            @Name(value = "weightPropertyName", description = "The name of the property to use as the weight.")
                    String weightPropertyName,
            @Name(value = "latPropertyName", description = "The name of the property to use as the latitude.")
                    String latPropertyName,
            @Name(value = "lonPropertyName", description = "The name of the property to use as the longitude.")
                    String lonPropertyName) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                new BasicEvaluationContext(tx, db),
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(weightPropertyName),
                CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName));
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure("apoc.algo.aStarConfig")
    @Description(
            "Runs the A* search algorithm to find the optimal path between two `NODE` values, using the given `RELATIONSHIP` property name for the cost function.\n"
                    + "This procedure looks for weight, latitude and longitude properties in the config.")
    public Stream<WeightedPathResult> aStarConfig(
            @Name(value = "startNode", description = "The node to start the search from.") Node startNode,
            @Name(value = "endNode", description = "The node to end the search on.") Node endNode,
            @Name(
                            value = "relTypesAndDirections",
                            description =
                                    "The relationship types to restrict the algorithm to. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String relTypesAndDirs,
            @Name(
                            value = "config",
                            description =
                                    "{ weight = 'distance' :: STRING, default = Double.MAX_VALUE :: FLOAT, y = 'latitude' :: STRING, x = 'longitude' :: STRING, pointPropName :: STRING }")
                    Map<String, Object> config) {

        config = config == null ? Collections.emptyMap() : config;
        String relationshipCostPropertyKey =
                config.getOrDefault("weight", "distance").toString();
        double defaultCost = ((Number) config.getOrDefault("default", Double.MAX_VALUE)).doubleValue();
        String pointPropertyName = (String) config.get("pointPropName");
        final EstimateEvaluator<Double> estimateEvaluator;
        if (pointPropertyName != null) {
            estimateEvaluator = new PathFindingUtils.GeoEstimateEvaluatorPointCustom(pointPropertyName);
        } else {
            String latPropertyName = config.getOrDefault("y", "latitude").toString();
            String lonPropertyName = config.getOrDefault("x", "longitude").toString();
            estimateEvaluator = CommonEvaluators.geoEstimateEvaluator(latPropertyName, lonPropertyName);
        }
        PathFinder<WeightedPath> algo = GraphAlgoFactory.aStar(
                new BasicEvaluationContext(tx, db),
                buildPathExpander(relTypesAndDirs),
                CommonEvaluators.doubleCostEvaluator(relationshipCostPropertyKey, defaultCost),
                estimateEvaluator);
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure("apoc.algo.dijkstra")
    @Description("Runs Dijkstra's algorithm using the given `RELATIONSHIP` property as the cost function.")
    public Stream<WeightedPathResult> dijkstra(
            @Name(value = "startNode", description = "The node to start the search from.") Node startNode,
            @Name(value = "endNode", description = "The node to end the search on.") Node endNode,
            @Name(
                            value = "relTypesAndDirections",
                            description =
                                    "The relationship types to restrict the algorithm to. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String relTypesAndDirs,
            @Name(value = "weightPropertyName", description = "The name of the property to use as the weight.")
                    String weightPropertyName,
            @Name(
                            value = "defaultWeight",
                            defaultValue = "NaN",
                            description =
                                    "The `defaultWeight` is used when no specific weight is provided for the given relationship or node. The default value for defaultWeight is NaN.")
                    double defaultWeight,
            @Name(
                            value = "numberOfWantedPaths",
                            defaultValue = "1",
                            description = "The number of wanted paths to return.")
                    long numberOfWantedPaths) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander(relTypesAndDirs),
                (relationship, direction) -> Util.toDouble(relationship.getProperty(weightPropertyName, defaultWeight)),
                (int) numberOfWantedPaths);
        return WeightedPathResult.streamWeightedPathResult(startNode, endNode, algo);
    }

    @NotThreadSafe
    @Procedure("apoc.algo.allSimplePaths")
    @Description(
            "Runs a search algorithm to find all of the simple paths between the given `RELATIONSHIP` values, up to a max depth described by `maxNodes`.\n"
                    + "The returned paths will not contain loops.")
    public Stream<PathResult> allSimplePaths(
            @Name(value = "startNode", description = "The node to start the search from.") Node startNode,
            @Name(value = "endNode", description = "The node to end the search on.") Node endNode,
            @Name(
                            value = "relTypesAndDirections",
                            description =
                                    "The relationship types to restrict the algorithm to. Relationship types are represented using APOC's rel-direction-pattern syntax; `[<]RELATIONSHIP_TYPE1[>]|[<]RELATIONSHIP_TYPE2[>]|...`.")
                    String relTypesAndDirs,
            @Name(value = "maxNodes", description = "The max depth (in terms of nodes) the algorithm will explore.")
                    long maxNodes) {

        PathFinder<Path> algo = GraphAlgoFactory.allSimplePaths(
                new BasicEvaluationContext(tx, db), buildPathExpander(relTypesAndDirs), (int) maxNodes);
        Iterable<Path> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false).map(PathResult::new);
    }
}
