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
package apoc.agg;

import static apoc.util.Util.map;

import apoc.coll.SetBackedList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.*;

/**
 * @author mh
 * @since 18.12.17
 */
public class Graph {
    @UserAggregationFunction("apoc.agg.graph")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Returns all distinct `NODE` and `RELATIONSHIP` values collected into a `MAP` with the keys `nodes` and `relationships`.")
    public GraphAggregation graphCypher5() {
        return new GraphAggregation();
    }

    @Deprecated
    @UserAggregationFunction(name = "apoc.agg.graph", deprecatedBy = "Cypher's `COLLECT {}` expression.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "Returns all distinct `NODE` and `RELATIONSHIP` values collected into a `MAP` with the keys `nodes` and `relationships`.")
    public GraphAggregation graph() {
        return new GraphAggregation();
    }

    public static class GraphAggregation {

        private Set<Node> nodes = new HashSet<>();
        private Set<Relationship> rels = new HashSet<>();
        private Set<Relationship> plainRels = new HashSet<>();

        @UserAggregationUpdate
        public void aggregate(
                @Name(value = "path", description = "A path to return nodes and relationships from.") Object element) {
            consume(element);
        }

        public void consume(@Name("path") Object element) {
            if (element instanceof Node) {
                nodes.add((Node) element);
            }
            if (element instanceof Relationship) {
                plainRels.add((Relationship) element);
            }
            if (element instanceof Path) {
                Path path = (Path) element;
                for (Node node : path.nodes()) nodes.add(node);
                for (Relationship rel : path.relationships()) rels.add(rel);
            }
            if (element instanceof Map) {
                ((Map) element).values().forEach(this::consume);
            }
            if (element instanceof Iterable) {
                ((Iterable) element).forEach(this::consume);
            }
        }

        @UserAggregationResult
        public Map<String, Object> result() {
            if (!plainRels.isEmpty()) {
                for (Relationship rel : plainRels) {
                    nodes.add(rel.getStartNode());
                    nodes.add(rel.getEndNode());
                }
                rels.addAll(plainRels);
            }
            return map("nodes", new SetBackedList<>(nodes), "relationships", new SetBackedList<>(rels));
        }
    }
}
