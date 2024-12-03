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
package apoc.graph;

import static apoc.graph.GraphsUtils.extract;

import apoc.cypher.CypherUtils;
import apoc.result.RowResult;
import apoc.result.VirtualGraph;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.*;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

/**
 * @author mh
 * @since 27.05.16
 */
public class Graphs {

    @Context
    public Transaction tx;

    @Procedure("apoc.graph.fromData")
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the given data.")
    public Stream<VirtualGraph> fromData(
            @Name(value = "nodes", description = "The nodes to include in the resulting graph.") List<Node> nodes,
            @Name(value = "rels", description = "The relationship to include in the resulting graph.")
                    List<Relationship> relationships,
            @Name(value = "name", description = "The name of the resulting graph.") String name,
            @Name(value = "props", description = "The properties to include in the resulting graph.")
                    Map<String, Object> properties) {
        return Stream.of(new VirtualGraph(name, nodes, relationships, properties));
    }

    @Procedure("apoc.graph.from")
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the given data.")
    public Stream<VirtualGraph> from(
            @Name(
                            value = "data",
                            description =
                                    "An object to extract nodes and relationships from. It can be of type; NODE | RELATIONSHIP | PATH | LIST<ANY>.")
                    Object data,
            @Name(value = "name", description = "The name of the resulting graph.") String name,
            @Name(value = "props", description = "The properties to be present in the resulting graph.")
                    Map<String, Object> properties) {
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(10000);
        extract(data, nodes, rels);
        return Stream.of(new VirtualGraph(name, nodes, rels, properties));
    }

    @Procedure("apoc.graph.fromPath")
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the data returned by the given `PATH`.")
    public Stream<VirtualGraph> fromPath(
            @Name(value = "path", description = "A path to extract the nodes and relationships from.") Path paths,
            @Name(value = "name", description = "The name to give the resulting graph.") String name,
            @Name(value = "props", description = "The properties to include in the resulting graph.")
                    Map<String, Object> properties) {
        return Stream.of(new VirtualGraph(name, paths.nodes(), paths.relationships(), properties));
    }

    @Procedure("apoc.graph.fromPaths")
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the data returned by the given `PATH` values.")
    public Stream<VirtualGraph> fromPaths(
            @Name(value = "paths", description = "A list of paths to extract nodes and relationships from.")
                    List<Path> paths,
            @Name(value = "name", description = "The name of the resulting graph.") String name,
            @Name(value = "props", description = "The properties to include in the resulting graph.")
                    Map<String, Object> properties) {
        List<Node> nodes = new ArrayList<>(1000);
        List<Relationship> rels = new ArrayList<>(1000);
        for (Path path : paths) {
            Iterables.addAll(nodes, path.nodes());
            Iterables.addAll(rels, path.relationships());
        }
        return Stream.of(new VirtualGraph(name, nodes, rels, properties));
    }

    @Procedure("apoc.graph.fromDB")
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the data returned by the given database.")
    public Stream<VirtualGraph> fromDB(
            @Name(value = "name", description = "The name of the resulting graph.") String name,
            @Name(value = "props", description = "The properties to be present in the resulting graph.")
                    Map<String, Object> properties) {
        return Stream.of(new VirtualGraph(name, tx.getAllNodes(), tx.getAllRelationships(), properties));
    }

    @NotThreadSafe
    @Procedure("apoc.graph.fromCypher")
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the data returned by the given Cypher statement.")
    public Stream<VirtualGraph> fromCypher(
            @Name(value = "statement", description = "The Cypher statement to create the graph from.") String statement,
            @Name(value = "params", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> params,
            @Name(value = "name", description = "The name of the resulting graph.") String name,
            @Name(value = "props", description = "The properties to include in the resulting graph.")
                    Map<String, Object> properties) {
        params = params == null ? Collections.emptyMap() : params;
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(1000);
        Map<String, Object> props = new HashMap<>(properties);
        tx.execute(CypherUtils.withParamMapping(statement, params.keySet()), params).stream()
                .forEach(row -> {
                    row.forEach((k, v) -> {
                        if (!extract(v, nodes, rels)) {
                            props.put(k, v);
                        }
                    });
                });
        return Stream.of(new VirtualGraph(name, nodes, rels, props));
    }

    @Procedure(name = "apoc.graph.fromDocument", mode = Mode.WRITE)
    @Description(
            "Generates a virtual sub-graph by extracting all of the `NODE` and `RELATIONSHIP` values from the data returned by the given JSON file.")
    public Stream<VirtualGraph> fromDocument(
            @Name(value = "json", description = "A JSON object to generate a graph from.") Object document,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            write = false :: BOOLEAN,
                            labelField = 'type' :: STRING.
                            idField = 'id' :: STRING,
                            generateID = true :: BOOLEAN,
                            defaultLabel = '' :: STRING,
                            skipValidation = false :: BOOLEAN,
                            mappings = {} :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        DocumentToGraph documentToGraph = new DocumentToGraph(tx, new GraphsConfig(config));
        return Stream.of(documentToGraph.create(document));
    }

    @Procedure(name = "apoc.graph.validateDocument", mode = Mode.READ)
    @Description("Validates the JSON file and returns the result of the validation.")
    public Stream<RowResult> validateDocument(
            @Name(value = "json", description = "The JSON object to validate.") Object document,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                            write = false :: BOOLEAN,
                            labelField = 'type' :: STRING.
                            idField = 'id' :: STRING,
                            generateID = true :: BOOLEAN,
                            defaultLabel = '' :: STRING,
                            skipValidation = false :: BOOLEAN,
                            mappings = {} :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        GraphsConfig graphConfig = new GraphsConfig(config);
        DocumentToGraph documentToGraph = new DocumentToGraph(tx, graphConfig);
        Map<Long, List<String>> dups = documentToGraph.validateDocument(document);

        return dups.entrySet().stream()
                .map(e -> new RowResult(
                        Util.map("index", e.getKey(), "message", String.join(StringUtils.LF, e.getValue()))));
    }
}
