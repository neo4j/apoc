package apoc.graph;

import apoc.cypher.CypherUtils;
import apoc.graph.document.builder.DocumentToGraph;
import apoc.graph.util.GraphsConfig;
import apoc.result.RowResult;
import apoc.result.VirtualGraph;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;
import static apoc.graph.GraphsUtils.extract;

/**
 * @author mh
 * @since 27.05.16
 */
public class Graphs {

    @Context
    public Transaction tx;

    @Procedure("apoc.graph.fromData")
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the given data.")
    public Stream<VirtualGraph> fromData(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> relationships, @Name("name") String name, @Name("props") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name,nodes,relationships,properties));
    }

    @Procedure("apoc.graph.from")
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the given data.")
    public Stream<VirtualGraph> from(@Name("data") Object data, @Name("name") String name, @Name("properties") Map<String,Object> properties) {
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(10000);
        extract(data, nodes,rels);
        return Stream.of(new VirtualGraph(name,nodes,rels,properties));
    }

    @Procedure("apoc.graph.fromPath")
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the data returned by the given path.")
    public Stream<VirtualGraph> fromPath(@Name("path") Path paths, @Name("name") String name, @Name("props") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name, paths.nodes(), paths.relationships(),properties));
    }

    @Procedure("apoc.graph.fromPaths")
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the data returned by the given paths.")
    public Stream<VirtualGraph> fromPaths(@Name("paths") List<Path> paths, @Name("name") String name, @Name("props") Map<String,Object> properties) {
        List<Node> nodes = new ArrayList<>(1000);
        List<Relationship> rels = new ArrayList<>(1000);
        for (Path path : paths) {
            Iterables.addAll(nodes,path.nodes());
            Iterables.addAll(rels,path.relationships());
        }
        return Stream.of(new VirtualGraph(name,nodes,rels,properties));
    }

    @Procedure("apoc.graph.fromDB")
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the data returned by the given database.")
    public Stream<VirtualGraph> fromDB(@Name("name") String name, @Name("props") Map<String,Object> properties) {
        return Stream.of(new VirtualGraph(name,tx.getAllNodes(),tx.getAllRelationships(),properties));
    }

    @Procedure("apoc.graph.fromCypher")
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the data returned by the given Cypher statement.")
    public Stream<VirtualGraph> fromCypher(@Name("statement") String statement,  @Name("params") Map<String,Object> params,@Name("name") String name,  @Name("props") Map<String,Object> properties) {
        params = params == null ? Collections.emptyMap() : params;
        Set<Node> nodes = new HashSet<>(1000);
        Set<Relationship> rels = new HashSet<>(1000);
        Map<String,Object> props = new HashMap<>(properties);
        tx.execute(CypherUtils.withParamMapping(statement, params.keySet()), params).stream().forEach(row -> {
            row.forEach((k,v) -> {
                if (!extract(v,nodes,rels)) {
                    props.put(k,v);
                }
            });

        });
        return Stream.of(new VirtualGraph(name,nodes,rels,props));
    }

    @Procedure(name = "apoc.graph.fromDocument", mode = Mode.WRITE)
    @Description("Generates a virtual sub-graph by extracting all of the nodes and relationships from the data returned by the given JSON file.")
    public Stream<VirtualGraph> fromDocument(@Name("json") Object document, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) throws Exception {
        DocumentToGraph documentToGraph = new DocumentToGraph(tx, new GraphsConfig(config));
        return Stream.of(documentToGraph.create(document));
    }

    @Procedure(name = "apoc.graph.validateDocument", mode = Mode.READ)
    @Description("Validates the JSON file and returns the result of the validation.")
    public Stream<RowResult> validateDocument(@Name("json") Object document, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        GraphsConfig graphConfig = new GraphsConfig(config);
        DocumentToGraph documentToGraph = new DocumentToGraph(tx, graphConfig);
        Map<Long, List<String>> dups = documentToGraph.validateDocument(document);

        return dups.entrySet().stream()
                .map(e -> new RowResult(Util.map("index", e.getKey(), "message", String.join(StringUtils.LF, e.getValue()))));
    }


}
