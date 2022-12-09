package apoc.export.arrow;

import apoc.Pools;
import apoc.export.util.NodesAndRelsSubGraph;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import apoc.result.VirtualGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public class ExportArrow {

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log logger;

    @Context
    public TerminationGuard terminationGuard;

    @Procedure("apoc.export.arrow.stream.all")
    @Description("Exports the full database as an arrow byte array.")
    public Stream<ByteArrayResult> all(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return new ExportArrowService(db, pools, terminationGuard, logger).stream(new DatabaseSubGraph(tx), new ArrowConfig(config));
    }

    @Procedure("apoc.export.arrow.stream.graph")
    @Description("Exports the given graph as an arrow byte array.")
    public Stream<ByteArrayResult> graph(@Name("graph") Object graph, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        final SubGraph subGraph;
        if (graph instanceof Map) {
            Map<String, Object> mGraph = (Map<String, Object>) graph;
            if (!mGraph.containsKey("nodes")) {
                throw new IllegalArgumentException("Graph Map must contains `nodes` field and `relationships` optionally");
            }
            subGraph = new NodesAndRelsSubGraph(tx, (Collection<Node>) mGraph.get("nodes"),
                    (Collection<Relationship>) mGraph.get("relationships"));
        } else if (graph instanceof VirtualGraph) {
            VirtualGraph vGraph = (VirtualGraph) graph;
            subGraph = new NodesAndRelsSubGraph(tx, vGraph.nodes(), vGraph.relationships());
        } else {
            throw new IllegalArgumentException("Supported inputs are VirtualGraph, Map");
        }
        return new ExportArrowService(db, pools, terminationGuard, logger).stream(subGraph, new ArrowConfig(config));
    }

    @Procedure("apoc.export.arrow.stream.query")
    @Description("Exports the given Cypher query as an arrow byte array.")
    public Stream<ByteArrayResult> query(@Name("query") String query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        Map<String, Object> params = config == null ? Collections.emptyMap() : (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query, params);
        return new ExportArrowService(db, pools, terminationGuard, logger).stream(result, new ArrowConfig(config));
    }

    @Procedure("apoc.export.arrow.all")
    @Description("Exports the full database as an arrow file.")
    public Stream<ProgressInfo> all(@Name("file") String fileName, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return new ExportArrowService(db, pools, terminationGuard, logger).file(fileName, new DatabaseSubGraph(tx), new ArrowConfig(config));
    }

    @Procedure("apoc.export.arrow.graph")
    @Description("Exports the given graph as an arrow file.")
    public Stream<ProgressInfo> graph(@Name("file") String fileName, @Name("graph") Object graph, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        final SubGraph subGraph;
        if (graph instanceof Map) {
            Map<String, Object> mGraph = (Map<String, Object>) graph;
            if (!mGraph.containsKey("nodes")) {
                throw new IllegalArgumentException("Graph Map must contains `nodes` field and `relationships` optionally");
            }
            subGraph = new NodesAndRelsSubGraph(tx, (Collection<Node>) mGraph.get("nodes"),
                    (Collection<Relationship>) mGraph.get("relationships"));
        } else if (graph instanceof VirtualGraph) {
            VirtualGraph vGraph = (VirtualGraph) graph;
            subGraph = new NodesAndRelsSubGraph(tx, vGraph.nodes(), vGraph.relationships());
        } else {
            throw new IllegalArgumentException("Supported inputs are VirtualGraph, Map");
        }
        return new ExportArrowService(db, pools, terminationGuard, logger).file(fileName, subGraph, new ArrowConfig(config));
    }

    @Procedure("apoc.export.arrow.query")
    @Description("Exports the results from the given Cypher query as an arrow file.")
    public Stream<ProgressInfo> query(@Name("file") String fileName, @Name("query") String query, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        Map<String, Object> params = config == null ? Collections.emptyMap() : (Map<String, Object>) config.getOrDefault("params", Collections.emptyMap());
        Result result = tx.execute(query, params);
        return new ExportArrowService(db, pools, terminationGuard, logger).file(fileName, result, new ArrowConfig(config));
    }
}