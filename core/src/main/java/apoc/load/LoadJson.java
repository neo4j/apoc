package apoc.load;

import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.load.LoadJsonUtils.loadJsonStream;
import static apoc.util.CompressionConfig.COMPRESSION;

public class LoadJson {

    private static final String AUTH_HEADER_KEY = "Authorization";
    private static final String LOAD_TYPE = "json";

    @Context
    public GraphDatabaseService db;

    @SuppressWarnings("unchecked")
    @Procedure("apoc.load.jsonArray")
    @Description("Loads array from a JSON URL (e.g. web-API) to then import the given JSON file as a stream of values.")
    public Stream<ObjectResult> jsonArray(@Name("url") String url, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return JsonUtil.loadJson(url, null, null, path, true, (List<String>) config.get("pathOptions"))
                .flatMap((value) -> {
                    if (value instanceof List) {
                        List list = (List) value;
                        if (list.isEmpty()) return Stream.empty();
                        if (list.get(0) instanceof Map) return list.stream().map(ObjectResult::new);
                    }
                    return Stream.of(new ObjectResult(value));
                });
        // throw new RuntimeException("Incompatible Type " + (value == null ? "null" : value.getClass()));
    }

    @Procedure("apoc.load.json")
    @Description("Imports JSON file as a stream of values if the given JSON file is an array.\n" +
            "If the given JSON file is a map, this procedure imports a single value instead.")
    public Stream<MapResult> json(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        return jsonParams(urlOrKeyOrBinary,null,null, path, config);
    }

    @SuppressWarnings("unchecked")
    @Procedure("apoc.load.jsonParams")
    @Description("Loads parameters from a JSON URL (e.g. web-API) as a stream of values if the given JSON file is an array.\n" +
            "If the given JSON file is a map, this procedure imports a single value instead.")
    public Stream<MapResult> jsonParams(@Name("urlOrKeyOrBinary") Object urlOrKeyOrBinary, @Name("headers") Map<String,Object> headers, @Name("payload") String payload, @Name(value = "path",defaultValue = "") String path, @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        boolean failOnError = (boolean) config.getOrDefault("failOnError", true);
        String compressionAlgo = (String) config.getOrDefault(COMPRESSION, CompressionAlgo.NONE.name());
        List<String> pathOptions = (List<String>) config.get("pathOptions");
        return loadJsonStream(urlOrKeyOrBinary, headers, payload, path, failOnError, compressionAlgo, pathOptions);
    }

}
