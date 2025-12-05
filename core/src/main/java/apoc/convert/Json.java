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
package apoc.convert;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

import apoc.meta.Types;
import apoc.util.JsonUtil;
import apoc.util.Util;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.*;

public class Json {

    // visible for testing
    public static String NODE = "node";
    public static String RELATIONSHIP = "relationship";

    public static Object writeJsonResult(Object value) {
        Types type = Types.of(value);
        switch (type) {
            case NODE:
                return nodeToMap((Node) value);
            case RELATIONSHIP:
                return relToMap((Relationship) value);
            case PATH:
                return writeJsonResult(StreamSupport.stream(((Path) value).spliterator(), false)
                        .map(i -> i instanceof Node ? nodeToMap((Node) i) : relToMap((Relationship) i))
                        .collect(Collectors.toList()));
            case LIST:
                return ConvertUtils.convertToList(value).stream()
                        .map(Json::writeJsonResult)
                        .collect(Collectors.toList());
            case MAP:
                return ((Map<String, Object>) value)
                        .entrySet().stream()
                                .collect(
                                        HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                                        (mapAccumulator, entry) ->
                                                mapAccumulator.put(entry.getKey(), writeJsonResult(entry.getValue())),
                                        HashMap::putAll);
            default:
                return value;
        }
    }

    private static Map<String, Object> relToMap(Relationship rel) {
        Map<String, Object> mapRel = map(
                "id", String.valueOf(rel.getId()),
                "type", RELATIONSHIP,
                "label", rel.getType().toString(),
                "start", nodeToMap(rel.getStartNode()),
                "end", nodeToMap(rel.getEndNode()));

        return mapWithOptionalProps(mapRel, rel.getAllProperties());
    }

    private static Map<String, Object> nodeToMap(Node node) {
        Map<String, Object> mapNode = map("id", String.valueOf(node.getId()));

        mapNode.put("type", NODE);

        if (node.getLabels().iterator().hasNext()) {
            mapNode.put("labels", labelStrings(node));
        }
        return mapWithOptionalProps(mapNode, node.getAllProperties());
    }

    private static Map<String, Object> mapWithOptionalProps(Map<String, Object> mapEntity, Map<String, Object> props) {
        if (!props.isEmpty()) {
            mapEntity.put("properties", props);
        }
        return mapEntity;
    }

    @UserFunction("apoc.json.path")
    @Description("Returns the given JSON path.")
    public Object path(
            @Name(value = "json", description = "A JSON string.") String json,
            @Name(value = "path", defaultValue = "$", description = "The path to extract from the JSON string.")
                    String path,
            @Name(
                            value = "pathOptions",
                            defaultValue = "null",
                            description =
                                    "A list of JSON path option enum values: ALWAYS_RETURN_LIST, AS_PATH_LIST, DEFAULT_PATH_LEAF_TO_NULL, REQUIRE_PROPERTIES, SUPPRESS_EXCEPTIONS.")
                    List<String> pathOptions) {
        return JsonUtil.parse(json, path, Object.class, pathOptions);
    }

    @UserFunction("apoc.convert.toJson")
    @Description("Serializes the given JSON value.")
    public String toJson(@Name(value = "value", description = "The value to serialize.") Object value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(writeJsonResult(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure(name = "apoc.convert.setJsonProperty", mode = Mode.WRITE)
    @Description("Serializes the given JSON object and sets it as a property on the given `NODE`.")
    public void setJsonProperty(
            @Name(value = "node", description = "The node to set the JSON property on.") Node node,
            @Name(value = "key", description = "The name of the property to set.") String key,
            @Name(value = "value", description = "The property to serialize as a JSON object.") Object value) {
        try {
            node.setProperty(key, JsonUtil.OBJECT_MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @UserFunction("apoc.convert.getJsonProperty")
    @Description(
            "Converts a serialized JSON object from the property of the given `NODE` into the equivalent Cypher structure (e.g. `MAP`, `LIST<ANY>`).")
    public Object getJsonProperty(
            @Name(value = "node", description = "The node containing a JSON string property.") Node node,
            @Name(value = "key", description = "The property key to convert.") String key,
            @Name(
                            value = "path",
                            defaultValue = "",
                            description =
                                    "A JSON path expression used to extract a certain part from the node property string.")
                    String path,
            @Name(
                            value = "pathOptions",
                            defaultValue = "null",
                            description =
                                    "JSON path options: ('ALWAYS_RETURN_LIST', 'AS_PATH_LIST', 'DEFAULT_PATH_LEAF_TO_NULL', 'REQUIRE_PROPERTIES', 'SUPPRESS_EXCEPTIONS')")
                    List<String> pathOptions) {
        String value = (String) node.getProperty(key, null);
        return JsonUtil.parse(value, path, Object.class, pathOptions);
    }

    @UserFunction("apoc.convert.getJsonPropertyMap")
    @Description("Converts a serialized JSON object from the property of the given `NODE` into a Cypher `MAP`.")
    public Map<String, Object> getJsonPropertyMap(
            @Name(value = "node", description = "The node containing a JSON stringified map.") Node node,
            @Name(value = "key", description = "The property key to convert.") String key,
            @Name(
                            value = "path",
                            defaultValue = "",
                            description =
                                    "A JSON path expression used to extract a certain part from the node property string.")
                    String path,
            @Name(
                            value = "pathOptions",
                            defaultValue = "null",
                            description =
                                    "JSON path options: ('ALWAYS_RETURN_LIST', 'AS_PATH_LIST', 'DEFAULT_PATH_LEAF_TO_NULL', 'REQUIRE_PROPERTIES', 'SUPPRESS_EXCEPTIONS')")
                    List<String> pathOptions) {
        String value = (String) node.getProperty(key, null);
        return JsonUtil.parse(value, path, Map.class, pathOptions);
    }

    @UserFunction("apoc.convert.fromJsonMap")
    @Description("Converts the given JSON map into a Cypher `MAP`.")
    public Map<String, Object> fromJsonMap(
            @Name(value = "map", description = "A JSON stringified map.") String value,
            @Name(
                            value = "path",
                            defaultValue = "",
                            description = "A JSON path expression used to extract a certain part from the map.")
                    String path,
            @Name(
                            value = "pathOptions",
                            defaultValue = "null",
                            description =
                                    "JSON path options: ('ALWAYS_RETURN_LIST', 'AS_PATH_LIST', 'DEFAULT_PATH_LEAF_TO_NULL', 'REQUIRE_PROPERTIES', 'SUPPRESS_EXCEPTIONS')")
                    List<String> pathOptions) {
        return JsonUtil.parse(value, path, Map.class, pathOptions);
    }

    @UserFunction("apoc.convert.fromJsonList")
    @Description("Converts the given JSON list into a Cypher `LIST<STRING>`.")
    public List<Object> fromJsonList(
            @Name(value = "list", description = "A JSON stringified list.") String value,
            @Name(
                            value = "path",
                            defaultValue = "",
                            description = "A JSON path expression used to extract a certain part from the list.")
                    String path,
            @Name(
                            value = "pathOptions",
                            defaultValue = "null",
                            description =
                                    "JSON path options: ('ALWAYS_RETURN_LIST', 'AS_PATH_LIST', 'DEFAULT_PATH_LEAF_TO_NULL', 'REQUIRE_PROPERTIES', 'SUPPRESS_EXCEPTIONS')")
                    List<String> pathOptions) {
        return JsonUtil.parse(value, path, List.class, pathOptions);
    }

    public record ToTreeMapResult(
            @Description("The resulting tree.") Map<String, Object> value) {
        public static final apoc.result.MapResult EMPTY = new apoc.result.MapResult(Collections.emptyMap());

        public static apoc.result.MapResult empty() {
            return EMPTY;
        }
    }

    @Procedure(value = "apoc.convert.toTree", deprecatedBy = "apoc.paths.toJsonTree")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Returns a stream of `MAP` values, representing the given `PATH` values as a tree with at least one root.")
    public Stream<ToTreeMapResult> toTree(
            @Name(value = "paths", description = "A list of paths to convert into a tree.") List<Path> paths,
            @Name(
                            value = "lowerCaseRels",
                            defaultValue = "true",
                            description = "Whether or not to convert relationship types to lower case.")
                    boolean lowerCaseRels,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description = "{ nodes = {} :: MAP, rels = {} :: MAP, sortPaths = true :: BOOLEAN }")
                    Map<String, Object> config) {
        if (paths == null || paths.isEmpty()) return Stream.of(new ToTreeMapResult(Collections.emptyMap()));
        ConvertConfig conf = new ConvertConfig(config);
        Map<String, List<String>> nodes = conf.getNodes();
        Map<String, List<String>> rels = conf.getRels();

        Map<Long, Map<String, Object>> maps = new HashMap<>(paths.size() * 100);

        Stream<Path> stream = paths.stream();
        if (conf.isSortPaths()) {
            stream = stream.sorted(Comparator.comparingInt(Path::length).reversed());
        }
        stream.forEach(path -> {
            Iterator<Entity> it = path.iterator();
            while (it.hasNext()) {
                Node n = (Node) it.next();
                Map<String, Object> nMap = maps.computeIfAbsent(n.getId(), (id) -> toMap(n, nodes));
                if (it.hasNext()) {
                    Relationship r = (Relationship) it.next();
                    Node m = r.getOtherNode(n);
                    String typeName = lowerCaseRels
                            ? r.getType().name().toLowerCase()
                            : r.getType().name();
                    // todo take direction into account and create collection into outgoing direction ??
                    // parent-[:HAS_CHILD]->(child) vs. (parent)<-[:PARENT_OF]-(child)
                    if (!nMap.containsKey(typeName)) nMap.put(typeName, new ArrayList<>(16));
                    List<Map<String, Object>> list = (List) nMap.get(typeName);
                    // Check that this combination of rel and node doesn't already exist
                    Optional<Map<String, Object>> optMap = list.stream()
                            .filter(elem -> elem.get("_elementId").equals(m.getElementId())
                                    && elem.get(typeName + "._elementId").equals(r.getElementId()))
                            .findFirst();
                    if (!optMap.isPresent()) {
                        Map<String, Object> mMap = toMap(m, nodes);
                        mMap = addRelProperties(mMap, typeName, r, rels);
                        maps.put(m.getId(), mMap);
                        list.add(maps.get(m.getId()));
                    }
                }
            }
        });

        return paths.stream()
                .map(Path::startNode)
                .distinct()
                .map(n -> maps.remove(n.getId()))
                .map(m -> m == null ? Collections.<String, Object>emptyMap() : m)
                .map(ToTreeMapResult::new);
    }

    @Procedure("apoc.paths.toJsonTree")
    @Description(
            "Creates a stream of nested documents representing the graph as a tree by traversing outgoing relationships.")
    public Stream<ToTreeMapResult> pathsToTree(
            @Name(value = "paths", description = "A list of paths to convert into a tree.") List<Path> paths,
            @Name(
                            value = "lowerCaseRels",
                            defaultValue = "true",
                            description = "Whether or not to convert relationship types to lower case.")
                    boolean lowerCaseRels,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description = "{ nodes = {} :: MAP, rels = {} :: MAP, sortPaths = true :: BOOLEAN }")
                    Map<String, Object> config) {
        if (paths == null || paths.isEmpty()) return Stream.of(new ToTreeMapResult(Collections.emptyMap()));
        ConvertConfig conf = new ConvertConfig(config);
        Map<String, List<String>> nodes = conf.getNodes();
        Map<String, List<String>> rels = conf.getRels();
        Set<Long> visitedInOtherPaths = new HashSet<>();
        Set<Long> nodesToKeepInResult = new HashSet<>();
        Map<Long, Map<String, Object>> tree = new HashMap<>();

        Stream<Path> allPaths = paths.stream();
        if (conf.isSortPaths()) {
            allPaths = allPaths.sorted(Comparator.comparingInt(Path::length).reversed());
        }
        allPaths.forEach(path -> {
            // This api will always return relationships in an outgoing fashion ()-[r]->()
            var pathRelationships = path.relationships();
            // If no relationships exist in the path, then add the node by itself
            if (!pathRelationships.iterator().hasNext()) {
                Node currentNode = path.startNode();
                Long currentNodeId = currentNode.getId();

                if (!visitedInOtherPaths.contains(currentNodeId)) {
                    nodesToKeepInResult.add(currentNodeId);
                }
                tree.computeIfAbsent(currentNode.getId(), (id) -> toMap(currentNode, nodes));
            }
            pathRelationships.iterator().forEachRemaining((currentRel) -> {
                Node currentNode = currentRel.getStartNode();
                Long currentNodeId = currentNode.getId();

                if (!visitedInOtherPaths.contains(currentNodeId)) {
                    nodesToKeepInResult.add(currentNodeId);
                }

                Node nextNode = currentRel.getEndNode();
                Map<String, Object> nodeMap =
                        tree.computeIfAbsent(currentNode.getId(), (id) -> toMap(currentNode, nodes));

                Long nextNodeId = nextNode.getId();
                String typeName = lowerCaseRels
                        ? currentRel.getType().name().toLowerCase()
                        : currentRel.getType().name();
                // todo take direction into account and create collection into outgoing direction ??
                // parent-[:HAS_CHILD]->(child) vs. (parent)<-[:PARENT_OF]-(child)
                if (!nodeMap.containsKey(typeName)) nodeMap.put(typeName, new ArrayList<>());
                // Check that this combination of rel and node doesn't already exist
                List<Map<String, Object>> currentNodeRels = (List) nodeMap.get(typeName);
                boolean alreadyProcessedRel = currentNodeRels.stream()
                        .anyMatch(elem -> elem.get("_id").equals(nextNodeId)
                                && elem.get(typeName + "._id").equals(currentRel.getId()));
                if (!alreadyProcessedRel) {
                    boolean nodeAlreadyVisited = tree.containsKey(nextNodeId);
                    Map<String, Object> nextNodeMap = toMap(nextNode, nodes);
                    addRelProperties(nextNodeMap, typeName, currentRel, rels);

                    if (!nodeAlreadyVisited) {
                        tree.put(nextNodeId, nextNodeMap);
                    }

                    visitedInOtherPaths.add(nextNodeId);
                    currentNodeRels.add(nextNodeMap);
                }
            });
        });

        var result =
                nodesToKeepInResult.stream().map(nodeId -> tree.get(nodeId)).map(ToTreeMapResult::new);
        return result;
    }

    @UserFunction("apoc.convert.toSortedJsonMap")
    @Description("Converts a serialized JSON object from the property of a given `NODE` into a Cypher `MAP`.")
    public String toSortedJsonMap(
            @Name(value = "value", description = "The value to convert into a stringified JSON map.") Object value,
            @Name(
                            value = "ignoreCase",
                            defaultValue = "true",
                            description = "Whether or not to ignore the case of the keys when sorting.")
                    boolean ignoreCase) {
        Map<String, Object> inputMap;
        Map<String, Object> sortedMap;

        if (value instanceof Node) {
            inputMap = ((Node) value).getAllProperties();
        } else if (value instanceof Map) {
            inputMap = (Map<String, Object>) value;
        } else {
            throw new IllegalArgumentException("input value must be a Node or a map");
        }

        if (ignoreCase) {
            sortedMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            sortedMap.putAll(inputMap);
        } else {
            sortedMap = new TreeMap<>(inputMap);
        }

        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(sortedMap);
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    private Map<String, Object> addRelProperties(
            Map<String, Object> mMap, String typeName, Relationship r, Map<String, List<String>> relFilters) {
        String prefix = typeName + ".";
        mMap.put(prefix + "_elementId", r.getElementId());
        mMap.put(prefix + "_id", r.getId());
        Map<String, Object> rProps = r.getAllProperties();
        if (rProps.isEmpty()) return mMap;
        if (relFilters.containsKey(typeName)) {
            rProps = filterProperties(rProps, relFilters.get(typeName));
        }
        rProps.forEach((k, v) -> mMap.put(prefix + k, v));
        return mMap;
    }

    private Map<String, Object> toMap(Node n, Map<String, List<String>> nodeFilters) {
        Map<String, Object> props = n.getAllProperties();
        Map<String, Object> result = new LinkedHashMap<>(props.size() + 2);
        String type = Util.labelString(n);
        result.put("_id", n.getId());
        result.put("_elementId", n.getElementId());
        result.put("_type", type);
        var types = type.split(":");
        var filter =
                Arrays.stream(types).filter((t) -> nodeFilters.containsKey(t)).findFirst();
        if (filter.isPresent()) { // Check if list contains LABEL
            props = filterProperties(props, nodeFilters.get(filter.get()));
        }
        result.putAll(props);
        return result;
    }

    private Map<String, Object> filterProperties(Map<String, Object> props, List<String> filters) {
        boolean isExclude = filters.get(0).startsWith("-");

        return props.entrySet().stream()
                .filter(e -> isExclude ? !filters.contains("-" + e.getKey()) : filters.contains(e.getKey()))
                .collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue()));
    }
}
