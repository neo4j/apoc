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

import apoc.meta.Types;
import apoc.result.MapResult;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

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
                return writeJsonResult(StreamSupport.stream(((Path)value).spliterator(),false)
                        .map(i-> i instanceof Node ? nodeToMap((Node) i) : relToMap((Relationship) i))
                        .collect(Collectors.toList()));
            case LIST:
                return ConvertUtils.convertToList(value).stream().map(Json::writeJsonResult).collect(Collectors.toList());
            case MAP:
                return ((Map<String, Object>) value).entrySet()
                        .stream()
                        .collect(HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                                (mapAccumulator, entry) -> mapAccumulator.put(entry.getKey(), writeJsonResult(entry.getValue())), 
                                HashMap::putAll);  
            default:
                return value;
        }
    }

    private static Map<String,Object> relToMap(Relationship rel) {
        Map<String, Object> mapRel = map(
                "id", String.valueOf(rel.getId()),
                "type", RELATIONSHIP,
                "label", rel.getType().toString(),
                "start", nodeToMap(rel.getStartNode()),
                "end", nodeToMap(rel.getEndNode()));

        return mapWithOptionalProps(mapRel, rel.getAllProperties());
    }

    private static Map<String,Object> nodeToMap(Node node) {
        Map<String, Object> mapNode = map("id", String.valueOf(node.getId()));

        mapNode.put("type", NODE);

        if (node.getLabels().iterator().hasNext()) {
            mapNode.put("labels", labelStrings(node));
        }
        return mapWithOptionalProps(mapNode, node.getAllProperties());
    }

    private static Map<String, Object> mapWithOptionalProps(Map<String,Object> mapEntity, Map<String,Object> props) {
        if (!props.isEmpty()) {
            mapEntity.put("properties", props);
        }
        return mapEntity;
    }

    @UserFunction("apoc.json.path")
    @Description("Returns the given JSON path.")
    public Object path(@Name("json") String json, @Name(value = "path",defaultValue = "$") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        return JsonUtil.parse(json, path, Object.class, pathOptions);
    }
    @UserFunction("apoc.convert.toJson")
    @Description("Serializes the given JSON value.")
    public String toJson(@Name("value") Object value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(writeJsonResult(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @Procedure(name = "apoc.convert.setJsonProperty", mode = Mode.WRITE)
    @Description("Serializes the given JSON object and sets it as a property on the given `NODE`.")
    public void setJsonProperty(@Name("node") Node node, @Name("key") String key, @Name("value") Object value) {
        try {
            node.setProperty(key, JsonUtil.OBJECT_MAPPER.writeValueAsString(value));
        } catch (IOException e) {
            throw new RuntimeException("Can't convert " + value + " to json", e);
        }
    }

    @UserFunction("apoc.convert.getJsonProperty")
    @Description("Converts a serialized JSON object from the property of the given `NODE` into the equivalent Cypher structure (e.g. `MAP`, `LIST<ANY>`).")
    public Object getJsonProperty(@Name("node") Node node, @Name("key") String key,@Name(value = "path",defaultValue = "") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        String value = (String) node.getProperty(key, null);
        return JsonUtil.parse(value, path, Object.class, pathOptions);
    }

    @UserFunction("apoc.convert.getJsonPropertyMap")
    @Description("Converts a serialized JSON object from the property of the given `NODE` into a Cypher `MAP`.")
    public Map<String,Object> getJsonPropertyMap(@Name("node") Node node, @Name("key") String key,@Name(value = "path",defaultValue = "") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        String value = (String) node.getProperty(key, null);
        return JsonUtil.parse(value, path, Map.class, pathOptions);
    }

    @UserFunction("apoc.convert.fromJsonMap")
    @Description("Converts the given JSON `MAP` into a Cypher `MAP`.")
    public Map<String,Object> fromJsonMap(@Name("map") String value,@Name(value = "path",defaultValue = "") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        return JsonUtil.parse(value, path, Map.class, pathOptions);
    }

    @UserFunction("apoc.convert.fromJsonList")
    @Description("Converts the given JSON `LIST<STRING>` into a Cypher `LIST<STRING>`.")
    public List<Object> fromJsonList(@Name("list") String value, @Name(value = "path",defaultValue = "") String path, @Name(value = "pathOptions", defaultValue = "null") List<String> pathOptions) {
        return JsonUtil.parse(value, path, List.class, pathOptions);
    }

    @Procedure("apoc.convert.toTree")
    @Description("Returns a stream of `MAP` values, representing the given `PATH` values as a tree with at least one root.")
    // todo optinally provide root node
    public Stream<MapResult> toTree(@Name("paths") List<Path> paths, @Name(value = "lowerCaseRels",defaultValue = "true") boolean lowerCaseRels, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (paths.isEmpty()) return Stream.of(new MapResult(Collections.emptyMap()));
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
                    String typeName = lowerCaseRels ? r.getType().name().toLowerCase() : r.getType().name();
                    // todo take direction into account and create collection into outgoing direction ??
                    // parent-[:HAS_CHILD]->(child) vs. (parent)<-[:PARENT_OF]-(child)
                    if (!nMap.containsKey(typeName)) nMap.put(typeName, new ArrayList<>(16));
                    List<Map<String, Object>> list = (List) nMap.get(typeName);
                    Optional<Map<String, Object>> optMap = list.stream()
                            .filter(elem -> elem.get("_id").equals(m.getId()))
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
                .map(m -> m == null ? Collections.<String,Object>emptyMap() : m)
                .map(MapResult::new);
    }

    @UserFunction("apoc.convert.toSortedJsonMap")
    @Description("Converts a serialized JSON object from the property of a given `NODE` into a Cypher `MAP`.")
    public String toSortedJsonMap(@Name("value") Object value, @Name(value="ignoreCase", defaultValue = "true") boolean ignoreCase) {
        Map<String, Object> inputMap;
        Map<String, Object> sortedMap;

        if (value instanceof Node) {
            inputMap = ((Node)value).getAllProperties();
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

    private Map<String, Object> addRelProperties(Map<String, Object> mMap, String typeName, Relationship r, Map<String, List<String>> relFilters) {
        Map<String, Object> rProps = r.getAllProperties();
        if (rProps.isEmpty()) return mMap;
        String prefix = typeName + ".";
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
        result.put("_type", type);
        if (nodeFilters.containsKey(type)){ //Check if list contains LABEL
            props = filterProperties(props, nodeFilters.get(type));
        }
        result.putAll(props);
        return result;
    }

    private Map<String, Object> filterProperties(Map<String, Object> props, List<String> filters) {
        boolean isExclude = filters.get(0).startsWith("-");

        return props.entrySet().stream().filter(e -> isExclude ? !filters.contains("-" + e.getKey()) : filters.contains(e.getKey())).collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue()));
    }

}
