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
package apoc.map;

import static java.util.regex.Pattern.quote;

import apoc.util.Util;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class Maps {

    @Context
    public Transaction tx;

    @UserFunction("apoc.map.groupBy")
    @Description("Creates a `MAP` of the `LIST<ANY>` keyed by the given property, with single values.")
    public Map<String, Object> groupBy(
            @Name(value = "values", description = "A list of map values to be grouped.") List<Object> values,
            @Name(value = "key", description = "The key to group the map values by.") String key) {
        Map<String, Object> result = new LinkedHashMap<>(values.size());
        for (Object value : values) {
            Object id = getKey(key, value);
            if (id != null) result.put(id.toString(), value);
        }
        return result;
    }

    @UserFunction("apoc.map.groupByMulti")
    @Description("Creates a `MAP` of the `LIST<ANY>` values keyed by the given property, with the `LIST<ANY>` values.")
    public Map<String, List<Object>> groupByMulti(
            @Name(value = "values", description = "A list of map values to be grouped.") List<Object> values,
            @Name(value = "key", description = "The key to group the map values by.") String key) {
        Map<String, List<Object>> result = new LinkedHashMap<>(values.size());
        for (Object value : values) {
            Object id = getKey(key, value);
            if (id != null)
                result.compute(id.toString(), (k, list) -> {
                    if (list == null) list = new ArrayList<>();
                    list.add(value);
                    return list;
                });
        }
        return result;
    }

    public Object getKey(@Name("key") String key, Object value) {
        Object id = null;
        if (value instanceof Map) {
            id = ((Map) value).get(key);
        }
        if (value instanceof Entity) {
            id = ((Entity) value).getProperty(key, null);
        }
        return id;
    }

    @UserFunction("apoc.map.fromNodes")
    @Description("Returns a `MAP` of the given prop to the node of the given label.")
    public Map<String, Node> fromNodes(
            @Name(value = "label", description = "The node labels from which the map will be created.") String label,
            @Name(value = "prop", description = "The property name to map the returned nodes by.") String property) {
        Map<String, Node> result = new LinkedHashMap<>(10000);
        try (ResourceIterator<Node> nodes = tx.findNodes(Label.label(label))) {
            while (nodes.hasNext()) {
                Node node = nodes.next();
                Object key = node.getProperty(property, null);
                if (key != null) {
                    result.put(key.toString(), node);
                }
            }
        }
        return result;
    }

    @UserFunction("apoc.map.fromPairs")
    @Description("Creates a `MAP` from the given `LIST<LIST<ANY>>` of key-value pairs.")
    public Map<String, Object> fromPairs(
            @Name(value = "pairs", description = "A list of pairs to create a map from.") List<List<Object>> pairs) {
        return Util.mapFromPairs(pairs);
    }

    @UserFunction("apoc.map.fromLists")
    @Description("Creates a `MAP` from the keys and values in the given `LIST<ANY>` values.")
    public Map<String, Object> fromLists(
            @Name(value = "keys", description = "A list of keys to create a map from.") List<String> keys,
            @Name(value = "values", description = "A list of values associated with the keys to create a map from.")
                    List<Object> values) {
        return Util.mapFromLists(keys, values);
    }

    @UserFunction("apoc.map.values")
    @Description("Returns a `LIST<ANY>` indicated by the given keys (returns a null value if a given key is missing).")
    public List<Object> values(
            @Name(value = "map", description = "A map to extract values from.") Map<String, Object> map,
            @Name(value = "keys", defaultValue = "[]", description = "A list of keys to extract from the given map.")
                    List<String> keys,
            @Name(
                            value = "addNullsForMissing",
                            defaultValue = "false",
                            description = "Whether or not to return missing values as null values.")
                    boolean addNullsForMissing) {
        if (keys == null || keys.isEmpty()) return Collections.emptyList();
        List<Object> values = new ArrayList<>(keys.size());
        for (String key : keys) {
            if (addNullsForMissing || map.containsKey(key)) values.add(map.get(key));
        }
        return values;
    }

    @UserFunction("apoc.map.fromValues")
    @Description("Creates a `MAP` from the alternating keys and values in the given `LIST<ANY>`.")
    public Map<String, Object> fromValues(
            @Name(value = "values", description = "A list of keys and values listed pairwise to create a map from.")
                    List<Object> values) {
        return Util.map(values);
    }

    @UserFunction("apoc.map.merge")
    @Description("Merges the two given `MAP` values into one `MAP`.")
    public Map<String, Object> merge(
            @Name(value = "map1", description = "The first map to merge with the second map.")
                    Map<String, Object> first,
            @Name(value = "map2", description = "The second map to merge with the first map.")
                    Map<String, Object> second) {
        return Util.merge(first, second);
    }

    @UserFunction("apoc.map.mergeList")
    @Description("Merges all `MAP` values in the given `LIST<MAP<STRING, ANY>>` into one `MAP`.")
    public Map<String, Object> mergeList(
            @Name(value = "maps", description = "A list of maps to merge.") List<Map<String, Object>> maps) {
        Map<String, Object> result = new LinkedHashMap<>(maps.size());
        for (Map<String, Object> map : maps) {
            result.putAll(map);
        }
        return result;
    }

    @UserFunction("apoc.map.get")
    @Description("Returns a value for the given key.\n"
            + "If the given key does not exist, or lacks a default value, this function will throw an exception.")
    public Object get(
            @Name(value = "map", description = "The map to extract a value from.") Map<String, Object> map,
            @Name(value = "key", description = "The key to extract.") String key,
            @Name(value = "value", defaultValue = "null", description = "The default value of the given key.")
                    Object value,
            @Name(
                            value = "fail",
                            defaultValue = "true",
                            description =
                                    "If a key is not present and no default is provided, it will either throw an exception if true, or return a null value")
                    boolean fail) {
        if (fail && value == null && !map.containsKey(key))
            throw new IllegalArgumentException("Key " + key + " is not of one of the existing keys " + map.keySet());
        return map.getOrDefault(key, value);
    }

    @UserFunction("apoc.map.mget")
    @Description("Returns a `LIST<ANY>` for the given keys.\n"
            + "If one of the keys does not exist, or lacks a default value, this function will throw an exception.")
    public List<Object> mget(
            @Name(value = "map", description = "The map to extract a list of values from.") Map<String, Object> map,
            @Name(value = "keys", description = "The list of keys to extract.") List<String> keys,
            @Name(value = "values", defaultValue = "[]", description = "The default values of the given keys.")
                    List<Object> values,
            @Name(
                            value = "fail",
                            defaultValue = "true",
                            description =
                                    "If a key is not present and no default is provided, it will either throw an exception if true, or return a null value")
                    boolean fail) {
        if (keys == null || map == null) return null;
        int keySize = keys.size();
        List<Object> result = new ArrayList<>(keySize);
        int valuesSize = values == null ? -1 : values.size();
        for (int i = 0; i < keySize; i++) {
            result.add(get(map, keys.get(i), i < valuesSize ? values.get(i) : null, fail));
        }
        return result;
    }

    @UserFunction("apoc.map.submap")
    @Description("Returns a sub-map for the given keys.\n"
            + "If one of the keys does not exist, or lacks a default value, this function will throw an exception.")
    public Map<String, Object> submap(
            @Name(value = "map", description = "The map to extract a submap from.") Map<String, Object> map,
            @Name(value = "keys", description = "The list of keys to extract into a submap.") List<String> keys,
            @Name(value = "values", defaultValue = "[]", description = "The default values of the given keys.")
                    List<Object> values,
            @Name(
                            value = "fail",
                            defaultValue = "true",
                            description =
                                    "If a key is not present and no default is provided, it will either throw an exception if true, or return a null value.")
                    boolean fail) {
        if (keys == null || map == null) return null;
        int keySize = keys.size();
        Map<String, Object> result = new LinkedHashMap<>(keySize);
        int valuesSize = values == null ? -1 : values.size();
        for (int i = 0; i < keySize; i++) {
            String key = keys.get(i);
            result.put(key, get(map, key, i < valuesSize ? values.get(i) : null, fail));
        }
        return result;
    }

    @UserFunction("apoc.map.setKey")
    @Description("Adds or updates the given entry in the `MAP`.")
    public Map<String, Object> setKey(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "key", description = "The key to add or update the map with.") String key,
            @Name(value = "value", description = "The value to set the given key to.") Object value) {
        return Util.merge(map, Util.map(key, value));
    }

    @UserFunction("apoc.map.setEntry")
    @Description("Adds or updates the given entry in the `MAP`.")
    public Map<String, Object> setEntry(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "key", description = "The key to add or update the map with.") String key,
            @Name(value = "value", description = "The value to set the given key to.") Object value) {
        return Util.merge(map, Util.map(key, value));
    }

    @UserFunction("apoc.map.setPairs")
    @Description("Adds or updates the given key/value pairs (e.g. [key1,value1],[key2,value2]) in a `MAP`.")
    public Map<String, Object> setPairs(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "pairs", description = "A list of pairs to add or update the map with.")
                    List<List<Object>> pairs) {
        return Util.merge(map, Util.mapFromPairs(pairs));
    }

    @UserFunction("apoc.map.setLists")
    @Description(
            "Adds or updates the given keys/value pairs provided in `LIST<ANY>` format (e.g. [key1, key2],[value1, value2]) in a `MAP`.")
    public Map<String, Object> setLists(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "keys", description = "A list of keys to add or update the map with.") List<String> keys,
            @Name(
                            value = "values",
                            description = "A list of values associated to the keys to add or update the map with.")
                    List<Object> values) {
        return Util.merge(map, Util.mapFromLists(keys, values));
    }

    @UserFunction("apoc.map.setValues")
    @Description("Adds or updates the alternating key/value pairs (e.g. [key1,value1,key2,value2]) in a `MAP`.")
    public Map<String, Object> setValues(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "pairs", description = "A list of items listed pairwise to add or update the map with.")
                    List<Object> pairs) {
        return Util.merge(map, Util.map(pairs));
    }

    @UserFunction("apoc.map.removeKey")
    @Description("Removes the given key from the `MAP` (recursively if recursive is true).")
    public Map<String, Object> removeKey(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "key", description = "The key to remove from the map.") String key,
            @Name(value = "config", defaultValue = "{}", description = "{ recursive = false :: BOOLEAN }")
                    Map<String, Object> config) {
        if (!map.containsKey(key)) {
            return map;
        }

        return removeKeys(map, Collections.singletonList(key), config);
    }

    @UserFunction("apoc.map.removeKeys")
    @Description("Removes the given keys from the `MAP` (recursively if recursive is true).")
    public Map<String, Object> removeKeys(
            @Name(value = "map", description = "The map to be updated.") Map<String, Object> map,
            @Name(value = "keys", description = "The keys to remove from the map.") List<String> keys,
            @Name(value = "config", defaultValue = "{}", description = "{ recursive = false :: BOOLEAN }")
                    Map<String, Object> config) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.keySet().removeAll(keys);
        Map<String, Object> checkedConfig = config == null ? Collections.emptyMap() : config;
        boolean removeRecursively = Util.toBoolean(checkedConfig.getOrDefault("recursive", false));
        if (removeRecursively) {
            for (Iterator<Map.Entry<String, Object>> iterator = res.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                if (entry.getValue() instanceof Map) {
                    Map<String, Object> updatedMap =
                            removeKeys((Map<String, Object>) entry.getValue(), keys, checkedConfig);
                    if (updatedMap.isEmpty()) {
                        iterator.remove();
                    } else if (!updatedMap.equals(entry.getValue())) {
                        entry.setValue(updatedMap);
                    }
                } else if (entry.getValue() instanceof Collection) {
                    Collection<Object> values = (Collection<Object>) entry.getValue();
                    List<Object> updatedValues = values.stream()
                            .map(value -> value instanceof Map
                                    ? removeKeys((Map<String, Object>) value, keys, checkedConfig)
                                    : value)
                            .filter(value -> value instanceof Map ? !((Map<String, Object>) value).isEmpty() : true)
                            .collect(Collectors.toList());
                    if (updatedValues.isEmpty()) {
                        iterator.remove();
                    } else {
                        entry.setValue(updatedValues);
                    }
                }
            }
        }
        return res;
    }

    @UserFunction("apoc.map.clean")
    @Description("Filters the keys and values contained in the given `LIST<ANY>` values.")
    public Map<String, Object> clean(
            @Name(value = "map", description = "The map to clean.") Map<String, Object> map,
            @Name(value = "keys", description = "The list of property keys to be removed.") List<String> keys,
            @Name(value = "values", description = "The list of values to be removed.") List<Object> values) {
        HashSet<String> keySet = new HashSet<>(keys);
        HashSet<Object> valueSet = new HashSet<>(values);

        LinkedHashMap<String, Object> res = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (keySet.contains(entry.getKey())
                    || value == null
                    || valueSet.contains(value)
                    || valueSet.contains(value.toString())) continue;
            res.put(entry.getKey(), value);
        }
        return res;
    }

    @UserFunction("apoc.map.updateTree")
    @Description("Adds the data `MAP` on each level of the nested tree, where the key-value pairs match.")
    public Map<String, Object> updateTree(
            @Name(value = "tree", description = "The map to be updated.") Map<String, Object> tree,
            @Name(value = "key", description = "The name of the key to match on.") String key,
            @Name(
                            value = "data",
                            description =
                                    "A list of pairs, where the first item is the value to match with the given key, and the second is a map to add to the tree.")
                    List<List<Object>> data) {
        Map<Object, Map<String, Object>> map = new HashMap<>(data.size());
        for (List<Object> datum : data) {
            if (datum.size() < 2 || !((datum.get(1) instanceof Map)))
                throw new IllegalArgumentException("Wrong data list entry: " + datum);
            map.put(datum.get(0), (Map) datum.get(1));
        }
        return visit(tree, (m) -> {
            Map<String, Object> entry = map.get(m.get(key));
            if (entry != null) {
                m.putAll(entry);
            }
            return m;
        });
    }

    Map<String, Object> visit(Map<String, Object> tree, Function<Map<String, Object>, Map<String, Object>> mapper) {
        Map<String, Object> result = mapper.apply(new LinkedHashMap<>(tree));

        result.entrySet().forEach(e -> {
            if (e.getValue() instanceof List) {
                List<Object> list = (List<Object>) e.getValue();
                List newList = list.stream()
                        .map(v -> {
                            if (v instanceof Map) {
                                Map<String, Object> map = (Map<String, Object>) v;
                                return visit(map, mapper);
                            }
                            return v;
                        })
                        .collect(Collectors.toList());
                e.setValue(newList);
            } else if (e.getValue() instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) e.getValue();
                e.setValue(visit(map, mapper));
            }
        });
        return result;
    }

    @UserFunction("apoc.map.flatten")
    @Description("Flattens nested items in the given `MAP`.\n"
            + "This function is the reverse of the `apoc.map.unflatten` function.")
    public Map<String, Object> flatten(
            @Name(value = "map", description = "A nested map to flatten.") Map<String, Object> map,
            @Name(
                            value = "delimiter",
                            defaultValue = ".",
                            description = "The delimiter used to separate the levels of the flattened map.")
                    String delimiter) {
        Map<String, Object> flattenedMap = new HashMap<>();
        flattenMapRecursively(flattenedMap, map, "", delimiter == null ? "." : delimiter);
        return flattenedMap;
    }

    @SuppressWarnings("unchecked")
    private void flattenMapRecursively(
            Map<String, Object> flattenedMap, Map<String, Object> map, String prefix, String delimiter) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map) {
                flattenMapRecursively(
                        flattenedMap,
                        (Map<String, Object>) entry.getValue(),
                        prefix + entry.getKey() + delimiter,
                        delimiter);
            } else {
                flattenedMap.put(prefix + entry.getKey(), entry.getValue());
            }
        }
    }

    @UserFunction("apoc.map.unflatten")
    @Description("Unflattens items in the given `MAP` to nested items.\n"
            + "This function is the reverse of the `apoc.map.flatten` function.")
    public Map<String, Object> unflatten(
            @Name(value = "map", description = "The map to unflatten.") Map<String, Object> map,
            @Name(
                            value = "delimiter",
                            defaultValue = ".",
                            description = "The delimiter used to separate the levels of the flattened map.")
                    String delimiter) {
        return unflattenMapRecursively(map, StringUtils.isBlank(delimiter) ? "." : delimiter);
    }

    private Map<String, Object> unflattenMapRecursively(Map<String, Object> inputMap, String delimiter) {
        Map<String, Object> resultMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : inputMap.entrySet()) {
            unflatEntry(resultMap, entry.getValue(), entry.getKey(), delimiter);
        }
        return resultMap;
    }

    public static void unflatEntry(Map<String, Object> map, Object value, String key, String delimiter) {
        final String[] keys = key.split(quote(delimiter), 2);
        final String firstPart = keys[0];

        if (keys.length == 1) {
            map.put(firstPart, value);
        } else {
            final Map<String, Object> currentMap =
                    (Map<String, Object>) map.computeIfAbsent(firstPart, k -> new HashMap<String, Object>());
            unflatEntry(currentMap, value, keys[1], delimiter);
        }
    }

    @UserFunction("apoc.map.sortedProperties")
    @Description("Returns a `LIST<ANY>` of key/value pairs.\n"
            + "The pairs are sorted by alphabetically by key, with optional case sensitivity.")
    public List<List<Object>> sortedProperties(
            @Name(value = "map", description = "The map to extract the properties from.") Map<String, Object> map,
            @Name(
                            value = "ignoreCase",
                            defaultValue = "true",
                            description = "Whether or not to take the case into account when sorting.")
                    boolean ignoreCase) {
        List<List<Object>> sortedProperties = new ArrayList<>();
        List<String> keys = new ArrayList<>(map.keySet());

        if (ignoreCase) {
            Collections.sort(keys, String.CASE_INSENSITIVE_ORDER);
        } else {
            Collections.sort(keys);
        }

        for (String key : keys) {
            sortedProperties.add(Arrays.asList(key, map.get(key)));
        }

        return sortedProperties;
    }
}
