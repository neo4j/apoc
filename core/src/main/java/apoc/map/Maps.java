package apoc.map;

import apoc.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.quote;

public class Maps {

    @Context
    public Transaction tx;

    @UserFunction("apoc.map.groupBy")
    @Description("Creates a map of the list keyed by the given property, with single values.")
    public Map<String,Object> groupBy(@Name("values") List<Object> values, @Name("key") String key) {
        Map<String,Object> result = new LinkedHashMap<>(values.size());
        for (Object value : values) {
            Object id = getKey(key, value);
            if (id != null) result.put(id.toString(), value);
        }
        return result;
    }
    @UserFunction("apoc.map.groupByMulti")
    @Description("Creates a map of the lists keyed by the given property, with the list values.")
    public Map<String,List<Object>> groupByMulti(@Name("values") List<Object> values, @Name("key") String key) {
        Map<String,List<Object>> result = new LinkedHashMap<>(values.size());
        for (Object value : values) {
            Object id = getKey(key, value);
            if (id != null) result.compute(id.toString(), (k,list) -> {
                if (list==null) list = new ArrayList<>();
                list.add(value);
                return list;
            });
        }
        return result;
    }

    public Object getKey(@Name("key") String key, Object value) {
        Object id = null;
        if (value instanceof Map) {
            id = ((Map)value).get(key);
        }
        if (value instanceof Entity) {
            id = ((Entity)value).getProperty(key,null);
        }
        return id;
    }

    @UserFunction("apoc.map.fromNodes")
    @Description("Creates a map from the labels and properties of the given nodes.")
    public Map<String, Node> fromNodes(@Name("label") String label, @Name("prop") String property) {
        Map<String, Node> result = new LinkedHashMap<>(10000);
        try (ResourceIterator<Node> nodes = tx.findNodes(Label.label(label))) {
            while (nodes.hasNext()) {
                Node node = nodes.next();
                Object key = node.getProperty(property, null);
                if (key!=null) {
                    result.put(key.toString(), node);
                }
            }
        }
        return result;
    }

    @UserFunction("apoc.map.fromPairs")
    @Description("Creates a map from the given list of key-value pairs.")
    public Map<String,Object> fromPairs(@Name("pairs") List<List<Object>> pairs) {
        return Util.mapFromPairs(pairs);
    }

    @UserFunction("apoc.map.fromLists")
    @Description("Creates a map from the keys and values in the given list.")
    public Map<String,Object> fromLists(@Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Util.mapFromLists(keys, values);
    }

    @UserFunction("apoc.map.values")
    @Description("Returns a list of values indicated by the given keys (returns a null value if a given key is missing).")
    public List<Object> values(@Name("map") Map<String,Object> map, @Name(value = "keys",defaultValue = "[]") List<String> keys, @Name(value = "addNullsForMissing",defaultValue = "false") boolean addNullsForMissing) {
        if (keys == null || keys.isEmpty()) return Collections.emptyList();
        List<Object> values = new ArrayList<>(keys.size());
        for (String key : keys) {
            if (addNullsForMissing || map.containsKey(key)) values.add(map.get(key));
        }
        return values;
    }

    @UserFunction("apoc.map.fromValues")
    @Description("Creates a map from the alternating keys and values in the given list.")
    public Map<String,Object> fromValues(@Name("values") List<Object> values) {
        return Util.map(values);
    }

    @UserFunction("apoc.map.merge")
    @Description("Merges the two given maps into one map.")
    public Map<String,Object> merge(@Name("map1") Map<String,Object> first, @Name("map2") Map<String,Object> second) {
        return Util.merge(first,second);
    }

    @UserFunction("apoc.map.mergeList")
    @Description("Merges all maps in the given list into one map.")
    public Map<String,Object> mergeList(@Name("maps") List<Map<String,Object>> maps) {
        Map<String,Object> result = new LinkedHashMap<>(maps.size());
        for (Map<String, Object> map : maps) {
            result.putAll(map);
        }
        return result;
    }

    @UserFunction("apoc.map.get")
    @Description("Returns a value for the given key.\n" +
            "If the given key does not exist, or lacks a default value, this function will throw an exception.")
    public Object get(@Name("map") Map<String,Object> map, @Name("key") String key, @Name(value = "value", defaultValue = "null") Object value, @Name(value = "fail",defaultValue = "true") boolean fail) {
        if (fail && value == null && !map.containsKey(key)) throw new IllegalArgumentException("Key "+key+" is not of one of the existing keys "+map.keySet());
        return map.getOrDefault(key, value);
    }

    @UserFunction("apoc.map.mget")
    @Description("Returns a list of values for the given keys.\n" +
            "If one of the keys does not exist, or lacks a default value, this function will throw an exception.")
    public List<Object> mget(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name(value = "values", defaultValue = "[]") List<Object> values, @Name(value = "fail",defaultValue = "true") boolean fail) {
        if (keys==null || map==null) return null;
        int keySize = keys.size();
        List<Object> result = new ArrayList<>(keySize);
        int valuesSize = values == null ? -1 : values.size();
        for (int i = 0; i < keySize; i++) {
            result.add(get(map, keys.get(i), i < valuesSize ? values.get(i) : null,fail));
        }
        return result;
    }

    @UserFunction("apoc.map.submap")
    @Description("Returns a sub-map for the given keys.\n" +
            "If one of the keys does not exist, or lacks a default value, this function will throw an exception.")
    public Map<String, Object> submap(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name(value = "values", defaultValue = "[]") List<Object> values, @Name(value = "fail",defaultValue = "true") boolean fail) {
        if (keys==null || map==null) return null;
        int keySize = keys.size();
        Map<String,Object> result = new LinkedHashMap<>(keySize);
        int valuesSize = values == null ? -1 : values.size();
        for (int i = 0; i < keySize; i++) {
            String key = keys.get(i);
            result.put(key, get(map, key, i < valuesSize ? values.get(i) : null,fail));
        }
        return result;
    }

    @UserFunction("apoc.map.setKey")
    @Description("Adds or updates the given entry in the map.")
    public Map<String,Object> setKey(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        return Util.merge(map, Util.map(key,value));
    }

    @UserFunction("apoc.map.setEntry")
    @Description("Adds or updates the given entry in the map.")
    public Map<String,Object> setEntry(@Name("map") Map<String,Object> map, @Name("key") String key, @Name("value") Object value) {
        return Util.merge(map, Util.map(key,value));
    }

    @UserFunction("apoc.map.setPairs")
    @Description("Adds or updates the given key/value pairs (e.g. [key1,value1],[key2,value2]) in a map.")
    public Map<String,Object> setPairs(@Name("map") Map<String,Object> map, @Name("pairs") List<List<Object>> pairs) {
        return Util.merge(map, Util.mapFromPairs(pairs));
    }

    @UserFunction("apoc.map.setLists")
    @Description("Adds or updates the given keys/value pairs provided in list format (e.g. [key1, key2],[value1, value2]) in a map.")
    public Map<String,Object> setLists(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return Util.merge(map, Util.mapFromLists(keys, values));
    }

    @UserFunction("apoc.map.setValues")
    @Description("apoc.map.setValues(map,[key1,value1,key2,value2])")
    public Map<String,Object> setValues(@Name("map") Map<String,Object> map, @Name("pairs") List<Object> pairs) {
        return Util.merge(map, Util.map(pairs));
    }

    @UserFunction("apoc.map.removeKey")
    @Description("Adds or updates the alternating key/value pairs (e.g. [key1,value1,key2,value2]) in a map.")
    public Map<String,Object> removeKey(@Name("map") Map<String,Object> map, @Name("key") String key,  @Name(value="config", defaultValue = "{}") Map<String, Object> config) {
        if (!map.containsKey(key)) {
            return map;
        }

        return removeKeys(map, Collections.singletonList(key), config);
    }

    @UserFunction("apoc.map.removeKeys")
    @Description("Removes the given keys from the map (recursively if recursive is true).")
    public Map<String, Object> removeKeys(@Name("map") Map<String, Object> map, @Name("keys") List<String> keys, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        Map<String, Object> res = new LinkedHashMap<>(map);
        res.keySet().removeAll(keys);
        Map<String, Object> checkedConfig = config == null ? Collections.emptyMap() : config;
        boolean removeRecursively = Util.toBoolean(checkedConfig.getOrDefault("recursive", false));
        if (removeRecursively) {
            for (Iterator<Map.Entry<String, Object>> iterator = res.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                if (entry.getValue() instanceof Map) {
                    Map<String, Object> updatedMap = removeKeys((Map<String, Object>) entry.getValue(), keys, checkedConfig);
                    if (updatedMap.isEmpty()) {
                        iterator.remove();
                    } else if (!updatedMap.equals(entry.getValue())) {
                        entry.setValue(updatedMap);
                    }
                } else if (entry.getValue() instanceof Collection) {
                    Collection<Object> values = (Collection<Object>) entry.getValue();
                    List<Object> updatedValues = values.stream()
                            .map(value -> value instanceof Map ? removeKeys((Map<String, Object>) value, keys, checkedConfig) : value)
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
    @Description("Filters the keys and values contained in the given lists.")
    public Map<String,Object> clean(@Name("map") Map<String,Object> map, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        HashSet<String> keySet = new HashSet<>(keys);
        HashSet<Object> valueSet = new HashSet<>(values);

        LinkedHashMap<String, Object> res = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();
            if (keySet.contains(entry.getKey()) || value == null || valueSet.contains(value) || valueSet.contains(value.toString())) continue;
            res.put(entry.getKey(),value);
        }
        return res;
    }

    @UserFunction("apoc.map.updateTree")
    @Description("Adds the data map on each level of the nested tree, where the key-value pairs match.")
    public Map<String,Object> updateTree(@Name("tree") Map<String, Object> tree, @Name("key") String key, @Name("data") List<List<Object>> data) {
        Map<Object,Map<String,Object>> map = new HashMap<>(data.size());
        for (List<Object> datum : data) {
            if (datum.size()<2 || !((datum.get(1) instanceof Map))) throw new IllegalArgumentException("Wrong data list entry: "+datum);
            map.put(datum.get(0), (Map)datum.get(1));
        }
        return visit(tree, (m) -> {
            Map<String, Object> entry = map.get(m.get(key));
            if (entry != null) {
                m.putAll(entry);
            }
            return m;
        });
    }

    Map<String,Object> visit(Map<String,Object> tree, Function<Map<String,Object>,Map<String,Object>> mapper) {
        Map<String, Object> result = mapper.apply(new LinkedHashMap<>(tree));

        result.entrySet().forEach(e -> {
            if (e.getValue() instanceof List) {
                List<Object> list = (List<Object>) e.getValue();
                List newList = list.stream().map(v -> {
                    if (v instanceof Map) {
                        Map<String, Object> map = (Map<String, Object>) v;
                        return visit(map, mapper);
                    }
                    return v;
                }).collect(Collectors.toList());
                e.setValue(newList);
            } else if (e.getValue() instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) e.getValue();
                e.setValue(visit(map,mapper));
            }
        });
        return result;
    }


    @UserFunction("apoc.map.flatten")
    @Description("Flattens nested items in the given map.\n" +
            "This function is the reverse of the `apoc.map.unflatten` function.")
    public Map<String,Object> flatten(@Name("map") Map<String, Object> map, @Name(value="delimiter", defaultValue = ".") String delimiter) {
        Map<String, Object> flattenedMap = new HashMap<>();
        flattenMapRecursively(flattenedMap, map, "", delimiter == null ? "." : delimiter);
        return flattenedMap;
    }

    @SuppressWarnings("unchecked")
    private void flattenMapRecursively(Map<String, Object> flattenedMap, Map<String, Object> map, String prefix, String delimiter) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
             if (entry.getValue() instanceof Map) {
                 flattenMapRecursively(flattenedMap, (Map<String, Object>) entry.getValue(), prefix + entry.getKey() + delimiter, delimiter);
             } else {
                 flattenedMap.put(prefix + entry.getKey(), entry.getValue());
             }
        }
    }

    @UserFunction("apoc.map.unflatten")
    @Description("Unflattens items in the given map to nested items.\n" +
            "This function is the reverse of the `apoc.map.flatten` function.")
    public Map<String, Object> unflatten(@Name("map") Map<String, Object> map, @Name(value = "delimiter", defaultValue = ".") String delimiter) {
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
            final Map<String, Object> currentMap = (Map<String, Object>) map.computeIfAbsent(firstPart, k -> new HashMap<String, Object>());
            unflatEntry(currentMap, value, keys[1], delimiter);
        }
    }

    @UserFunction("apoc.map.sortedProperties")
    @Description("Returns a list of key/value pairs in a list.\n" +
            "The pairs are sorted by alphabetically by key, with optional case sensitivity.")
    public List<List<Object>> sortedProperties(@Name("map") Map<String, Object> map, @Name(value="ignoreCase", defaultValue = "true") boolean ignoreCase) {
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
