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
package apoc.nodes;

import static java.util.Collections.*;

import apoc.Pools;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * @author mh
 * @since 14.06.17
 */
public class Grouping {

    private static final int BATCHSIZE = 10000;

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Context
    public Log log;

    @Context
    public Pools pools;

    public static class GroupResult {
        @Description("A list of grouped nodes represented as virtual nodes.")
        public List<Node> nodes;

        @Description("A list of grouped relationships represented as virtual relationships.")
        public List<Relationship> relationships;

        @Description("The grouping node.")
        public Node node;

        @Description("The grouping relationship.")
        public Relationship relationship;

        public GroupResult(Node node, Relationship relationship) {
            this.node = node;
            this.relationship = relationship;
            this.nodes = singletonList(node);
            this.relationships = singletonList(relationship);
        }

        public GroupResult(Node node, List<Relationship> relationships) {
            this.nodes = singletonList(node);
            this.relationships = relationships;
            this.node = node;
            this.relationship = relationships.isEmpty() ? null : relationships.get(0);
        }

        public Stream<GroupResult> spread() {
            return Stream.concat(Stream.of(this), relationships.stream().skip(1).map(r -> new GroupResult(node, r)));
        }
    }

    @NotThreadSafe
    @Procedure("apoc.nodes.group")
    @Description("Allows for the aggregation of `NODE` values based on the given properties.\n"
            + "This procedure returns virtual `NODE` values.")
    public Stream<GroupResult> group(
            @Name(
                            value = "labels",
                            description =
                                    "The list of node labels to aggregate over. Use `['*']` to indicate all node labels should be looked at.")
                    List<String> labelNames,
            @Name(value = "groupByProperties", description = "The property keys to group the nodes by.")
                    List<String> groupByProperties,
            @Name(
                            value = "aggregations",
                            defaultValue = "[{`*`:\"count\"},{`*`:\"count\"}]",
                            description =
                                    "The first map specifies the node properties to aggregate with their corresponding aggregation functions, while the second map specifies the relationship properties for aggregation.")
                    List<Map<String, Object>> aggregations,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        includeRels :: STRING | LIST<STRING>
                        excludeRels :: STRING | LIST<STRING>,
                        orphans = true :: BOOLEAN,
                        selfRels = true :: BOOLEAN,
                        limitNodes = -1 :: INTEGER,
                        limitRels = -1 :: INTEGER,
                        relsPerNode = -1 :: INTEGER,
                        filter :: MAP
                    }
                    """)
                    Map<String, Object> config) {

        Set<String> labels = new HashSet<>(labelNames);
        if (labels.remove("*"))
            labels.addAll(Iterables.stream(tx.getAllLabels()).map(Label::name).collect(Collectors.toSet()));

        String[] keys = groupByProperties.toArray(new String[groupByProperties.size()]);

        if (aggregations == null || aggregations.isEmpty()) {
            aggregations = Arrays.asList(singletonMap("*", "count"), singletonMap("*", "count"));
        }
        Map<String, List<String>> nodeAggNames =
                (aggregations.size() > 0) ? toStringListMap(aggregations.get(0)) : emptyMap();
        String[] nodeAggKeys = keyArray(nodeAggNames, "*");

        Map<String, List<String>> relAggNames =
                (aggregations.size() > 1) ? toStringListMap(aggregations.get(1)) : emptyMap();
        String[] relAggKeys = keyArray(relAggNames, "*");

        // todo bitset
        Set<String> includeRels = computeIncludedRels(config);

        /*
        config:{orphans:false,selfRels:false,limitNodes:100, limitRels:1000, filter:{Person.count_*.min,10,Person.sum_age.max,200,KNOWS.count_*.min:5}}
         */
        boolean orphans = (boolean) config.getOrDefault("orphans", true);
        boolean selfRels = (boolean) config.getOrDefault("selfRels", true);
        long limitNodes = (long) config.getOrDefault("limitNodes", -1L);
        long limitRels = (long) config.getOrDefault("limitRels", -1L);
        long relsPerNode = (long) config.getOrDefault("relsPerNode", -1L);

        // filter min, max on aggregated properties
        // (TYPE.)prop.min: value,(TYPE.)prop.max: value,
        // also filter (esp. max) during aggregation?
        Map<String, Number> filter = configuredFilter(config);

        Map<NodeKey, Set<Node>> grouped = new ConcurrentHashMap<>();
        Map<NodeKey, VirtualNode> virtualNodes = new ConcurrentHashMap<>();
        Map<RelKey, VirtualRelationship> virtualRels = new ConcurrentHashMap<>();

        List<Future> futures = new ArrayList<>(1000);

        ExecutorService pool = pools.getDefaultExecutorService();
        for (String labelName : labels) {
            Label label = Label.label(labelName);
            Label[] singleLabel = {label};

            try (ResourceIterator<Node> nodes =
                    (labelName.equals("*")) ? tx.getAllNodes().iterator() : tx.findNodes(label)) {
                while (nodes.hasNext()) {
                    List<Node> batch = Util.take(nodes, BATCHSIZE);
                    futures.add(Util.inTxFuture(pool, db, txInThread -> {
                        try {
                            for (Node node : batch) {
                                final Node boundNode = Util.rebind(txInThread, node);
                                NodeKey key = keyFor(boundNode, labelName, keys);
                                grouped.compute(key, (k, v) -> {
                                    if (v == null) v = new HashSet<>();
                                    v.add(boundNode);
                                    return v;
                                });
                                virtualNodes.compute(key, (k, v) -> {
                                    if (v == null) {
                                        v = new VirtualNode(singleLabel, propertiesFor(boundNode, keys));
                                    }
                                    VirtualNode vn = v;
                                    if (!nodeAggNames.isEmpty()) {
                                        aggregate(
                                                vn,
                                                nodeAggNames,
                                                nodeAggKeys.length > 0
                                                        ? boundNode.getProperties(nodeAggKeys)
                                                        : Collections.emptyMap());
                                    }
                                    return vn;
                                });
                            }
                        } catch (Exception e) {
                            log.error("Error grouping nodes", e);
                        }
                        return null;
                    }));
                    Util.removeFinished(futures);
                }
            }
        }
        Util.waitForFutures(futures);
        futures.clear();
        Iterator<Map.Entry<NodeKey, Set<Node>>> entries = grouped.entrySet().iterator();
        int size = 0;
        List<Map.Entry<NodeKey, Set<Node>>> batch = new ArrayList<>();
        while (entries.hasNext()) {
            Map.Entry<NodeKey, Set<Node>> outerEntry = entries.next();
            batch.add(outerEntry);
            size += outerEntry.getValue().size();
            if (size > BATCHSIZE || !entries.hasNext()) {
                ArrayList<Map.Entry<NodeKey, Set<Node>>> submitted = new ArrayList<>(batch);
                batch.clear();
                size = 0;
                futures.add(Util.inTxFuture(pool, db, txInThread -> {
                    try {
                        for (Map.Entry<NodeKey, Set<Node>> entry : submitted) {
                            for (Node node : entry.getValue()) {
                                node = Util.rebind(txInThread, node);
                                NodeKey startKey = entry.getKey();
                                VirtualNode v1 = virtualNodes.get(startKey);
                                for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                                    if (includeRels != null
                                            && !includeRels.contains(
                                                    rel.getType().name())) continue;
                                    Node endNode = rel.getEndNode();
                                    for (NodeKey endKey : keysFor(endNode, labels, keys)) {
                                        VirtualNode v2 = virtualNodes.get(endKey);
                                        if (v2 == null) continue;
                                        if (!selfRels && startKey.equals(endKey)) continue;
                                        virtualRels.compute(new RelKey(startKey, endKey, rel), (rk, vRel) -> {
                                            if (vRel == null) vRel = v1.createRelationshipTo(v2, rel.getType());
                                            if (!relAggNames.isEmpty()) {
                                                aggregate(
                                                        vRel,
                                                        relAggNames,
                                                        relAggKeys.length > 0
                                                                ? rel.getProperties(relAggKeys)
                                                                : Collections.emptyMap());
                                            }
                                            return vRel;
                                        });
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error grouping relationships", e);
                    }
                    return null;
                }));
                Util.removeFinished(futures);
            }
        }
        Util.waitForFutures(futures);
        Stream<VirtualNode> stream = fixAggregates(virtualNodes.values()).stream();
        // apply filter
        if (filter != null) stream = stream.filter(n -> filter(n.getLabels(), n.getAllProperties(), filter));
        if (limitNodes > -1) stream = stream.limit(limitNodes);

        Stream<GroupResult> groupResultStream =
                stream.map(n -> new GroupResult(n, getRelationships(n, filter, (int) relsPerNode)));
        if (!orphans)
            groupResultStream = groupResultStream.filter(
                    g -> g.relationships != null && !g.relationships.isEmpty() && g.node.getDegree() > 0);
        groupResultStream = groupResultStream.flatMap(GroupResult::spread);

        if (limitRels > -1) groupResultStream = groupResultStream.limit(limitRels);
        return groupResultStream;
    }

    private Map<String, Number> configuredFilter(Map<String, Object> config) {
        Map<String, Number> filter = (Map<String, Number>) config.get("filter");
        if (filter == null || filter.isEmpty()) return null;
        return filter;
    }

    private boolean filter(String type, Map<String, Object> props, Map<String, Number> filter) {
        if (filter == null || props.isEmpty()) return true;
        return filterProps(type, props, filter);
    }

    private boolean filter(Iterable<Label> types, Map<String, Object> props, Map<String, Number> filter) {
        if (filter == null || props.isEmpty()) return true;
        for (Label label : types) {
            String type = label.name();
            if (!filterProps(type, props, filter)) return false;
        }
        return true;
    }

    private boolean filterProps(String type, Map<String, Object> props, Map<String, Number> filter) {
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            if (!(entry.getValue() instanceof Number)) continue;
            long value = ((Number) entry.getValue()).longValue();
            Number min = filter.getOrDefault(type + "." + entry.getKey() + ".min", filter.get(entry.getKey() + ".min"));
            if (min != null && min.longValue() > value) return false;
            Number max = filter.getOrDefault(type + "." + entry.getKey() + ".max", filter.get(entry.getKey() + ".max"));
            if (max != null && max.longValue() < value) return false;
        }
        return true;
    }

    public List<Relationship> getRelationships(Node n, Map<String, Number> filter, int relsPerNode) {
        List<Relationship> rels = fixAggregates(Iterables.asList(n.getRelationships(Direction.OUTGOING)));
        if (filter != null) rels.removeIf(r -> !filter(r.getType().name(), r.getAllProperties(), filter));
        if (relsPerNode > -1) rels = rels.subList(0, Math.min(relsPerNode, rels.size()));
        return rels;
    }

    public Set<String> computeIncludedRels(@Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (!config.containsKey("includeRels") && !config.containsKey("excludeRels")) return null;

        Set<String> includeRels = Iterables.stream(tx.getAllRelationshipTypes())
                .map(RelationshipType::name)
                .collect(Collectors.toSet());
        if (config.containsKey("includeRels")) {
            Object rels = config.get("includeRels");
            if (rels instanceof Collection) includeRels.retainAll((Collection<String>) rels);
            if (rels instanceof String) includeRels.retainAll(singleton(rels));
        }
        if (config.containsKey("excludeRels")) {
            Object rels = config.get("excludeRels");
            if (rels instanceof Collection) includeRels.removeAll((Collection<String>) rels);
            if (rels instanceof String) includeRels.remove(rels);
        }
        return includeRels;
    }

    private Map<String, List<String>> toStringListMap(Map<String, Object> input) {
        Map<String, List<String>> nodeAggNames = new LinkedHashMap<>(input.size());
        input.forEach((k, v) -> nodeAggNames.put(
                k,
                v instanceof List
                        ? ((List<Object>) v).stream().map(Object::toString).collect(Collectors.toList())
                        : singletonList(v.toString())));
        return nodeAggNames;
    }

    private String[] keyArray(Map<String, ?> map, String... removeKeys) {
        List<String> keys = new ArrayList<>(map.keySet());
        for (String key : removeKeys) keys.remove(key);
        return keys.toArray(new String[keys.size()]);
    }

    private <C extends Collection<T>, T extends Entity> C fixAggregates(C pcs) {
        for (Entity pc : pcs) {
            pc.getAllProperties().entrySet().forEach((entry) -> {
                Object v = entry.getValue();
                String k = entry.getKey();
                if (k.matches("^(min|max|sum)_.+")) {
                    if (v instanceof Number && ((Number) v).doubleValue() == ((Number) v).longValue())
                        entry.setValue(((Number) v).longValue());
                }
                if (k.matches("^avg_.+") && v instanceof double[]) {
                    double[] values = (double[]) v;
                    entry.setValue(values[1] == 0 ? 0 : values[0] / values[1]);
                }
                if (k.matches("^avg_.+") && v instanceof DurationValue duration) {
                    Long count = ((Number) pc.getProperty(k + "_count", 0)).longValue();
                    entry.setValue(divDurationValue(duration, count));
                }
                if (k.matches("^collect_.+") && v instanceof Collection) {
                    entry.setValue(((Collection) v).toArray());
                }
            });
        }
        return pcs;
    }

    // This is copied from the monorepo (as there was no way to use it outside of Neo)
    public DurationValue divDurationValue(DurationValue div, Long number) {
        double divisor = number.doubleValue();

        try {
            return div.approximate(
                    (double) div.get("months").longValue() / divisor,
                    (double) div.get("days").longValue() / divisor,
                    (double) div.get("seconds").longValue() / divisor,
                    (double) div.get("nanoseconds").longValue() / divisor);
        } catch (ArithmeticException | java.lang.ArithmeticException e) {
            return div;
        }
    }

    private void aggregate(Entity pc, Map<String, List<String>> aggregations, Map<String, Object> properties) {
        aggregations.forEach((k2, aggNames) -> {
            for (String aggName : aggNames) {
                String key = aggName + "_" + k2;
                if ("count_*".equals(key)) {
                    pc.setProperty(key, ((Number) pc.getProperty(key, 0)).longValue() + 1);
                } else {
                    Object value = properties.get(k2);
                    if (value != null) {
                        switch (aggName) {
                            case "collect":
                                List<Object> existing = (List<Object>) pc.getProperty(key, new ArrayList<>());
                                existing.add(value);
                                pc.setProperty(key, existing);
                                break;
                            case "count":
                                pc.setProperty(key, ((Number) pc.getProperty(key, 0)).longValue() + 1);
                                break;
                            case "sum":
                                if (value instanceof DurationValue duration) {
                                    DurationValue dv =
                                            (DurationValue) pc.getProperty(key, DurationValue.duration(0, 0, 0, 0));
                                    pc.setProperty(key, (duration).add(dv));
                                } else if (value instanceof Number) {
                                    pc.setProperty(
                                            key,
                                            ((Number) pc.getProperty(key, 0)).doubleValue() + Util.toDouble(value));
                                }
                                break;
                            case "min":
                                pc.setProperty(key, getMin(key, pc, value));
                                break;
                            case "max":
                                pc.setProperty(key, getMax(key, pc, value));
                                break;
                            case "avg": {
                                if (value instanceof Number) {
                                    double[] avg = (double[]) pc.getProperty(key, new double[2]);
                                    avg[0] += Util.toDouble(value);
                                    avg[1] += 1;
                                    pc.setProperty(key, avg);
                                } else if (value instanceof DurationValue) {
                                    DurationValue dv =
                                            (DurationValue) pc.getProperty(key, DurationValue.duration(0, 0, 0, 0));
                                    pc.setProperty(key, ((DurationValue) value).add(dv));
                                    pc.setProperty(
                                            key + "_count",
                                            ((Number) pc.getProperty(key + "_count", 0)).longValue() + 1);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    private Object getMin(String key, Entity pc, Object value) {
        Object prop = pc.getProperty(key);

        if (prop == null) {
            return value;
        }

        Value valueA = Values.unsafeOf(prop, true);
        Value valueB = Values.unsafeOf(value, true);

        if (valueA == null) valueA = Values.NO_VALUE;
        if (valueB == null) valueB = Values.NO_VALUE;

        if (isComparableTypes(prop, value)) {
            return compareValues(valueA, valueB) ? prop : value;
        }

        return returnMinOfDifferentValues(prop, value);
    }

    private Object getMax(String key, Entity pc, Object value) {
        Object prop = pc.getProperty(key);

        if (prop == null) {
            return value;
        }

        Value valueA = Values.unsafeOf(prop, true);
        Value valueB = Values.unsafeOf(value, true);

        if (valueA == null) valueA = Values.NO_VALUE;
        if (valueB == null) valueB = Values.NO_VALUE;

        if (isComparableTypes(prop, value)) {
            return compareValues(valueA, valueB) ? value : prop;
        }

        return returnMaxOfDifferentValues(prop, value);
    }

    private boolean isComparableTypes(Object prop, Object value) {
        return (prop instanceof ZonedDateTime && value instanceof ZonedDateTime)
                || (prop instanceof LocalDateTime && value instanceof LocalDateTime)
                || (prop instanceof LocalDate && value instanceof LocalDate)
                || (prop instanceof OffsetTime && value instanceof OffsetTime)
                || (prop instanceof LocalTime && value instanceof LocalTime)
                || (prop instanceof String && value instanceof String)
                || (prop instanceof Boolean && value instanceof Boolean)
                || (prop instanceof Number && value instanceof Number)
                || ((prop instanceof Collection || prop.getClass().isArray())
                        && (value instanceof Collection || value.getClass().isArray()))
                || (prop instanceof PointValue && value instanceof PointValue);
    }

    private boolean compareValues(Value a, Value b) {
        return switch (Values.COMPARATOR.ternaryCompare(a, b)) {
            case UNDEFINED, EQUAL, SMALLER_THAN -> true;
            case GREATER_THAN -> false;
        };
    }

    private Object returnMinOfDifferentValues(Object prop, Object value) {
        return orderOfType(prop) < orderOfType(value) ? prop : value;
    }

    private Object returnMaxOfDifferentValues(Object prop, Object value) {
        return orderOfType(prop) < orderOfType(value) ? value : prop;
    }

    private int orderOfType(Object value) {
        if (value != null && value.getClass().isArray()) {
            return 0;
        }
        return switch (value) {
            case null -> 11;
            case Collection ignored -> 0;
            case PointValue ignored -> 1;
            case ZonedDateTime ignored -> 2;
            case LocalDateTime ignored -> 3;
            case LocalDate ignored -> 4;
            case OffsetTime ignored -> 5;
            case LocalTime ignored -> 6;
            case DurationValue ignored -> 7;
            case String ignored -> 8;
            case Boolean ignored -> 9;
            case Number ignored -> 10;
            default -> 12;
        };
    }

    /**
     * Returns the properties for the given node according to the specified keys. If a node does not have a property
     * assigned to given key, the value is set to {@code null}.
     *
     * @param node node
     * @param keys property keys
     * @return node properties for keys
     */
    private Map<String, Object> propertiesFor(Node node, String[] keys) {
        Map<String, Object> props = new HashMap<>(keys.length);

        for (String key : keys) {
            props.put(key, node.getProperty(key, null));
        }

        return props;
    }

    /**
     * Creates a grouping key for the given node using its label and grouping properties.
     *
     * @param node  node
     * @param label node label
     * @param keys  property keys
     * @return grouping key
     */
    private NodeKey keyFor(Node node, String label, String[] keys) {
        return new NodeKey(label, propertiesFor(node, keys));
    }

    /**
     * Creates a grouping key for each specified label.
     *
     * @param node   node
     * @param labels node labels
     * @param keys   property keys
     * @return grouping keys
     */
    private Collection<NodeKey> keysFor(Node node, Collection<String> labels, String[] keys) {
        Map<String, Object> props = propertiesFor(node, keys);
        List<NodeKey> result = new ArrayList<>(labels.size());
        if (labels.contains("*")) {
            result.add(new NodeKey("*", props));
        } else {
            for (Label label : node.getLabels()) {
                if (labels.contains(label.name())) {
                    result.add(new NodeKey(label.name(), props));
                }
            }
        }
        return result;
    }

    /**
     * Represents a grouping key for nodes.
     */
    static class NodeKey {
        private final int hash;
        private final String label;
        private final Map<String, Object> values;

        NodeKey(String label, Map<String, Object> values) {
            this.label = label;
            this.values = values;
            hash = 31 * label.hashCode() + values.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NodeKey key = (NodeKey) o;
            return label.equals(key.label) && values.equals(key.values);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Represents a grouping key for relationships.
     */
    private static class RelKey {
        private final int hash;
        private final NodeKey startKey;
        private final NodeKey endKey;
        private final String type;

        RelKey(NodeKey startKey, NodeKey endKey, Relationship rel) {
            this.startKey = startKey;
            this.endKey = endKey;
            this.type = rel.getType().name();
            hash = 31 * (31 * startKey.hashCode() + endKey.hashCode()) + type.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RelKey relKey = (RelKey) o;

            return startKey.equals(relKey.startKey) && endKey.equals(relKey.endKey) && type.equals(relKey.type);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }
}
