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
package apoc.coll;

import static apoc.convert.ConvertUtils.convertArrayToList;
import static apoc.util.Util.containsValueEquals;
import static apoc.util.Util.toAnyValues;
import static java.util.Arrays.asList;

import apoc.util.Util;
import java.lang.reflect.Array;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.RandomAccess;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;

public class Coll {

    public static final char ASCENDING_ORDER_CHAR = '^';

    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    @UserFunction("apoc.coll.stdev")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns sample or population standard deviation with `isBiasCorrected` true or false respectively.")
    public Number stdevCypher5(
            @Name(value = "list", description = "A list to collect the standard deviation from.") List<Number> list,
            @Name(
                            value = "isBiasCorrected",
                            defaultValue = "true",
                            description =
                                    "Will perform a sample standard deviation if `isBiasCorrected`, otherwise a population standard deviation is performed.")
                    boolean isBiasCorrected) {
        return stdev(list, isBiasCorrected);
    }

    @Deprecated
    @UserFunction(name = "apoc.coll.stdev", deprecatedBy = "Cypher's `stDev()` and `stDevP()` functions.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns sample or population standard deviation with `isBiasCorrected` true or false respectively.")
    public Number stdev(
            @Name(value = "list", description = "A list to collect the standard deviation from.") List<Number> list,
            @Name(
                            value = "isBiasCorrected",
                            defaultValue = "true",
                            description =
                                    "Will perform a sample standard deviation if `isBiasCorrected`, otherwise a population standard deviation is performed.")
                    boolean isBiasCorrected) {
        if (list == null || list.isEmpty()) return null;
        final double stdev = StandardDeviation.stdDev(
                list.stream().mapToDouble(Number::doubleValue).toArray(), isBiasCorrected);
        if ((long) stdev == stdev) return (long) stdev;
        return stdev;
    }

    @UserFunction("apoc.coll.runningTotal")
    @Description("Returns an accumulative `LIST<INTEGER | FLOAT>`.")
    public List<Number> runningTotal(
            @Name(value = "list", description = "The list to return a running total from.") List<Number> list) {
        if (list == null || list.isEmpty()) return null;
        MutableDouble sum = new MutableDouble();
        return list.stream()
                .map(i -> {
                    double value = sum.addAndGet(i.doubleValue());
                    if (value == sum.longValue()) return sum.longValue();
                    return value;
                })
                .collect(Collectors.toList());
    }

    public record ZipToRowsListResult(@Description("A zipped pair.") List<Object> value) {}

    @Procedure("apoc.coll.zipToRows")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the two `LIST<ANY>` values zipped together, with one row per zipped pair.")
    public Stream<ZipToRowsListResult> zipToRowsCypher5(
            @Name(value = "list1", description = "The list to zip together with `list2`.") List<Object> list1,
            @Name(value = "list2", description = "The list to zip together with `list1`.") List<Object> list2) {
        return zipToRows(list1, list2);
    }

    @Deprecated
    @Procedure(
            name = "apoc.coll.zipToRows",
            deprecatedBy =
                    "Cypher's `UNWIND` and `range()` function; `UNWIND range(0, size(list1) - 1) AS i RETURN [list1[i], list2[i]]`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the two `LIST<ANY>` values zipped together, with one row per zipped pair.")
    public Stream<ZipToRowsListResult> zipToRows(
            @Name(value = "list1", description = "The list to zip together with `list2`.") List<Object> list1,
            @Name(value = "list2", description = "The list to zip together with `list1`.") List<Object> list2) {
        if (list1.isEmpty()) return Stream.empty();
        ListIterator<Object> it = list2.listIterator();
        return list1.stream().map((e) -> new ZipToRowsListResult(asList(e, it.hasNext() ? it.next() : null)));
    }

    @UserFunction("apoc.coll.zip")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the two given `LIST<ANY>` values zipped together as a `LIST<LIST<ANY>>`.")
    public List<List<Object>> zipCypher5(
            @Name(value = "list1", description = "The list to zip together with `list2`.") List<Object> list1,
            @Name(value = "list2", description = "The list to zip together with `list1`.") List<Object> list2) {
        return zip(list1, list2);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.zip",
            deprecatedBy =
                    "Cypher's `UNWIND` and `range()` function; `COLLECT { UNWIND range(0, size(list1) - 1) AS i RETURN [list1[i], list2[i]] }`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the two given `LIST<ANY>` values zipped together as a `LIST<LIST<ANY>>`.")
    public List<List<Object>> zip(
            @Name(value = "list1", description = "The list to zip together with `list2`.") List<Object> list1,
            @Name(value = "list2", description = "The list to zip together with `list1`.") List<Object> list2) {
        if (list1 == null || list2 == null) return null;
        if (list1.isEmpty() || list2.isEmpty()) return Collections.emptyList();
        List<List<Object>> result = new ArrayList<>(list1.size());
        ListIterator it = list2.listIterator();
        for (Object o1 : list1) {
            result.add(asList(o1, it.hasNext() ? it.next() : null));
        }
        return result;
    }

    @UserFunction("apoc.coll.pairs")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns a `LIST<ANY>` of adjacent elements in the `LIST<ANY>` ([1,2],[2,3],[3,null]).")
    public List<List<Object>> pairsCypher5(
            @Name(value = "list", description = "The list to create pairs from.") List<Object> list) {
        return pairs(list);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.pairs",
            deprecatedBy =
                    "Cypher's list comprehension: `RETURN [i IN range(0, size(list) - 1) | [list[i], list[i + 1]]]`")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns a `LIST<ANY>` of adjacent elements in the `LIST<ANY>` ([1,2],[2,3],[3,null]).")
    public List<List<Object>> pairs(
            @Name(value = "list", description = "The list to create pairs from.") List<Object> list) {
        if (list == null) return null;
        if (list.isEmpty()) return Collections.emptyList();
        return zip(list, list.subList(1, list.size()));
    }

    @UserFunction("apoc.coll.pairsMin")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Returns `LIST<ANY>` values of adjacent elements in the `LIST<ANY>` ([1,2],[2,3]), skipping the final element.")
    public List<List<Object>> pairsMinCypher5(
            @Name(value = "list", description = "The list to create pairs from.") List<Object> list) {
        return pairsMin(list);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.pairsMin",
            deprecatedBy =
                    "Cypher's list comprehension: `RETURN [i IN range(0, size(list) - 2) | [list[i], list[i + 1]]]`")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "Returns `LIST<ANY>` values of adjacent elements in the `LIST<ANY>` ([1,2],[2,3]), skipping the final element.")
    public List<List<Object>> pairsMin(
            @Name(value = "list", description = "The list to create pairs from.") List<Object> list) {
        if (list == null) return null;
        if (list.isEmpty()) return Collections.emptyList();
        return zip(list.subList(0, list.size() - 1), list.subList(1, list.size()));
    }

    @UserFunction("apoc.coll.sum")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the sum of all the `INTEGER | FLOAT` in the `LIST<INTEGER | FLOAT>`.")
    public Double sumCypher5(
            @Name(value = "coll", description = "The list of numbers to create a sum from.") List<Number> list) {
        return sum(list);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.sum",
            deprecatedBy = "Cypher's `reduce()` function: `RETURN reduce(sum = 0.0, x IN list | sum + x)`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the sum of all the `INTEGER | FLOAT` in the `LIST<INTEGER | FLOAT>`.")
    public Double sum(
            @Name(value = "coll", description = "The list of numbers to create a sum from.") List<Number> list) {
        if (list == null || list.isEmpty()) return null;
        double sum = 0;
        for (Number number : list) {
            sum += number.doubleValue();
        }
        return sum;
    }

    @UserFunction("apoc.coll.avg")
    @Description("Returns the average of the numbers in the `LIST<INTEGER | FLOAT>`.")
    public Double avg(@Name(value = "coll", description = "The list to return the average from.") List<Number> list) {
        if (list == null || list.isEmpty()) return null;
        double avg = 0;
        for (Number number : list) {
            avg += number.doubleValue();
        }
        return (avg / (double) list.size());
    }

    @NotThreadSafe
    @UserFunction("apoc.coll.min")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the minimum of all values in the given `LIST<ANY>`.")
    public Object minCypher5(
            @Name(value = "values", description = "The list to find the minimum in.") List<Object> list) {
        return min(list);
    }

    @NotThreadSafe
    @Deprecated
    @UserFunction(
            name = "apoc.coll.min",
            deprecatedBy = "Cypher's `min()` function: `UNWIND values AS value RETURN min(value)`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the minimum of all values in the given `LIST<ANY>`.")
    public Object min(@Name(value = "values", description = "The list to find the minimum in.") List<Object> list) {
        if (list == null || list.isEmpty()) return null;
        if (list.size() == 1) return list.get(0);

        var preparser = "CYPHER " + Util.getCypherVersionString(procedureCallContext) + " runtime=slotted ";
        try (Result result = tx.execute(
                preparser
                        + "return reduce(res=null, x in $list | CASE WHEN res IS NULL OR x<res THEN x ELSE res END) as value",
                Collections.singletonMap("list", list))) {
            return result.next().get("value");
        }
    }

    @NotThreadSafe
    @UserFunction("apoc.coll.max")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the maximum of all values in the given `LIST<ANY>`.")
    public Object maxCypher5(
            @Name(value = "values", description = "The list to find the maximum in.") List<Object> list) {
        return max(list);
    }

    @NotThreadSafe
    @Deprecated
    @UserFunction(
            name = "apoc.coll.max",
            deprecatedBy = "Cypher's `max()` function: `UNWIND values AS value RETURN max(value)`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the maximum of all values in the given `LIST<ANY>`.")
    public Object max(@Name(value = "values", description = "The list to find the maximum in.") List<Object> list) {
        if (list == null || list.isEmpty()) return null;
        if (list.size() == 1) return list.get(0);
        var preparser = "CYPHER " + Util.getCypherVersionString(procedureCallContext) + " runtime=slotted ";
        try (Result result = tx.execute(
                preparser
                        + "RETURN reduce(res=null, x in $list | CASE WHEN res IS NULL OR res<x THEN x ELSE res END) AS value",
                Collections.singletonMap("list", list))) {
            return result.next().get("value");
        }
    }

    @Procedure("apoc.coll.elements")
    @Description("Deconstructs a `LIST<ANY>` into identifiers indicating their specific type.")
    public Stream<ElementsResult> elements(
            @Name(value = "coll", description = "A list of values to deconstruct.") List<Object> list,
            @Name(
                            value = "limit",
                            defaultValue = "-1",
                            description = "The maximum size of elements to deconstruct from the given list.")
                    long limit,
            @Name(value = "offset", defaultValue = "0", description = "The offset to start deconstructing from.")
                    long offset) {
        int elements = (limit < 0 ? list.size() : Math.min((int) (offset + limit), list.size())) - (int) offset;
        if (elements > ElementsResult.MAX_ELEMENTS) elements = ElementsResult.MAX_ELEMENTS;
        ElementsResult result = new ElementsResult();
        for (int i = 0; i < elements; i++) {
            result.add(list.get((int) offset + i));
        }
        return Stream.of(result);
    }

    public static class ElementsResult {
        @Description("The value of the first item.")
        public Object _1;

        @Description("The value of the second item.")
        public Object _2;

        @Description("The value of the third item.")
        public Object _3;

        @Description("The value of the fourth item.")
        public Object _4;

        @Description("The value of the fifth item.")
        public Object _5;

        @Description("The value of the sixth item.")
        public Object _6;

        @Description("The value of the seventh item.")
        public Object _7;

        @Description("The value of the eighth item.")
        public Object _8;

        @Description("The value of the ninth item.")
        public Object _9;

        @Description("The value of the tenth item.")
        public Object _10;

        @Description("The value of the first item, if it is a string value.")
        public String _1s;

        @Description("The value of the second item, if it is a string value.")
        public String _2s;

        @Description("The value of the third item, if it is a string value.")
        public String _3s;

        @Description("The value of the fourth item, if it is a string value.")
        public String _4s;

        @Description("The value of the fifth item, if it is a string value.")
        public String _5s;

        @Description("The value of the sixth item, if it is a string value.")
        public String _6s;

        @Description("The value of the seventh item, if it is a string value.")
        public String _7s;

        @Description("The value of the eighth item, if it is a string value.")
        public String _8s;

        @Description("The value of the ninth item, if it is a string value.")
        public String _9s;

        @Description("The value of the tenth item, if it is a string value.")
        public String _10s;

        @Description("The value of the first item, if it is an integer value.")
        public Long _1i;

        @Description("The value of the second item, if it is an integer value.")
        public Long _2i;

        @Description("The value of the third item, if it is an integer value.")
        public Long _3i;

        @Description("The value of the fourth item, if it is an integer value.")
        public Long _4i;

        @Description("The value of the fifth item, if it is an integer value.")
        public Long _5i;

        @Description("The value of the sixth item, if it is an integer value.")
        public Long _6i;

        @Description("The value of the seventh item, if it is an integer value.")
        public Long _7i;

        @Description("The value of the eighth item, if it is an integer value.")
        public Long _8i;

        @Description("The value of the ninth item, if it is an integer value.")
        public Long _9i;

        @Description("The value of the tenth item, if it is an integer value.")
        public Long _10i;

        @Description("The value of the first item, if it is a float value.")
        public Double _1f;

        @Description("The value of the second item, if it is a float value.")
        public Double _2f;

        @Description("The value of the third item, if it is a float value.")
        public Double _3f;

        @Description("The value of the fourth item, if it is a float value.")
        public Double _4f;

        @Description("The value of the fifth item, if it is a float value.")
        public Double _5f;

        @Description("The value of the sixth item, if it is a float value.")
        public Double _6f;

        @Description("The value of the seventh item, if it is a float value.")
        public Double _7f;

        @Description("The value of the eighth item, if it is a float value.")
        public Double _8f;

        @Description("The value of the ninth item, if it is a float value.")
        public Double _9f;

        @Description("The value of the tenth item, if it is a float value.")
        public Double _10f;

        @Description("The value of the first item, if it is a boolean value.")
        public Boolean _1b;

        @Description("The value of the second item, if it is a boolean value.")
        public Boolean _2b;

        @Description("The value of the third item, if it is a boolean value.")
        public Boolean _3b;

        @Description("The value of the fourth item, if it is a boolean value.")
        public Boolean _4b;

        @Description("The value of the fifth item, if it is a boolean value.")
        public Boolean _5b;

        @Description("The value of the sixth item, if it is a boolean value.")
        public Boolean _6b;

        @Description("The value of the seventh item, if it is a boolean value.")
        public Boolean _7b;

        @Description("The value of the eighth item, if it is a boolean value.")
        public Boolean _8b;

        @Description("The value of the ninth item, if it is a boolean value.")
        public Boolean _9b;

        @Description("The value of the tenth item, if it is a boolean value.")
        public Boolean _10b;

        @Description("The value of the first item, if it is a list value.")
        public List<Object> _1l;

        @Description("The value of the second item, if it is a list value.")
        public List<Object> _2l;

        @Description("The value of the third item, if it is a list value.")
        public List<Object> _3l;

        @Description("The value of the fourth item, if it is a list value.")
        public List<Object> _4l;

        @Description("The value of the fifth item, if it is a list value.")
        public List<Object> _5l;

        @Description("The value of the sixth item, if it is a list value.")
        public List<Object> _6l;

        @Description("The value of the seventh item, if it is a list value.")
        public List<Object> _7l;

        @Description("The value of the eighth item, if it is a list value.")
        public List<Object> _8l;

        @Description("The value of the ninth item, if it is a list value.")
        public List<Object> _9l;

        @Description("The value of the tenth item, if it is a list value.")
        public List<Object> _10l;

        @Description("The value of the first item, if it is a map value.")
        public Map<String, Object> _1m;

        @Description("The value of the second item, if it is a map value.")
        public Map<String, Object> _2m;

        @Description("The value of the third item, if it is a map value.")
        public Map<String, Object> _3m;

        @Description("The value of the fourth item, if it is a map value.")
        public Map<String, Object> _4m;

        @Description("The value of the fifth item, if it is a map value.")
        public Map<String, Object> _5m;

        @Description("The value of the sixth item, if it is a map value.")
        public Map<String, Object> _6m;

        @Description("The value of the seventh item, if it is a map value.")
        public Map<String, Object> _7m;

        @Description("The value of the eighth item, if it is a map value.")
        public Map<String, Object> _8m;

        @Description("The value of the ninth item, if it is a map value.")
        public Map<String, Object> _9m;

        @Description("The value of the tenth item, if it is a map value.")
        public Map<String, Object> _10m;

        @Description("The value of the first item, if it is a node value.")
        public Node _1n;

        @Description("The value of the second item, if it is a node value.")
        public Node _2n;

        @Description("The value of the third item, if it is a node value.")
        public Node _3n;

        @Description("The value of the fourth item, if it is a node value.")
        public Node _4n;

        @Description("The value of the fifth item, if it is a node value.")
        public Node _5n;

        @Description("The value of the sixth item, if it is a node value.")
        public Node _6n;

        @Description("The value of the seventh item, if it is a node value.")
        public Node _7n;

        @Description("The value of the eighth item, if it is a node value.")
        public Node _8n;

        @Description("The value of the ninth item, if it is a node value.")
        public Node _9n;

        @Description("The value of the tenth item, if it is a node value.")
        public Node _10n;

        @Description("The value of the first item, if it is a relationship value.")
        public Relationship _1r;

        @Description("The value of the second item, if it is a relationship value.")
        public Relationship _2r;

        @Description("The value of the third item, if it is a relationship value.")
        public Relationship _3r;

        @Description("The value of the fourth item, if it is a relationship value.")
        public Relationship _4r;

        @Description("The value of the fifth item, if it is a relationship value.")
        public Relationship _5r;

        @Description("The value of the sixth item, if it is a relationship value.")
        public Relationship _6r;

        @Description("The value of the seventh item, if it is a relationship value.")
        public Relationship _7r;

        @Description("The value of the eighth item, if it is a relationship value.")
        public Relationship _8r;

        @Description("The value of the ninth item, if it is a relationship value.")
        public Relationship _9r;

        @Description("The value of the tenth item, if it is a relationship value.")
        public Relationship _10r;

        @Description("The value of the first item, if it is a path value.")
        public Path _1p;

        @Description("The value of the second item, if it is a path value.")
        public Path _2p;

        @Description("The value of the third item, if it is a path value.")
        public Path _3p;

        @Description("The value of the fourth item, if it is a path value.")
        public Path _4p;

        @Description("The value of the fifth item, if it is a path value.")
        public Path _5p;

        @Description("The value of the sixth item, if it is a path value.")
        public Path _6p;

        @Description("The value of the seventh item, if it is a path value.")
        public Path _7p;

        @Description("The value of the eighth item, if it is a path value.")
        public Path _8p;

        @Description("The value of the ninth item, if it is a path value.")
        public Path _9p;

        @Description("The value of the tenth item, if it is a path value.")
        public Path _10p;

        @Description("The number of deconstructed elements.")
        public long elements;

        static final int MAX_ELEMENTS = 10;

        void add(Object o) {
            if (elements == MAX_ELEMENTS) return;
            // Arrays are considered lists in Cypher and
            // should be treated as such
            if (o != null && o.getClass().isArray()) {
                o = convertArrayToList(o);
            }
            setObject(o, (int) elements);
            if (o instanceof String) {
                setString((String) o, (int) elements);
            }
            if (o instanceof Number) {
                setLong(((Number) o).longValue(), (int) elements);
                setDouble(((Number) o).doubleValue(), (int) elements);
            }
            if (o instanceof Boolean) {
                setBoolean((Boolean) o, (int) elements);
            }
            if (o instanceof Map) {
                setMap((Map) o, (int) elements);
            }
            if (o instanceof List) {
                setList((List) o, (int) elements);
            }
            if (o instanceof Node) {
                setNode((Node) o, (int) elements);
            }
            if (o instanceof Relationship) {
                setRelationship((Relationship) o, (int) elements);
            }
            if (o instanceof Path) {
                setPath((Path) o, (int) elements);
            }
            elements++;
        }

        public void setObject(Object o, int idx) {
            switch (idx) {
                case 0 -> _1 = o;
                case 1 -> _2 = o;
                case 2 -> _3 = o;
                case 3 -> _4 = o;
                case 4 -> _5 = o;
                case 5 -> _6 = o;
                case 6 -> _7 = o;
                case 7 -> _8 = o;
                case 8 -> _9 = o;
                case 9 -> _10 = o;
            }
        }

        public void setString(String o, int idx) {
            switch (idx) {
                case 0:
                    _1s = o;
                    break;
                case 1:
                    _2s = o;
                    break;
                case 2:
                    _3s = o;
                    break;
                case 3:
                    _4s = o;
                    break;
                case 4:
                    _5s = o;
                    break;
                case 5:
                    _6s = o;
                    break;
                case 6:
                    _7s = o;
                    break;
                case 7:
                    _8s = o;
                    break;
                case 8:
                    _9s = o;
                    break;
                case 9:
                    _10s = o;
                    break;
            }
        }

        public void setLong(Long o, int idx) {
            switch (idx) {
                case 0 -> _1i = o;
                case 1 -> _2i = o;
                case 2 -> _3i = o;
                case 3 -> _4i = o;
                case 4 -> _5i = o;
                case 5 -> _6i = o;
                case 6 -> _7i = o;
                case 7 -> _8i = o;
                case 8 -> _9i = o;
                case 9 -> _10i = o;
            }
        }

        public void setBoolean(Boolean o, int idx) {
            switch (idx) {
                case 0 -> _1b = o;
                case 1 -> _2b = o;
                case 2 -> _3b = o;
                case 3 -> _4b = o;
                case 4 -> _5b = o;
                case 5 -> _6b = o;
                case 6 -> _7b = o;
                case 7 -> _8b = o;
                case 8 -> _9b = o;
                case 9 -> _10b = o;
            }
        }

        public void setDouble(Double o, int idx) {
            switch (idx) {
                case 0 -> _1f = o;
                case 1 -> _2f = o;
                case 2 -> _3f = o;
                case 3 -> _4f = o;
                case 4 -> _5f = o;
                case 5 -> _6f = o;
                case 6 -> _7f = o;
                case 7 -> _8f = o;
                case 8 -> _9f = o;
                case 9 -> _10f = o;
            }
        }

        public void setNode(Node o, int idx) {
            switch (idx) {
                case 0:
                    _1n = o;
                    break;
                case 1:
                    _2n = o;
                    break;
                case 2:
                    _3n = o;
                    break;
                case 3:
                    _4n = o;
                    break;
                case 4:
                    _5n = o;
                    break;
                case 5:
                    _6n = o;
                    break;
                case 6:
                    _7n = o;
                    break;
                case 7:
                    _8n = o;
                    break;
                case 8:
                    _9n = o;
                    break;
                case 9:
                    _10n = o;
                    break;
            }
        }

        public void setRelationship(Relationship o, int idx) {
            switch (idx) {
                case 0:
                    _1r = o;
                    break;
                case 1:
                    _2r = o;
                    break;
                case 2:
                    _3r = o;
                    break;
                case 3:
                    _4r = o;
                    break;
                case 4:
                    _5r = o;
                    break;
                case 5:
                    _6r = o;
                    break;
                case 6:
                    _7r = o;
                    break;
                case 7:
                    _8r = o;
                    break;
                case 8:
                    _9r = o;
                    break;
                case 9:
                    _10r = o;
                    break;
            }
        }

        public void setPath(Path o, int idx) {
            switch (idx) {
                case 0 -> _1p = o;
                case 1 -> _2p = o;
                case 2 -> _3p = o;
                case 3 -> _4p = o;
                case 4 -> _5p = o;
                case 5 -> _6p = o;
                case 6 -> _7p = o;
                case 7 -> _8p = o;
                case 8 -> _9p = o;
                case 9 -> _10p = o;
            }
        }

        public void setMap(Map o, int idx) {
            switch (idx) {
                case 0 -> _1m = o;
                case 1 -> _2m = o;
                case 2 -> _3m = o;
                case 3 -> _4m = o;
                case 4 -> _5m = o;
                case 5 -> _6m = o;
                case 6 -> _7m = o;
                case 7 -> _8m = o;
                case 8 -> _9m = o;
                case 9 -> _10m = o;
            }
        }

        public void setList(List o, int idx) {
            switch (idx) {
                case 0 -> _1l = o;
                case 1 -> _2l = o;
                case 2 -> _3l = o;
                case 3 -> _4l = o;
                case 4 -> _5l = o;
                case 5 -> _6l = o;
                case 6 -> _7l = o;
                case 7 -> _8l = o;
                case 8 -> _9l = o;
                case 9 -> _10l = o;
            }
        }
    }

    public record PartitionListResult(@Description("The partitioned list.") List<Object> value) {}

    @Procedure("apoc.coll.partition")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Partitions the original `LIST<ANY>` into a new `LIST<ANY>` of the given batch size.\n"
            + "The final `LIST<ANY>` may be smaller than the given batch size.")
    public Stream<PartitionListResult> partitionCypher5(
            @Name(value = "coll", description = "The list to partition into smaller sublists.") List<Object> list,
            @Name(value = "batchSize", description = "The max size of each partitioned sublist.") long batchSize) {
        return partition(list, batchSize);
    }

    @Deprecated
    @Procedure(
            name = "apoc.coll.partition",
            deprecatedBy =
                    "Cypher's list comprehension: `RETURN [i IN range(0, size(list), offset) | list[i..i + offset]] AS value`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Partitions the original `LIST<ANY>` into a new `LIST<ANY>` of the given batch size.\n"
            + "The final `LIST<ANY>` may be smaller than the given batch size.")
    public Stream<PartitionListResult> partition(
            @Name(value = "coll", description = "The list to partition into smaller sublists.") List<Object> list,
            @Name(value = "batchSize", description = "The max size of each partitioned sublist.") long batchSize) {
        if (list == null || list.isEmpty()) return Stream.empty();
        return partitionList(list, (int) batchSize).map(PartitionListResult::new);
    }

    @UserFunction("apoc.coll.partition")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Partitions the original `LIST<ANY>` into a new `LIST<ANY>` of the given batch size.\n"
            + "The final `LIST<ANY>` may be smaller than the given batch size.")
    public List<Object> partitionFnCypher5(
            @Name(value = "coll", description = "The list to partition into smaller sublists.") List<Object> list,
            @Name(value = "batchSize", description = "The max size of each partitioned sublist.") long batchSize) {
        return partitionFn(list, batchSize);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.partition",
            deprecatedBy =
                    "Cypher's list comprehension: `RETURN [i IN range(0, size(list), offset) | list[i..i + offset]] AS value`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Partitions the original `LIST<ANY>` into a new `LIST<ANY>` of the given batch size.\n"
            + "The final `LIST<ANY>` may be smaller than the given batch size.")
    public List<Object> partitionFn(
            @Name(value = "coll", description = "The list to partition into smaller sublists.") List<Object> list,
            @Name(value = "batchSize", description = "The max size of each partitioned sublist.") long batchSize) {
        if (list == null || list.isEmpty()) return new ArrayList<>();
        return partitionList(list, (int) batchSize).collect(Collectors.toList());
    }

    public record SplitListResult(@Description("The split list.") List<Object> value) {}

    @Procedure("apoc.coll.split")
    @Description("Splits a collection by the given value.\n"
            + "The value itself will not be part of the resulting `LIST<ANY>` values.")
    public Stream<SplitListResult> split(
            @Name(value = "coll", description = "The list to split into parts.") List<Object> list,
            @Name(value = "value", description = "The value to split the given list by.") Object value) {
        if (list == null || list.isEmpty()) return Stream.empty();
        List<Object> l = new ArrayList<>(list);
        List<List<Object>> result = new ArrayList<>(10);
        int idx = Util.indexOf(l, value);
        while (idx != -1) {
            List<Object> subList = l.subList(0, idx);
            if (!subList.isEmpty()) result.add(subList);
            l = l.subList(idx + 1, l.size());
            idx = Util.indexOf(l, value);
        }
        if (!l.isEmpty()) result.add(l);
        return result.stream().map(SplitListResult::new);
    }

    private Stream<List<Object>> partitionList(@Name("values") List list, @Name("batchSize") int batchSize) {
        int total = list.size();
        int pages = total % batchSize == 0 ? total / batchSize : total / batchSize + 1;
        return IntStream.range(0, pages).parallel().boxed().map(page -> {
            int from = page * batchSize;
            return list.subList(from, Math.min(from + batchSize, total));
        });
    }

    @UserFunction("apoc.coll.contains")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns whether or not the given value exists in the given collection.")
    public boolean containsCypher5(
            @Name(value = "coll", description = "The list to search for the given value.") List<Object> coll,
            @Name(value = "value", description = "The value in the list to check for the existence of.") Object value) {
        return contains(coll, value);
    }

    @Deprecated
    @UserFunction(name = "apoc.coll.contains", deprecatedBy = "Cypher's `IN`: `value IN coll`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns whether or not the given value exists in the given collection.")
    public boolean contains(
            @Name(value = "coll", description = "The list to search for the given value.") List<Object> coll,
            @Name(value = "value", description = "The value in the list to check for the existence of.") Object value) {
        if (coll == null || coll.isEmpty()) return false;
        return Util.containsValueEquals(coll, value);
    }

    @UserFunction("apoc.coll.set")
    @Description("Sets the element at the given index to the new value.")
    public List<Object> set(
            @Name(value = "coll", description = "The list to be updated.") List<Object> coll,
            @Name(value = "index", description = "The position of the value in the list to be updated.") long index,
            @Name(value = "value", description = "The new value to be set.") Object value) {
        if (coll == null) return null;
        if (index < 0 || value == null || index >= coll.size()) return coll;

        List<Object> list = new ArrayList<>(coll);
        list.set((int) index, value);
        return list;
    }

    @UserFunction("apoc.coll.insert")
    @Description("Inserts a value into the specified index in the `LIST<ANY>`.")
    public List<Object> insert(
            @Name(value = "coll", description = "The list to insert a value into.") List<Object> coll,
            @Name(value = "index", description = "The position in the list to insert the given value.") long index,
            @Name(value = "value", description = "The value to be inserted.") Object value) {
        if (coll == null) return null;
        if (index < 0 || value == null || index > coll.size()) return coll;

        List<Object> list = new ArrayList<>(coll);
        list.add((int) index, value);
        return list;
    }

    @UserFunction("apoc.coll.insertAll")
    @Description("Inserts all of the values into the `LIST<ANY>`, starting at the specified index.")
    public List<Object> insertAll(
            @Name(value = "coll", description = "The list to insert the values into.") List<Object> coll,
            @Name(value = "index", description = "The position in the list to start inserting the given values.")
                    long index,
            @Name(value = "values", description = "The values to be inserted.") List<Object> values) {
        if (coll == null) return null;
        if (index < 0 || values == null || values.isEmpty() || index > coll.size()) return coll;

        List<Object> list = new ArrayList<>(coll);
        list.addAll((int) index, values);
        return list;
    }

    @UserFunction("apoc.coll.remove")
    @Description(
            "Removes a range of values from the `LIST<ANY>`, beginning at position index for the given length of values.")
    public List<Object> remove(
            @Name(value = "coll", description = "The list to remove values from.") List<Object> coll,
            @Name(value = "index", description = "The starting index in the list to begin removing values from.")
                    long index,
            @Name(value = "length", defaultValue = "1", description = "The number of values to remove.") long length) {
        if (coll == null) return null;
        if (index < 0 || index >= coll.size() || length <= 0) return coll;

        List<Object> list = new ArrayList<>(coll);
        for (long i = index + length - 1; i >= index; i--) {
            if (i < list.size()) list.remove((int) i);
        }
        return list;
    }

    @UserFunction("apoc.coll.indexOf")
    @Description("Returns the index for the first occurrence of the specified value in the `LIST<ANY>`.")
    public long indexOf(
            @Name(value = "coll", description = "The list to find the given value in.") List<Object> coll,
            @Name(value = "value", description = "The value to find the first occurrence of in the given list.")
                    Object value) {
        // return reduce(res=[0,-1], x in $list | CASE WHEN x=$value AND res[1]=-1 THEN [res[0], res[0]+1] ELSE
        // [res[0]+1, res[1]] END)[1] as value
        if (coll == null || coll.isEmpty()) return -1;
        return Util.indexOf(coll, value);
    }

    @UserFunction("apoc.coll.containsAll")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns whether or not all of the given values exist in the given collection.")
    public boolean containsAllCypher5(
            @Name(value = "coll1", description = "The list to search for the given values in.") List<Object> coll,
            @Name(value = "coll2", description = "The list of values in the given list to check for the existence of.")
                    List<Object> values) {
        return containsAll(coll, values);
    }

    @Deprecated
    @UserFunction(name = "apoc.coll.containsAll", deprecatedBy = "Cypher's `all()`: `all(x IN coll2 WHERE x IN coll1)`")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns whether or not all of the given values exist in the given collection.")
    public boolean containsAll(
            @Name(value = "coll1", description = "The list to search for the given values in.") List<Object> coll,
            @Name(value = "coll2", description = "The list of values in the given list to check for the existence of.")
                    List<Object> values) {
        if (coll == null || coll.isEmpty() || values == null) return false;
        Set<Object> objects = new HashSet<>(coll);

        return values.stream().allMatch(i -> containsValueEquals(objects, i));
    }

    @UserFunction("apoc.coll.containsSorted")
    @Description(
            "Returns whether or not the given value exists in an already sorted collection (using a binary search).")
    public boolean containsSorted(
            @Name(value = "coll", description = "The sorted list to search for the given value.") List<Object> coll,
            @Name(value = "value", description = "The value to check for the existence of in the list.") Object value) {
        if (coll == null || coll.isEmpty()) return false;
        int batchSize = 5000 - 1; // Collections.binarySearchThreshold
        List list = (coll instanceof RandomAccess || coll.size() < batchSize) ? coll : new ArrayList(coll);
        return Collections.binarySearch(list, value) >= 0;
    }

    @UserFunction("apoc.coll.containsAllSorted")
    @Description(
            "Returns whether or not all of the given values in the second `LIST<ANY>` exist in an already sorted collection (using a binary search).")
    public boolean containsAllSorted(
            @Name(value = "coll1", description = "The sorted list to search for the given values.") List<Object> coll,
            @Name(value = "coll2", description = "The list of values to check for existence of in the given list.")
                    List<Object> values) {
        if (coll == null || values == null) return false;
        int batchSize = 5000 - 1; // Collections.binarySearchThreshold
        List list = (coll instanceof RandomAccess || coll.size() < batchSize) ? coll : new ArrayList(coll);
        for (Object value : values) {
            boolean result = Collections.binarySearch(list, value) >= 0;
            if (!result) return false;
        }
        return true;
    }

    @UserFunction("apoc.coll.isEqualCollection")
    @Description(
            "Returns true if the two collections contain the same elements with the same cardinality in any order.")
    public boolean isEqualCollection(
            @Name(value = "coll", description = "The list of values to compare against `list2` and check for equality.")
                    List<Object> first,
            @Name(
                            value = "values",
                            description = "The list of values to compare against `list1` and check for equality.")
                    List<Object> second) {
        if (first == null && second == null) return true;
        if (first == null || second == null || first.size() != second.size()) return false;
        return new HashSet<>(frequencies(second)).containsAll(frequencies(first));
    }

    @UserFunction("apoc.coll.toSet")
    @Description("Returns a unique `LIST<ANY>` from the given `LIST<ANY>`.")
    public List<Object> toSet(
            @Name(value = "coll", description = "The list of values to remove all duplicates from.")
                    List<Object> list) {
        if (list == null) return null;
        List<AnyValue> anyValues = toAnyValues(list);
        return new SetBackedList(new LinkedHashSet(anyValues));
    }

    @UserFunction("apoc.coll.sumLongs")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the sum of all the `INTEGER | FLOAT` in the `LIST<INTEGER | FLOAT>`.")
    public Long sumLongsCypher5(
            @Name(
                            value = "coll",
                            description =
                                    "The list of numbers to create a sum from after each is cast to a java Long value.")
                    List<Number> list) {
        return sumLongs(list);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.sumLongs",
            deprecatedBy =
                    "Cypher's `reduce()` function: `RETURN reduce(sum = 0.0, x IN toIntegerList(list) | sum + x)`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the sum of all the `INTEGER | FLOAT` in the `LIST<INTEGER | FLOAT>`.")
    public Long sumLongs(
            @Name(
                            value = "coll",
                            description =
                                    "The list of numbers to create a sum from after each is cast to a java Long value.")
                    List<Number> list) {
        if (list == null) return null;
        long sum = 0;
        for (Number number : list) {
            sum += number.longValue();
        }
        return sum;
    }

    @UserFunction("apoc.coll.sort")
    @Description("Sorts the given `LIST<ANY>` into ascending order.")
    public List<Object> sort(@Name(value = "coll", description = "The list to be sorted.") List<Object> coll) {
        if (coll == null || coll.isEmpty()) return Collections.emptyList();
        List sorted = new ArrayList<>(coll);
        Collections.sort((List<? extends Comparable>) sorted);
        return sorted;
    }

    @UserFunction("apoc.coll.sortNodes")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Sorts the given `LIST<NODE>` by the property of the nodes into descending order.")
    public List<Node> sortNodesCypher5(
            @Name(value = "coll", description = "The list of nodes to be sorted.") List<Node> coll,
            @Name(value = "prop", description = "The property key on the node to be used to sort the list by.")
                    String prop) {
        return sortNodes(coll, prop);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.sortNodes",
            deprecatedBy =
                    "Cypher's COLLECT {} and ORDER BY: `RETURN COLLECT { MATCH (n) RETURN n ORDER BY n.prop DESC }`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Sorts the given `LIST<NODE>` by the property of the nodes into descending order.")
    public List<Node> sortNodes(
            @Name(value = "coll", description = "The list of nodes to be sorted.") List<Node> coll,
            @Name(value = "prop", description = "The property key on the node to be used to sort the list by.")
                    String prop) {
        if (coll == null || coll.isEmpty()) return Collections.emptyList();
        List<Node> sorted = new ArrayList<>(coll);
        int reverseOrder = reverseOrder(prop);
        String cleanedProp = cleanProperty(prop);
        Collections.sort(
                sorted,
                (x, y) -> reverseOrder * compare(x.getProperty(cleanedProp, null), y.getProperty(cleanedProp, null)));
        return sorted;
    }

    @UserFunction("apoc.coll.sortMaps")
    @Description(
            "Sorts the given `LIST<MAP<STRING, ANY>>` into descending order, based on the `MAP` property indicated by `prop`.")
    public List<Map<String, Object>> sortMaps(
            @Name(value = "list", description = "The list of maps to be sorted.") List<Map<String, Object>> coll,
            @Name(value = "prop", description = "The property key to be used to sort the list of maps by.")
                    String prop) {
        if (coll == null || coll.isEmpty()) return Collections.emptyList();
        List<Map<String, Object>> sorted = new ArrayList<>(coll);
        int reverseOrder = reverseOrder(prop);
        String cleanedProp = cleanProperty(prop);
        Collections.sort(sorted, (x, y) -> reverseOrder * compare(x.get(cleanedProp), y.get(cleanedProp)));
        return sorted;
    }

    public int reverseOrder(String prop) {
        return prop.charAt(0) == ASCENDING_ORDER_CHAR ? 1 : -1;
    }

    public String cleanProperty(String prop) {
        return prop.charAt(0) == ASCENDING_ORDER_CHAR ? prop.substring(1) : prop;
    }

    public static int compare(Object o1, Object o2) {
        if (o1 == null) return o2 == null ? 0 : -1;
        if (o2 == null) return 1;
        if (o1.equals(o2)) return 0;
        if (o1 instanceof Number && o2 instanceof Number) {
            if (o1 instanceof Double || o2 instanceof Double || o1 instanceof Float || o2 instanceof Float)
                return Double.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
            return Long.compare(((Number) o1).longValue(), ((Number) o2).longValue());
        }
        if (o1 instanceof Boolean b1 && o2 instanceof Boolean) return b1 ? 1 : -1;
        if (o1 instanceof Node n1 && o2 instanceof Node n2)
            return n1.getElementId().compareTo(n2.getElementId());
        if (o1 instanceof Relationship rel1 && o2 instanceof Relationship rel2)
            return rel1.getElementId().compareTo(rel2.getElementId());
        return o1.toString().compareTo(o2.toString());
    }

    @UserFunction("apoc.coll.union")
    @Description("Returns the distinct union of the two given `LIST<ANY>` values.")
    public List<Object> union(
            @Name(
                            value = "list1",
                            description =
                                    "The list of values to compare against `list2` and form a distinct union from.")
                    List<Object> first,
            @Name(
                            value = "list2",
                            description =
                                    "The list of values to compare against `list1` and form a distinct union from.")
                    List<Object> second) {
        if (first == null) return second;
        if (second == null) return first;

        Set<Object> set = Util.toAnyValuesSet(first);
        for (Object item : second) {
            set.add(ValueUtils.of(item));
        }
        return new SetBackedList(set);
    }

    @UserFunction("apoc.coll.removeAll")
    @Description("Returns the first `LIST<ANY>` with all elements also present in the second `LIST<ANY>` removed.")
    public List<Object> removeAll(
            @Name(value = "list1", description = "The list to remove values from.") List<Object> first,
            @Name(value = "list2", description = "The values to remove from the given list.") List<Object> second) {
        if (first == null) return null;
        List<Object> list = new ArrayList<>(toAnyValues(first));
        if (second != null) list.removeAll(toAnyValues(second));
        return list;
    }

    @UserFunction("apoc.coll.subtract")
    @Description("Returns the first `LIST<ANY>` as a set with all the elements of the second `LIST<ANY>` removed.")
    public List<Object> subtract(
            @Name(value = "list1", description = "The list to remove values from.") List<Object> first,
            @Name(value = "list2", description = "The list of values to be removed from `list1`.")
                    List<Object> second) {
        if (first == null) return null;
        if (second == null) return first;

        var set1 = Util.toAnyValuesSet(first);
        var set2 = Util.toAnyValuesSet(second);
        set1.removeAll(set2);
        return new SetBackedList(set1);
    }

    @UserFunction("apoc.coll.intersection")
    @Description("Returns the distinct intersection of two `LIST<ANY>` values.")
    public List<Object> intersection(
            @Name(
                            value = "list1",
                            description =
                                    "The list of values to compare against `list2` and form an intersection from.")
                    List<Object> first,
            @Name(
                            value = "list2",
                            description =
                                    "The list of values to compare against `list1` and form an intersection from.")
                    List<Object> second) {
        if (first == null || second == null) return Collections.emptyList();
        Set<Object> set = Util.toAnyValuesSet(first);
        set.retainAll(Util.toAnyValuesSet(second));
        return new SetBackedList(set);
    }

    @UserFunction("apoc.coll.disjunction")
    @Description("Returns the disjunct set from two `LIST<ANY>` values.")
    public List<Object> disjunction(
            @Name(
                            value = "list1",
                            description = "The list of values to compare against `list2` and form a disjunction from.")
                    List<Object> first,
            @Name(
                            value = "list2",
                            description = "The list of values to compare against `list1` and form a disjunction from.")
                    List<Object> second) {
        if (first == null) return second;
        if (second == null) return first;
        Set<Object> intersection = Util.toAnyValuesSet(first);
        Set<Object> secondSet = Util.toAnyValuesSet(second);
        Set<Object> set = new HashSet<>(intersection);
        intersection.retainAll(secondSet);
        set.addAll(secondSet);
        set.removeAll(intersection);
        return new SetBackedList(set);
    }

    @UserFunction("apoc.coll.unionAll")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the full union of the two given `LIST<ANY>` values (duplicates included).")
    public List<Object> unionAllCypher5(
            @Name(value = "list1", description = "The list of values to compare against `list2` and form a union from.")
                    List<Object> first,
            @Name(value = "list2", description = "The list of values to compare against `list1` and form a union from.")
                    List<Object> second) {
        return unionAll(first, second);
    }

    @Deprecated
    @UserFunction(name = "apoc.coll.unionAll", deprecatedBy = "Cypher's list concatenation: `list1 + list2`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the full union of the two given `LIST<ANY>` values (duplicates included).")
    public List<Object> unionAll(
            @Name(value = "list1", description = "The list of values to compare against `list2` and form a union from.")
                    List<Object> first,
            @Name(value = "list2", description = "The list of values to compare against `list1` and form a union from.")
                    List<Object> second) {
        if (first == null) return second;
        if (second == null) return first;
        List<Object> list = new ArrayList<>(first);
        list.addAll(second);
        return list;
    }

    @UserFunction("apoc.coll.shuffle")
    @Description("Returns the `LIST<ANY>` shuffled.")
    public List<Object> shuffle(@Name(value = "coll", description = "The list to be shuffled.") List<Object> coll) {
        if (coll == null || coll.isEmpty()) {
            return Collections.emptyList();
        } else if (coll.size() == 1) {
            return coll;
        }

        List<Object> shuffledList = new ArrayList<>(coll);
        Collections.shuffle(shuffledList);
        return shuffledList;
    }

    @UserFunction("apoc.coll.randomItem")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns a random item from the `LIST<ANY>`, or null on `LIST<NOTHING>` or `LIST<NULL>`.")
    public Object randomItemCypher5(
            @Name(value = "coll", description = "The list to return a random item from.") List<Object> coll) {
        return randomItem(coll);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.randomItem",
            deprecatedBy = "Cypher's `rand()` function: `RETURN list[toInteger(rand() * size(list))]`")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns a random item from the `LIST<ANY>`, or null on `LIST<NOTHING>` or `LIST<NULL>`.")
    public Object randomItem(
            @Name(value = "coll", description = "The list to return a random item from.") List<Object> coll) {
        if (coll == null || coll.isEmpty()) {
            return null;
        } else if (coll.size() == 1) {
            return coll.get(0);
        }

        return coll.get(ThreadLocalRandom.current().nextInt(coll.size()));
    }

    @UserFunction("apoc.coll.randomItems")
    @Description(
            "Returns a `LIST<ANY>` of `itemCount` random items from the original `LIST<ANY>` (optionally allowing elements in the original `LIST<ANY>` to be selected more than once).")
    public List<Object> randomItems(
            @Name(value = "coll", description = "The list to return random items from.") List<Object> coll,
            @Name(value = "itemCount", description = "The number of random items to return from the list.")
                    long itemCount,
            @Name(
                            value = "allowRepick",
                            defaultValue = "false",
                            description = "Whether elements from the original list can be selected more than once.")
                    boolean allowRepick) {
        if (coll == null || coll.isEmpty() || itemCount <= 0) {
            return Collections.emptyList();
        }

        List<Object> pickList = new ArrayList<>(coll);
        List<Object> randomItems = new ArrayList<>((int) itemCount);
        Random random = ThreadLocalRandom.current();

        if (!allowRepick && itemCount >= coll.size()) {
            Collections.shuffle(pickList);
            return pickList;
        }

        while (randomItems.size() < itemCount) {
            Object item = allowRepick
                    ? pickList.get(random.nextInt(pickList.size()))
                    : pickList.remove(random.nextInt(pickList.size()));
            randomItems.add(item);
        }

        return randomItems;
    }

    @UserFunction("apoc.coll.containsDuplicates")
    @Description("Returns true if a collection contains duplicate elements.")
    public boolean containsDuplicates(
            @Name(value = "coll", description = "The list to check for duplicates in.") List<Object> coll) {
        if (coll == null || coll.size() <= 1) {
            return false;
        }

        var set = new HashSet<>(Math.max((int) (coll.size() / .75f) + 1, 16));
        boolean hasntAdded;
        for (Object item : coll) {
            // Use the ValueUtil.of version, as arrays and lists in Cypher may differ, but are considered the *same*.
            hasntAdded = set.add(ValueUtils.of(item));
            // If the item has already been added, then return true as it is a duplicate
            if (!hasntAdded) {
                return true;
            }
        }

        return set.size() < coll.size();
    }

    @UserFunction("apoc.coll.duplicates")
    @Description("Returns a `LIST<ANY>` of duplicate items in the collection.")
    public List<Object> duplicates(
            @Name(value = "coll", description = "The list to collect duplicate values from.") List<Object> coll) {
        if (coll == null || coll.size() <= 1) {
            return Collections.emptyList();
        }

        var set = new HashSet<>(Math.max((int) (coll.size() / .75f) + 1, 16));
        Set<Object> duplicates = new LinkedHashSet<>();
        for (Object item : coll) {
            // Use the ValueUtil.of version, as arrays and lists in Cypher may differ, but are considered the *same*.
            AnyValue normalizedItem = ValueUtils.of(item);
            // If the item has already been added, then add as it is a duplicate
            if (!set.add(normalizedItem)) {
                duplicates.add(normalizedItem);
            }
        }

        return new ArrayList(duplicates);
    }

    @UserFunction("apoc.coll.duplicatesWithCount")
    @Description(
            "Returns a `LIST<ANY>` of duplicate items in the collection and their count, keyed by `item` and `count`.")
    public List<Map<String, Object>> duplicatesWithCount(
            @Name(value = "coll", description = "The list to collect duplicate values and their count from.")
                    List<Object> coll) {
        if (coll == null || coll.size() <= 1) {
            return Collections.emptyList();
        }

        // mimicking a counted bag
        Map<Object, MutableInt> duplicates = new LinkedHashMap<>(coll.size());
        List<Map<String, Object>> resultList = new ArrayList<>();

        for (Object obj : coll) {
            MutableInt counter = duplicates.computeIfAbsent(ValueUtils.of(obj), k -> new MutableInt());
            counter.increment();
        }

        duplicates.forEach((o, intCounter) -> {
            int count = intCounter.intValue();
            if (count > 1) {
                Map<String, Object> entry = new LinkedHashMap<>(2);
                entry.put("item", o);
                entry.put("count", (long) count);
                resultList.add(entry);
            }
        });

        return resultList;
    }

    @UserFunction("apoc.coll.frequencies")
    @Description("Returns a `LIST<ANY>` of frequencies of the items in the collection, keyed by `item` and `count`.")
    public List<Map<String, Object>> frequencies(
            @Name(value = "coll", description = "The list to return items and their count from.") List<Object> coll) {
        if (coll == null || coll.isEmpty()) {
            return Collections.emptyList();
        }

        // mimicking a counted bag
        Map<Object, MutableInt> counts = new LinkedHashMap<>(coll.size());
        List<Map<String, Object>> resultList = new ArrayList<>();

        for (Object obj : coll) {
            MutableInt counter = counts.computeIfAbsent(ValueUtils.of(obj), k -> new MutableInt());
            counter.increment();
        }

        counts.forEach((o, intCounter) -> {
            int count = intCounter.intValue();
            Map<String, Object> entry = new LinkedHashMap<>(2);
            entry.put("item", o);
            entry.put("count", (long) count);
            resultList.add(entry);
        });

        return resultList;
    }

    @UserFunction("apoc.coll.frequenciesAsMap")
    @Description("Returns a `MAP` of frequencies of the items in the collection, keyed by `item` and `count`.")
    public Map<String, Object> frequenciesAsMap(
            @Name(value = "coll", description = "The list to return items and their count from.") List<Object> coll) {
        if (coll == null) return Collections.emptyMap();
        return frequencies(coll).stream()
                .collect(Collectors.toMap(t -> Util.toPrettyPrint(t.get("item")), v -> v.get("count")));
    }

    @UserFunction("apoc.coll.occurrences")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the count of the given item in the collection.")
    public long occurrencesCypher5(
            @Name(value = "coll", description = "The list to collect the count of the given value from.")
                    List<Object> coll,
            @Name(value = "item", description = "The value to count in the given list.") Object item) {
        return occurrences(coll, item);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.occurrences",
            deprecatedBy =
                    "Cypher's reduce() and `CASE` expression: `RETURN reduce(count = 0, x IN coll | count + CASE WHEN x = item THEN 1 ELSE 0 END)`")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the count of the given item in the collection.")
    public long occurrences(
            @Name(value = "coll", description = "The list to collect the count of the given value from.")
                    List<Object> coll,
            @Name(value = "item", description = "The value to count in the given list.") Object item) {
        if (coll == null || coll.isEmpty()) {
            return 0;
        }

        long occurrences = 0;

        for (Object obj : coll) {
            if (Util.valueEquals(item, obj)) {
                occurrences++;
            }
        }

        return occurrences;
    }

    @UserFunction("apoc.coll.flatten")
    @Description("Flattens the given `LIST<ANY>` (to flatten nested `LIST<ANY>` values, set recursive to true).")
    public List<Object> flatten(
            @Name(value = "coll", description = "The list to flatten.") List<Object> coll,
            @Name(
                            value = "recursive",
                            defaultValue = "false",
                            description = "Whether nested list items should also be flattened.")
                    boolean recursive) {
        if (coll == null) return Collections.emptyList();
        if (recursive) return flattenRecursive(coll, 0); // flatten everything
        return flattenRecursive(coll, 0, 2); // flatten one level of lists in the input list if not recursive
    }

    private static List<Object> flattenRecursive(Object aObject, int aDepth, int aStopDepth) {
        List<Object> vResult = new ArrayList<Object>();

        if (aDepth == aStopDepth) { // always for a future arbitrary stopping point
            vResult.add(aObject);
        } else {
            if (aObject.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(aObject); i++) {
                    vResult.addAll(flattenRecursive(Array.get(aObject, i), aDepth + 1, aStopDepth));
                }
            } else if (aObject instanceof List) {
                for (Object vElement : (List<?>) aObject) {
                    vResult.addAll(flattenRecursive(vElement, aDepth + 1, aStopDepth));
                }
            } else {
                vResult.add(aObject);
            }
        }
        return vResult;
    }

    private static List<Object> flattenRecursive(Object aObject, int aDepth) {
        return flattenRecursive(aObject, aDepth, -1); // we only stop when all lists are flattened
    }

    @UserFunction("apoc.coll.sortMulti")
    @Description(
            """
            Sorts the given `LIST<MAP<STRING, ANY>>` by the given fields.
            To indicate that a field should be sorted according to ascending values, prefix it with a caret (^).
            It is also possible to add limits to the `LIST<MAP<STRING, ANY>>` and to skip values.""")
    public List<Map<String, Object>> sortMulti(
            @Name(value = "coll", description = "The list of maps to be sorted.") List<Map<String, Object>> coll,
            @Name(
                            value = "orderFields",
                            defaultValue = "[]",
                            description = "The property keys to be used to sort the list of maps by.")
                    List<String> orderFields,
            @Name(value = "limit", defaultValue = "-1", description = "The amount of results to return.") long limit,
            @Name(value = "skip", defaultValue = "0", description = "The amount to skip by.") long skip) {
        List<Map<String, Object>> result = new ArrayList<>(coll);

        if (orderFields != null && !orderFields.isEmpty()) {

            List<Pair<String, Boolean>> fields = orderFields.stream()
                    .map(v -> {
                        boolean asc = v.charAt(0) == '^';
                        return Pair.of(asc ? v.substring(1) : v, asc);
                    })
                    .collect(Collectors.toList());

            Comparator<Map<String, Comparable<Object>>> compare = (o1, o2) -> {
                int a = 0;
                for (Pair<String, Boolean> s : fields) {
                    if (a != 0) break;
                    String name = s.getLeft();
                    Comparable<Object> v1 = o1.get(name);
                    Comparable<Object> v2 = o2.get(name);
                    if (v1 != v2) {
                        int cmp = (v1 == null) ? -1 : (v2 == null) ? 1 : v1.compareTo(v2);
                        a = (s.getRight()) ? cmp : -cmp;
                    }
                }
                return a;
            };

            Collections.sort((List<Map<String, Comparable<Object>>>) (List) result, compare);
        }
        if (skip > 0 && limit != -1L) return result.subList((int) skip, (int) (skip + limit));
        if (skip > 0) return result.subList((int) skip, result.size());
        if (limit != -1L) return result.subList(0, (int) limit);
        return result;
    }

    @UserFunction("apoc.coll.combinations")
    @Description(
            "Returns a collection of all combinations of `LIST<ANY>` elements between the selection size `minSelect` and `maxSelect` (default: `minSelect`).")
    public List<List<Object>> combinations(
            @Name(value = "coll", description = "The list to return the combinations from.") List<Object> coll,
            @Name(value = "minSelect", description = "The minimum selection size of the combination.") long minSelectIn,
            @Name(
                            value = "maxSelect",
                            defaultValue = "-1",
                            description = "The maximum selection size of the combination.")
                    long maxSelectIn) {
        int minSelect = (int) minSelectIn;
        int maxSelect = (int) maxSelectIn;
        maxSelect = maxSelect == -1 ? minSelect : maxSelect;

        if (coll == null
                || coll.isEmpty()
                || minSelect < 1
                || minSelect > coll.size()
                || minSelect > maxSelect
                || maxSelect > coll.size()) {
            return Collections.emptyList();
        }

        List<List<Object>> combinations = new ArrayList<>();

        for (int i = minSelect; i <= maxSelect; i++) {
            Iterator<int[]> itr = new Combinations(coll.size(), i).iterator();

            while (itr.hasNext()) {
                List<Object> entry = new ArrayList<>(i);
                int[] indexes = itr.next();
                if (indexes.length > 0) {
                    for (int index : indexes) {
                        entry.add(coll.get(index));
                    }
                    combinations.add(entry);
                }
            }
        }

        return combinations;
    }

    @UserFunction("apoc.coll.different")
    @Description("Returns true if all the values in the given `LIST<ANY>` are unique.")
    public boolean different(
            @Name(value = "coll", description = "The list to check for duplicates.") List<Object> coll) {
        if (coll == null) return false;
        var set = new HashSet<>(Math.max((int) (coll.size() / .75f) + 1, 16));
        boolean hasntAdded;
        for (Object item : coll) {
            // Use the ValueUtil.of version, as arrays and lists in Cypher may differ, but are considered the *same*.
            hasntAdded = set.add(ValueUtils.of(item));
            // If the item has already been added, then return true as it is a duplicate
            if (!hasntAdded) {
                return false;
            }
        }
        return true;
    }

    @UserFunction("apoc.coll.dropDuplicateNeighbors")
    @Description("Removes duplicate consecutive objects in the `LIST<ANY>`.")
    public List<Object> dropDuplicateNeighbors(
            @Name(value = "list", description = "The list to remove duplicate consecutive values from.")
                    List<Object> list) {
        if (list == null) return null;
        List<Object> newList = new ArrayList<>(list.size());

        Object last = null;
        for (Object element : list) {
            if (element == null && last != null || element != null && !Util.valueEquals(element, last)) {
                newList.add(element);
                last = element;
            }
        }

        return newList;
    }

    @UserFunction("apoc.coll.fill")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns a `LIST<ANY>` with the given count of items.")
    public List<Object> fillCypher5(
            @Name(value = "item", description = "The item to be present in the returned list.") String item,
            @Name(
                            value = "count",
                            description = "The number of times the given item should appear in the returned list.")
                    long count) {
        return Collections.nCopies((int) count, item);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.fill",
            deprecatedBy = "Cypher's list comprehension: `RETURN [i IN range(1, count) | item]`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns a `LIST<ANY>` with the given count of items.")
    public List<Object> fill(
            @Name(value = "item", description = "The item to be present in the returned list.") String item,
            @Name(
                            value = "count",
                            description = "The number of times the given item should appear in the returned list.")
                    long count) {
        return Collections.nCopies((int) count, item);
    }

    @UserFunction("apoc.coll.sortText")
    @Description("Sorts the given `LIST<STRING>` into ascending order.")
    public List<String> sortText(
            @Name(value = "coll", description = "The list of strings to be sorted.") List<String> coll,
            @Name(
                            value = "conf",
                            defaultValue = "{}",
                            description =
                                    "A map containing a single key `locale` to indicate which language to use when sorting the strings.")
                    Map<String, Object> conf) {
        if (conf == null) conf = Collections.emptyMap();
        if (coll == null || coll.isEmpty()) return Collections.emptyList();
        List<String> sorted = new ArrayList<>(coll);
        String localeAsStr = conf.getOrDefault("locale", "").toString();
        final Locale locale = !localeAsStr.isBlank() ? Locale.forLanguageTag(localeAsStr) : null;
        Collator collator = locale != null ? Collator.getInstance(locale) : Collator.getInstance();
        Collections.sort(sorted, collator);
        return sorted;
    }

    @UserFunction("apoc.coll.pairWithOffset")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns a `LIST<ANY>` of pairs defined by the offset.")
    public List<List<Object>> pairWithOffsetFnCypher5(
            @Name(value = "coll", description = "The list to create pairs from.") List<Object> values,
            @Name(value = "offset", description = "The offset to make each pair with from the given list.")
                    long offset) {
        return pairWithOffsetFn(values, offset);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.coll.pairWithOffset",
            deprecatedBy =
                    "Cyphers list comprehension: `RETURN [i IN range(0, size(list) - 1) | [list[i], list[i + offset]]] AS value`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns a `LIST<ANY>` of pairs defined by the offset.")
    public List<List<Object>> pairWithOffsetFn(
            @Name(value = "coll", description = "The list to create pairs from.") List<Object> values,
            @Name(value = "offset", description = "The offset to make each pair with from the given list.")
                    long offset) {
        if (values == null) return null;
        BiFunction<List<Object>, Long, Object> extract =
                (list, index) -> index < list.size() && index >= 0 ? list.get(index.intValue()) : null;
        final int length = Double.valueOf(Math.ceil((double) values.size() / Math.abs(offset)))
                .intValue();
        List<List<Object>> result = new ArrayList<>(length);
        for (long i = 0; i < values.size(); i++) {
            final List<Object> objects = asList(extract.apply(values, i), extract.apply(values, i + offset));
            result.add(objects);
        }
        return result;
    }

    public record PairWithOffsetListResult(@Description("The created pair.") List<Object> value) {}

    @Procedure("apoc.coll.pairWithOffset")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns a `LIST<ANY>` of pairs defined by the offset.")
    public Stream<PairWithOffsetListResult> pairWithOffsetCypher5(
            @Name(value = "coll", description = "The list to create pairs from.") List<Object> values,
            @Name(value = "offset", description = "The offset to make each pair with from the given list.")
                    long offset) {
        return pairWithOffsetFn(values, offset).stream().map(PairWithOffsetListResult::new);
    }

    @Deprecated
    @Procedure(
            name = "apoc.coll.pairWithOffset",
            deprecatedBy =
                    "Cypher's list comprehension: `RETURN [i IN range(0, size(list) - 1) | [list[i], list[i + offset]]] AS value`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns a `LIST<ANY>` of pairs defined by the offset.")
    public Stream<PairWithOffsetListResult> pairWithOffset(
            @Name(value = "coll", description = "The list to create pairs from.") List<Object> values,
            @Name(value = "offset", description = "The offset to make each pair with from the given list.")
                    long offset) {
        return pairWithOffsetFn(values, offset).stream().map(PairWithOffsetListResult::new);
    }
}
