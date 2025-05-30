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
package apoc.export.util;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

import apoc.util.JsonUtil;
import apoc.util.Util;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;

/**
 * @author mh
 * @since 23.02.16
 */
public class FormatUtils {

    public static String formatNumber(Number value) {
        if (value == null) return null;
        return value.toString();
    }

    public static String formatString(Object value) {
        return "\""
                + String.valueOf(value)
                        .replaceAll("\\\\", "\\\\\\\\")
                        .replaceAll("\n", "\\\\n")
                        .replaceAll("\r", "\\\\r")
                        .replaceAll("\t", "\\\\t")
                        .replaceAll("\"", "\\\\\"")
                + "\"";
    }

    public static String joinLabels(Node node, String delimiter) {
        return getLabelsAsStream(node).collect(Collectors.joining(delimiter));
    }

    public static Map<String, Object> toMap(Entity pc) {
        if (pc == null) return null;
        if (pc instanceof Node) {
            Node node = (Node) pc;
            return map("id", node.getId(), "labels", labelStrings(node), "properties", pc.getAllProperties());
        }
        if (pc instanceof Relationship) {
            Relationship rel = (Relationship) pc;
            return map(
                    "id",
                    rel.getId(),
                    "type",
                    rel.getType().name(),
                    "start",
                    rel.getStartNode().getId(),
                    "end",
                    rel.getEndNode().getId(),
                    "properties",
                    pc.getAllProperties());
        }
        throw new RuntimeException("Invalid graph element " + pc);
    }

    public static String toString(Object value, Function<String, String> escapeFunction) {
        return toString(value, escapeFunction, false);
    }

    public static String toString(Object value, Function<String, String> escapeFunction, boolean keepNulls) {
        if (value == null && !keepNulls) return "";
        if (value == null) return null;
        if (value instanceof Path) {
            return toString(StreamSupport.stream(((Path) value).spliterator(), false)
                    .map(FormatUtils::toMap)
                    .collect(Collectors.toList()));
        }
        if (value instanceof Entity) {
            return Util.toJson(toMap((Entity) value)); // todo id, label, type ?
        }
        if (value.getClass().isArray() || value instanceof Iterable || value instanceof Map) {
            return Util.toJson(value);
        }
        if (value instanceof Number) {
            return formatNumber((Number) value);
        }
        if (value instanceof Point) {
            return formatPoint((Point) value);
        }
        return escapeFunction.apply(value.toString());
    }

    public static String toString(Object value) {
        return toString(value, Function.identity());
    }

    public static String toString(Object value, boolean keepNulls) {
        return toString(value, Function.identity(), keepNulls);
    }

    public static String toXmlString(Object value) {
        return toString(value, FormatUtils::removeInvalidXMLCharacters);
    }

    public static String removeInvalidXMLCharacters(String value) {
        final String invalidChars = "[\\u0000-\\u0008\\u000B\\u000C\\u000E-\\u001F\\ufffe-\\uffff]";
        return value.replaceAll(invalidChars, "");
    }

    public static String formatPoint(Point value) {
        try {
            return JsonUtil.OBJECT_MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getLabelsSorted(Node node) {
        return getLabelsAsStream(node).collect(Collectors.toList());
    }

    private static Stream<String> getLabelsAsStream(Node node) {
        return StreamSupport.stream(node.getLabels().spliterator(), false)
                .map(Label::name)
                .sorted();
    }
}
