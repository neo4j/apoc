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

import apoc.coll.SetBackedList;
import apoc.meta.Types;
import apoc.util.Util;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * @author mh
 * @since 29.05.16
 */
public class Convert {

    @UserFunction("apoc.convert.toMap")
    @Description("Converts the given value into a `MAP`.")
    public Map<String, Object> toMap(
            @Name(value = "map", description = "The value to convert into a map.") Object map) {

        if (map instanceof Entity) {
            return ((Entity) map).getAllProperties();
        } else if (map instanceof Map) {
            return (Map<String, Object>) map;
        } else {
            return null;
        }
    }

    @UserFunction("apoc.convert.toList")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Converts the given value into a `LIST<ANY>`.")
    public List<Object> toListCypher5(
            @Name(value = "value", description = "The value to convert into a list.") Object list) {
        return ConvertUtils.convertToList(list);
    }

    @Deprecated
    @UserFunction(
            name = "apoc.convert.toList",
            deprecatedBy = "Cypher's conversion functions, see the docs for more information.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Converts the given value into a `LIST<ANY>`.")
    public List<Object> toList(@Name(value = "value", description = "The value to convert into a list.") Object list) {
        return ConvertUtils.convertToList(list);
    }

    @UserFunction("apoc.convert.toNode")
    @Description("Converts the given value into a `NODE`.")
    public Node toNode(@Name(value = "node", description = "The value to convert into a node.") Object node) {
        return node instanceof Node ? (Node) node : null;
    }

    @UserFunction("apoc.convert.toRelationship")
    @Description("Converts the given value into a `RELATIONSHIP`.")
    public Relationship toRelationship(
            @Name(value = "rel", description = "The value to convert into a relationship.") Object relationship) {
        return relationship instanceof Relationship ? (Relationship) relationship : null;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> convertToList(Object list, Class<T> type) {
        List<Object> convertedList = ConvertUtils.convertToList(list);
        if (convertedList == null) {
            return null;
        }
        Stream<T> stream = null;
        Types varType = Types.of(type);
        switch (varType) {
            case INTEGER:
                stream = (Stream<T>) convertedList.stream().map(Util::toLong);
                break;
            case FLOAT:
                stream = (Stream<T>) convertedList.stream().map(Util::toDouble);
                break;
            case STRING:
                stream = (Stream<T>) convertedList.stream().map(Util::toString);
                break;
            case BOOLEAN:
                stream = (Stream<T>) convertedList.stream().map(Util::toBoolean);
                break;
            case NODE:
                stream = (Stream<T>) convertedList.stream().map(this::toNode);
                break;
            case RELATIONSHIP:
                stream = (Stream<T>) convertedList.stream().map(this::toRelationship);
                break;
            default:
                throw new RuntimeException("Supported types are: Integer, Float, String, Boolean, Node, Relationship");
        }
        return stream.collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    @UserFunction("apoc.convert.toSet")
    @Description("Converts the given value into a set represented in Cypher as a `LIST<ANY>`.")
    public List<Object> toSet(@Name(value = "list", description = "The list to convert into a set.") Object value) {
        List list = ConvertUtils.convertToList(value);
        return list == null ? null : new SetBackedList(new LinkedHashSet<>(list));
    }

    @UserFunction("apoc.convert.toNodeList")
    @Description("Converts the given value into a `LIST<NODE>`.")
    public List<Node> toNodeList(
            @Name(value = "list", description = "The value to covert into a node list.") Object list) {
        return convertToList(list, Node.class);
    }

    @UserFunction("apoc.convert.toRelationshipList")
    @Description("Converts the given value into a `LIST<RELATIONSHIP>`.")
    public List<Relationship> toRelationshipList(
            @Name(value = "relList", description = "The value to convert into a relationship list.") Object list) {
        return convertToList(list, Relationship.class);
    }
}
