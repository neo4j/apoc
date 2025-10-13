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
package apoc.schema;

import static org.neo4j.graphdb.Label.label;

import apoc.result.AssertSchemaResult;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

public class Schemas {

    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    @NotThreadSafe
    @Procedure(name = "apoc.schema.assert", mode = Mode.SCHEMA)
    @Description("Drops all other existing indexes and constraints when `dropExisting` is `true` (default is `true`).\n"
            + "Asserts at the end of the operation that the given indexes and unique constraints are there.")
    public Stream<AssertSchemaResult> schemaAssert(
            @Name(
                            value = "indexes",
                            description = "A map that pairs labels with lists of properties to create indexes from.")
                    Map<String, List<Object>> indexes,
            @Name(
                            value = "constraints",
                            description =
                                    "A map that pairs labels with lists of properties to create constraints from.")
                    Map<String, List<Object>> constraints,
            @Name(
                            value = "dropExisting",
                            defaultValue = "true",
                            description = "Whether or not to drop all other existing indexes and constraints.")
                    boolean dropExisting) {
        return Stream.concat(
                assertIndexes(indexes, dropExisting, Util.getCypherVersionString(procedureCallContext)).stream(),
                assertConstraints(constraints, dropExisting).stream());
    }

    @NotThreadSafe
    @UserFunction(name = "apoc.schema.node.indexExists")
    @Description(
            "Returns a `BOOLEAN` depending on whether or not an index exists for the given `NODE` label with the given property names.")
    public Boolean indexExistsOnNode(
            @Name(value = "labelName", description = "The node label to check for an index on.") String labelName,
            @Name(value = "propertyName", description = "The property names to check for an index on.")
                    List<String> propertyNames) {
        return indexExists(labelName, propertyNames);
    }

    @NotThreadSafe
    @UserFunction(value = "apoc.schema.relationship.indexExists")
    @Description(
            "Returns a `BOOLEAN` depending on whether or not an index exists for the given `RELATIONSHIP` type with the given property names.")
    public Boolean indexExistsOnRelationship(
            @Name(value = "type", description = "The relationship type to check for an index on.") String relName,
            @Name(value = "propertyName", description = "The property names to check for an index on.")
                    List<String> propertyNames) {
        return indexExistsForRelationship(relName, propertyNames);
    }

    @NotThreadSafe
    @UserFunction(name = "apoc.schema.node.constraintExists")
    @Description(
            "Returns a `BOOLEAN` depending on whether or not a constraint exists for the given `NODE` label with the given property names.")
    public Boolean constraintExistsOnNode(
            @Name(value = "labelName", description = "The node label to check for a constraint on.") String labelName,
            @Name(value = "propertyName", description = "The property names to check for a constraint on.")
                    List<String> propertyNames) {
        return constraintsExists(labelName, propertyNames);
    }

    @NotThreadSafe
    @UserFunction(name = "apoc.schema.relationship.constraintExists")
    @Description(
            "Returns a `BOOLEAN` depending on whether or not a constraint exists for the given `RELATIONSHIP` type with the given property names.")
    public Boolean constraintExistsOnRelationship(
            @Name(value = "type", description = "The relationship type to check for a constraint on.") String type,
            @Name(value = "propertyName", description = "The property names to check for a constraint on.")
                    List<String> propertyNames) {
        return constraintsExistsForRelationship(type, propertyNames);
    }

    public List<AssertSchemaResult> assertConstraints(Map<String, List<Object>> constraints0, boolean dropExisting) {
        Map<String, List<Object>> constraints = copyMapOfObjects(constraints0);
        List<AssertSchemaResult> result = new ArrayList<>(constraints.size());
        Schema schema = tx.schema();

        for (ConstraintDefinition definition : schema.getConstraints()) {
            ConstraintType constraintType = definition.getConstraintType();
            String label = Util.isRelationshipCategory(constraintType)
                    ? definition.getRelationshipType().name()
                    : definition.getLabel().name();
            AssertSchemaResult info = new AssertSchemaResult(label, Iterables.asList(definition.getPropertyKeys()));
            if (Util.constraintIsUnique(constraintType)) {
                info = info.unique();
            }
            if (!checkIfConstraintExists(label, constraints, info)) {
                if (dropExisting) {
                    definition.drop();
                    info.dropped();
                }
            }
            result.add(info);
        }

        for (Map.Entry<String, List<Object>> constraint : constraints.entrySet()) {
            for (Object key : constraint.getValue()) {
                if (key instanceof String) {
                    result.add(createUniqueConstraint(schema, constraint.getKey(), key.toString()));
                } else if (key instanceof List) {
                    result.add(createNodeKeyConstraint(constraint.getKey(), (List<Object>) key));
                }
            }
        }
        return result;
    }

    private boolean checkIfConstraintExists(
            String label, Map<String, List<Object>> constraints, AssertSchemaResult info) {
        if (constraints.containsKey(label)) {
            return constraints.get(label).removeIf(item -> {
                // when there is a constraint IS UNIQUE
                if (item instanceof String) {
                    return item.equals(info.key);
                    // when there is a constraint IS NODE KEY
                } else {
                    return info.keys.equals(item);
                }
            });
        }
        return false;
    }

    private AssertSchemaResult createNodeKeyConstraint(String lbl, List<Object> keys) {
        String keyProperties = keys.stream()
                .map(property -> String.format("n.`%s`", Util.sanitize(property.toString())))
                .collect(Collectors.joining(","));
        tx.execute(String.format(
                        "CREATE CONSTRAINT FOR (n:`%s`) REQUIRE (%s) IS NODE KEY", Util.sanitize(lbl), keyProperties))
                .close();
        List<String> keysToSting = keys.stream().map(Object::toString).collect(Collectors.toList());
        return new AssertSchemaResult(lbl, keysToSting).unique().created();
    }

    private AssertSchemaResult createUniqueConstraint(Schema schema, String lbl, String key) {
        schema.constraintFor(label(lbl)).assertPropertyIsUnique(key).create();
        return new AssertSchemaResult(lbl, key).unique().created();
    }

    public List<AssertSchemaResult> assertIndexes(
            Map<String, List<Object>> indexes0, boolean dropExisting, String cypherVersion)
            throws IllegalArgumentException {
        Schema schema = tx.schema();
        Map<String, List<Object>> indexes = copyMapOfObjects(indexes0);
        List<AssertSchemaResult> result = new ArrayList<>(indexes.size());

        for (IndexDefinition definition : Util.getIndexes(tx)) {
            if (definition.getIndexType() == IndexType.LOOKUP) continue;
            // Don't drop vector indexes
            if (definition.getIndexType() == IndexType.VECTOR) continue;
            if (definition.isConstraintIndex()) continue;
            if (definition.isMultiTokenIndex()) continue;

            Object label = getLabelForAssert(definition, definition.isNodeIndex());
            List<String> keys = new ArrayList<>();
            definition.getPropertyKeys().forEach(keys::add);

            AssertSchemaResult info = new AssertSchemaResult(label, keys);

            final boolean included = Optional.ofNullable(indexes.get(label))
                    .map(lbl -> {
                        if (keys.size() > 1) {
                            return lbl.remove(keys);
                        }
                        if (keys.size() == 1) {
                            return lbl.remove(keys.get(0));
                        }
                        // todo - it shouldn't be needed. only LOOKUP indexes, absent in 4.2 and previous and filtered
                        // for 4.3+, can be without keys
                        throw new IllegalArgumentException("Label given with no keys.");
                    })
                    .orElse(false);

            if (dropExisting && !included) {
                definition.drop();
                info.dropped();
            }

            result.add(info);
        }

        for (Map.Entry<String, List<Object>> index : indexes.entrySet()) {
            for (Object key : index.getValue()) {
                if (key instanceof String) {
                    result.add(createSinglePropertyIndex(schema, index.getKey(), (String) key));
                } else if (key instanceof List) {
                    result.add(createCompoundIndex(index.getKey(), (List<String>) key, cypherVersion));
                }
            }
        }
        return result;
    }

    private Object getLabelForAssert(IndexDefinition definition, boolean nodeIndex) {
        if (nodeIndex) {
            return definition.isMultiTokenIndex()
                    ? Iterables.stream(definition.getLabels()).map(Label::name).collect(Collectors.toList())
                    : Iterables.single(definition.getLabels()).name();
        } else {
            return definition.isMultiTokenIndex()
                    ? Iterables.stream(definition.getRelationshipTypes())
                            .map(RelationshipType::name)
                            .collect(Collectors.toList())
                    : Iterables.single(definition.getRelationshipTypes()).name();
        }
    }

    private AssertSchemaResult createSinglePropertyIndex(Schema schema, String lbl, String key) {
        schema.indexFor(label(lbl)).on(key).create();
        return new AssertSchemaResult(lbl, key).created();
    }

    private AssertSchemaResult createCompoundIndex(String label, List<String> keys, String cypherVersion) {
        List<String> backTickedKeys = new ArrayList<>();
        keys.forEach(key -> backTickedKeys.add(String.format("n.`%s`", Util.sanitize(key))));

        tx.execute(String.format(
                        "CYPHER %s CREATE INDEX FOR (n:`%s`) ON (%s)",
                        cypherVersion, Util.sanitize(label), String.join(",", backTickedKeys)))
                .close();
        return new AssertSchemaResult(label, keys).created();
    }

    private Map<String, List<Object>> copyMapOfObjects(Map<String, List<Object>> input) {
        if (input == null) {
            return Collections.emptyMap();
        }

        HashMap<String, List<Object>> result = new HashMap<>(input.size());

        input.forEach((k, v) -> result.put(k, new ArrayList<>(v)));
        return result;
    }

    /**
     * Checks if an index exists for a given label and a list of properties
     * This method checks for index on nodes
     *
     * @param labelName
     * @param propertyNames
     * @return true if the index exists otherwise it returns false
     */
    private Boolean indexExists(String labelName, List<String> propertyNames) {
        Iterable<IndexDefinition> nodeIndexes = Util.getIndexes(tx, label(labelName));
        return isIndexExistent(propertyNames, nodeIndexes);
    }

    private Boolean indexExistsForRelationship(String relName, List<String> propertyNames) {
        Iterable<IndexDefinition> relIndexes = Util.getIndexes(tx, RelationshipType.withName(relName));
        return isIndexExistent(propertyNames, relIndexes);
    }

    private Boolean isIndexExistent(List<String> propertyNames, Iterable<IndexDefinition> indexes) {
        for (IndexDefinition indexDefinition : indexes) {
            List<String> properties = Iterables.asList(indexDefinition.getPropertyKeys());

            if (properties.equals(propertyNames)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if a constraint exists for a given label and a list of properties
     * This method checks for constraints on node
     *
     * @param labelName
     * @param propertyNames
     * @return true if the constraint exists otherwise it returns false
     */
    private Boolean constraintsExists(String labelName, List<String> propertyNames) {
        Schema schema = tx.schema();

        for (ConstraintDefinition constraintDefinition :
                Iterables.asList(schema.getConstraints(Label.label(labelName)))) {
            List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());

            if (properties.equals(propertyNames)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a constraint exists for a given type and a list of properties
     * This method checks for constraints on relationships
     *
     * @param type
     * @param propertyNames
     * @return true if the constraint exists otherwise it returns false
     */
    private Boolean constraintsExistsForRelationship(String type, List<String> propertyNames) {
        Schema schema = tx.schema();

        for (ConstraintDefinition constraintDefinition :
                Iterables.asList(schema.getConstraints(RelationshipType.withName(type)))) {
            List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());

            if (properties.equals(propertyNames)) {
                return true;
            }
        }

        return false;
    }
}
