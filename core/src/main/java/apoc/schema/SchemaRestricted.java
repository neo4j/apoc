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

import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

import apoc.result.IndexConstraintNodeInfo;
import apoc.result.IndexConstraintRelationshipInfo;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.token.api.TokenConstants;

public class SchemaRestricted {
    private static final String IDX_NOT_FOUND = "NOT_FOUND";

    @Context
    public Transaction tx;

    @Context
    public KernelTransaction ktx;

    @Context
    public ProcedureCallContext procedureCallContext;

    @NotThreadSafe
    @Procedure(name = "apoc.schema.nodes", mode = Mode.SCHEMA)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns all indexes and constraints information for all `NODE` labels in the database.\n"
            + "It is possible to define a set of labels to include or exclude in the config parameters.")
    public Stream<IndexConstraintNodeInfo> schemaNodesCypher5(
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
            {
                labels :: LIST<STRING>,
                excludeLabels :: LIST<STRING>,
                relationships :: LIST<STRING>,
                excludeRelationships :: LIST<STRING>
            }
            """)
                    Map<String, Object> config) {
        return indexesAndConstraintsForNode(config, false);
    }

    @NotThreadSafe
    @Procedure(name = "apoc.schema.nodes", mode = Mode.SCHEMA)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns all indexes and constraints information for all `NODE` labels in the database.\n"
            + "It is possible to define a set of labels to include or exclude in the config parameters.")
    public Stream<IndexConstraintNodeInfo> nodes(
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
            {
                labels :: LIST<STRING>,
                excludeLabels :: LIST<STRING>,
                relationships :: LIST<STRING>,
                excludeRelationships :: LIST<STRING>
            }
            """)
                    Map<String, Object> config) {
        return indexesAndConstraintsForNode(config, true);
    }

    @NotThreadSafe
    @Procedure(name = "apoc.schema.relationships", mode = Mode.SCHEMA)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the indexes and constraints information for all the relationship types in the database.\n"
            + "It is possible to define a set of relationship types to include or exclude in the config parameters.")
    public Stream<IndexConstraintRelationshipInfo> schemaRelationshipsCypher5(
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
            {
                labels :: LIST<STRING>,
                excludeLabels :: LIST<STRING>,
                relationships :: LIST<STRING>,
                excludeRelationships :: LIST<STRING>
            }
            """)
                    Map<String, Object> config) {
        return indexesAndConstraintsForRelationships(config, false);
    }

    @NotThreadSafe
    @Procedure(name = "apoc.schema.relationships", mode = Mode.SCHEMA)
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the indexes and constraints information for all the relationship types in the database.\n"
            + "It is possible to define a set of relationship types to include or exclude in the config parameters.")
    public Stream<IndexConstraintRelationshipInfo> relationships(
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
            {
                labels :: LIST<STRING>,
                excludeLabels :: LIST<STRING>,
                relationships :: LIST<STRING>,
                excludeRelationships :: LIST<STRING>
            }
            """)
                    Map<String, Object> config) {
        return indexesAndConstraintsForRelationships(config, true);
    }

    /**
     * Collects indexes and constraints for nodes
     *
     * @return
     */
    private Stream<IndexConstraintNodeInfo> indexesAndConstraintsForNode(
            Map<String, Object> config, Boolean useStoredName) {
        Schema schema = tx.schema();

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeLabels = schemaConfig.getLabels();
        Set<String> excludeLabels = schemaConfig.getExcludeLabels();

        try (Statement ignore = ktx.acquireStatement()) {
            TokenRead tokenRead = ktx.tokenRead();

            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<IndexDescriptor> indexesIterator;
            Iterable<ConstraintDefinition> constraintsIterator;
            final Predicate<ConstraintDefinition> isNodeConstraint =
                    constraintDefinition -> Util.isNodeCategory(constraintDefinition.getConstraintType());

            if (includeLabels.isEmpty()) {

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();

                indexesIterator = getIndexesFromSchema(
                        allIndex,
                        index -> index.schema().entityType().equals(EntityType.NODE)
                                && Arrays.stream(index.schema().getEntityTokenIds())
                                        .noneMatch(id -> {
                                            try {
                                                return excludeLabels.contains(tokenRead.nodeLabelName(id));
                                            } catch (LabelNotFoundKernelException e) {
                                                return false;
                                            }
                                        }));

                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
                        .filter(isNodeConstraint)
                        .filter(constraint ->
                                !excludeLabels.contains(constraint.getLabel().name()))
                        .collect(Collectors.toList());
            } else {
                constraintsIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<ConstraintDefinition> constraintsForType =
                                    schema.getConstraints(Label.label(label));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isNodeConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeLabels.stream()
                        .filter(label -> !excludeLabels.contains(label) && tokenRead.nodeLabel(label) != -1)
                        .flatMap(label -> {
                            Iterable<IndexDescriptor> indexesForLabel =
                                    () -> schemaRead.indexesGetForLabel(tokenRead.nodeLabel(label));
                            return StreamSupport.stream(indexesForLabel.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            }

            Stream<IndexConstraintNodeInfo> constraintNodeInfoStream = StreamSupport.stream(
                            constraintsIterator.spliterator(), false)
                    .map(constraintDescriptor ->
                            nodeInfoFromConstraintDefinition(constraintDescriptor, tokenRead, useStoredName))
                    .sorted(Comparator.comparing(i -> i.label.toString()));

            Stream<IndexConstraintNodeInfo> indexNodeInfoStream = StreamSupport.stream(
                            indexesIterator.spliterator(), false)
                    .map(indexDescriptor ->
                            this.nodeInfoFromIndexDefinition(indexDescriptor, schemaRead, tokenRead, useStoredName))
                    .sorted(Comparator.comparing(i -> i.label.toString()));

            return Stream.of(constraintNodeInfoStream, indexNodeInfoStream).flatMap(e -> e);
        }
    }

    private List<IndexDescriptor> getIndexesFromSchema(
            Iterator<IndexDescriptor> allIndex, Predicate<IndexDescriptor> indexDescriptorPredicate) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(allIndex, Spliterator.ORDERED), false)
                .filter(indexDescriptorPredicate)
                .collect(Collectors.toList());
    }

    /**
     * Collects constraints for relationships
     *
     * @return
     */
    private Stream<IndexConstraintRelationshipInfo> indexesAndConstraintsForRelationships(
            Map<String, Object> config, Boolean useStoredName) {
        Schema schema = tx.schema();

        SchemaConfig schemaConfig = new SchemaConfig(config);
        Set<String> includeRelationships = schemaConfig.getRelationships();
        Set<String> excludeRelationships = schemaConfig.getExcludeRelationships();

        try (Statement ignore = ktx.acquireStatement()) {
            TokenRead tokenRead = ktx.tokenRead();
            SchemaRead schemaRead = ktx.schemaRead();
            Iterable<ConstraintDefinition> constraintsIterator;
            Iterable<IndexDescriptor> indexesIterator;

            final Predicate<ConstraintDefinition> isRelConstraint =
                    constraintDefinition -> Util.isRelationshipCategory(constraintDefinition.getConstraintType());

            if (!includeRelationships.isEmpty()) {
                constraintsIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type)
                                && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<ConstraintDefinition> constraintsForType =
                                    schema.getConstraints(RelationshipType.withName(type));
                            return StreamSupport.stream(constraintsForType.spliterator(), false)
                                    .filter(isRelConstraint);
                        })
                        .collect(Collectors.toList());

                indexesIterator = includeRelationships.stream()
                        .filter(type -> !excludeRelationships.contains(type)
                                && tokenRead.relationshipType(type) != TokenConstants.NO_TOKEN)
                        .flatMap(type -> {
                            Iterable<IndexDescriptor> indexesForRelType =
                                    () -> schemaRead.indexesGetForRelationshipType(tokenRead.relationshipType(type));
                            return StreamSupport.stream(indexesForRelType.spliterator(), false);
                        })
                        .collect(Collectors.toList());
            } else {
                Iterable<ConstraintDefinition> allConstraints = schema.getConstraints();
                constraintsIterator = StreamSupport.stream(allConstraints.spliterator(), false)
                        .filter(isRelConstraint)
                        .filter(constraint -> !excludeRelationships.contains(
                                constraint.getRelationshipType().name()))
                        .collect(Collectors.toList());

                Iterator<IndexDescriptor> allIndex = schemaRead.indexesGetAll();
                indexesIterator = getIndexesFromSchema(
                        allIndex,
                        index -> index.schema().entityType().equals(EntityType.RELATIONSHIP)
                                && Arrays.stream(index.schema().getEntityTokenIds())
                                        .noneMatch(id ->
                                                excludeRelationships.contains(tokenRead.relationshipTypeGetName(id))));
            }

            Stream<IndexConstraintRelationshipInfo> constraintRelationshipInfoStream = StreamSupport.stream(
                            constraintsIterator.spliterator(), false)
                    .map(c -> relationshipInfoFromConstraintDefinition(c, useStoredName));

            Stream<IndexConstraintRelationshipInfo> indexRelationshipInfoStream = StreamSupport.stream(
                            indexesIterator.spliterator(), false)
                    .map(index -> relationshipInfoFromIndexDescription(index, tokenRead, schemaRead, useStoredName));

            return Stream.of(constraintRelationshipInfoStream, indexRelationshipInfoStream)
                    .flatMap(e -> e);
        }
    }

    /**
     * ConstraintInfo info from ConstraintDefinition
     *
     * @param constraintDefinition
     * @param tokens
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromConstraintDefinition(
            ConstraintDefinition constraintDefinition, TokenNameLookup tokens, Boolean useStoredName) {
        String labelName = constraintDefinition.getLabel().name();
        List<String> properties = Iterables.asList(constraintDefinition.getPropertyKeys());
        return new IndexConstraintNodeInfo(
                // Pretty print for index name
                useStoredName
                        ? constraintDefinition.getName()
                        : String.format(":%s(%s)", labelName, StringUtils.join(properties, ",")),
                labelName,
                properties,
                StringUtils.EMPTY,
                constraintDefinition.getConstraintType().name(),
                "NO FAILURE",
                0,
                0,
                0,
                nodeConstraintCypher5Compatibility(
                        ktx.schemaRead()
                                .constraintGetForName(constraintDefinition.getName())
                                .userDescription(tokens),
                        useStoredName));
    }

    private String nodeConstraintCypher5Compatibility(String userDescription, Boolean useStoredName) {
        if (useStoredName) {
            return userDescription;
        } else {
            // Revert to old description on Cypher 5 for backwards compatibility.
            return userDescription.replace("'NODE PROPERTY UNIQUENESS'", "'UNIQUENESS'");
        }
    }

    /**
     * Index info from IndexDefinition
     *
     * @param indexDescriptor
     * @param schemaRead
     * @param tokens
     * @return
     */
    private IndexConstraintNodeInfo nodeInfoFromIndexDefinition(
            IndexDescriptor indexDescriptor, SchemaRead schemaRead, TokenNameLookup tokens, Boolean useStoredName) {
        int[] labelIds = indexDescriptor.schema().getEntityTokenIds();
        int length = labelIds.length;
        final Object labelName;
        if (length == 0) {
            labelName = TOKEN_LABEL;
        } else {
            final List<String> labels = IntStream.of(labelIds)
                    .mapToObj(tokens::labelGetName)
                    .sorted()
                    .collect(Collectors.toList());
            labelName = labels.size() > 1 ? labels : labels.get(0);
        }
        // to handle LOOKUP indexes
        List<String> properties = IntStream.of(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());

        // Pretty print for index name
        final String schemaInfoName = getSchemaInfoName(labelName, properties);
        final String userDescription = indexDescriptor.userDescription(tokens);
        try {
            return new IndexConstraintNodeInfo(
                    useStoredName ? indexDescriptor.getName() : schemaInfoName,
                    labelName,
                    properties,
                    schemaRead.indexGetState(indexDescriptor).toString(),
                    getIndexType(indexDescriptor),
                    schemaRead.indexGetState(indexDescriptor).equals(InternalIndexState.FAILED)
                            ? schemaRead.indexGetFailure(indexDescriptor)
                            : "NO FAILURE",
                    getPopulationProgress(indexDescriptor, schemaRead),
                    schemaRead.indexSize(indexDescriptor),
                    schemaRead.indexUniqueValuesSelectivity(indexDescriptor),
                    userDescription);
        } catch (IndexNotFoundKernelException e) {
            return new IndexConstraintNodeInfo(
                    schemaInfoName,
                    labelName,
                    properties,
                    IDX_NOT_FOUND,
                    getIndexType(indexDescriptor),
                    IDX_NOT_FOUND,
                    0,
                    0,
                    0,
                    userDescription);
        }
    }

    private IndexConstraintRelationshipInfo relationshipInfoFromIndexDescription(
            IndexDescriptor indexDescriptor, TokenNameLookup tokens, SchemaRead schemaRead, Boolean useStoredName) {
        int[] relIds = indexDescriptor.schema().getEntityTokenIds();
        int length = relIds.length;
        // to handle LOOKUP indexes
        final Object relName;
        if (length == 0) {
            relName = TOKEN_REL_TYPE;
        } else {
            final List<String> rels = IntStream.of(relIds)
                    .mapToObj(tokens::relationshipTypeGetName)
                    .sorted()
                    .collect(Collectors.toList());
            relName = rels.size() > 1 ? rels : rels.get(0);
        }
        final List<String> properties = Arrays.stream(indexDescriptor.schema().getPropertyIds())
                .mapToObj(tokens::propertyKeyGetName)
                .collect(Collectors.toList());

        // Pretty print for index name
        final String name = useStoredName ? indexDescriptor.getName() : getSchemaInfoName(relName, properties);
        final String schemaType = getIndexType(indexDescriptor);

        String indexStatus;
        try {
            indexStatus = schemaRead.indexGetState(indexDescriptor).toString();
        } catch (IndexNotFoundKernelException e) {
            indexStatus = IDX_NOT_FOUND;
        }

        return new IndexConstraintRelationshipInfo(name, schemaType, properties, indexStatus, relName);
    }

    /**
     * Constraint info from ConstraintDefinition for relationships
     *
     * @param constraintDefinition
     * @return
     */
    private IndexConstraintRelationshipInfo relationshipInfoFromConstraintDefinition(
            ConstraintDefinition constraintDefinition, Boolean useStoredName) {
        return new IndexConstraintRelationshipInfo(
                useStoredName
                        ? constraintDefinition.getName()
                        : String.format("CONSTRAINT %s", constraintDefinition.toString()),
                constraintDefinition.getConstraintType().name(),
                Iterables.asList(constraintDefinition.getPropertyKeys()),
                "",
                constraintDefinition.getRelationshipType().name());
    }

    private static String getIndexType(IndexDescriptor indexDescriptor) {
        return indexDescriptor.getIndexType().name();
    }

    private String getSchemaInfoName(Object labelOrType, List<String> properties) {
        final String labelOrTypeAsString =
                labelOrType instanceof String ? (String) labelOrType : StringUtils.join(labelOrType, ",");
        return String.format(":%s(%s)", labelOrTypeAsString, StringUtils.join(properties, ","));
    }

    private long getPopulationProgress(IndexDescriptor indexDescriptor, SchemaRead schemaRead)
            throws IndexNotFoundKernelException {
        PopulationProgress populationProgress = schemaRead.indexGetPopulationProgress(indexDescriptor);
        // when the index is failed the getTotal() is equal to 0
        long populationTotal = populationProgress.getTotal();
        if (populationTotal == 0) {
            return 0L;
        }
        return populationProgress.getCompleted() / populationTotal * 100;
    }
}
