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
package apoc.refactor.rename;

import apoc.Pools;
import apoc.periodic.BatchAndTotalResult;
import apoc.periodic.Periodic;
import apoc.util.MapUtil;
import apoc.util.Util;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

/**
 * @author AgileLARUS
 *
 * @since 03-04-2017
 */
public class Rename {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Transaction transaction;

    @Context
    public Pools pools;

    @Context
    public Transaction tx;

    /**
     * Rename the Label of a node by creating a new one and deleting the old.
     */
    @Procedure(name = "apoc.refactor.rename.label", mode = Mode.WRITE)
    @Description(
            "Renames the given label from `oldLabel` to `newLabel` for all `NODE` values.\n"
                    + "If a `LIST<NODE>` is provided, the renaming is applied to the `NODE` values within this `LIST<NODE>` only.")
    public Stream<BatchAndTotalResultWithInfo> label(
            @Name(value = "oldLabel", description = "The label to rename.") String oldLabel,
            @Name(value = "newLabel", description = "The new name to give the label.") String newLabel,
            @Name(
                            value = "nodes",
                            defaultValue = "[]",
                            description =
                                    "The nodes to apply the new name to. If this list is empty, all nodes with the old label will be renamed.")
                    List<Node> nodes) {
        nodes = nodes.stream().map(n -> Util.rebind(tx, n)).collect(Collectors.toList());
        oldLabel = Util.sanitize(oldLabel);
        newLabel = Util.sanitize(newLabel);
        String cypherIterate = nodes != null && !nodes.isEmpty()
                ? "UNWIND $nodes AS n WITH n WHERE n:`" + oldLabel + "` RETURN n"
                : "MATCH (n:`" + oldLabel + "`) RETURN n";
        String cypherAction = "REMOVE n:`" + oldLabel + "` SET n:`" + newLabel + "`";
        Map<String, Object> parameters = MapUtil.map(
                "batchSize", 100000, "parallel", true, "iterateList", true, "params", MapUtil.map("nodes", nodes));
        return getResultOfBatchAndTotalWithInfo(
                newPeriodic().iterate(cypherIterate, cypherAction, parameters), oldLabel, null, null);
    }

    /**
     * Rename the Relationship Type by creating a new one and deleting the old.
     */
    @Procedure(name = "apoc.refactor.rename.type", mode = Mode.WRITE)
    @Description(
            "Renames all `RELATIONSHIP` values with type `oldType` to `newType`.\n"
                    + "If a `LIST<RELATIONSHIP>` is provided, the renaming is applied to the `RELATIONSHIP` values within this `LIST<RELATIONSHIP>` only.")
    public Stream<BatchAndTotalResultWithInfo> type(
            @Name(value = "oldType", description = "The type to rename.") String oldType,
            @Name(value = "newType", description = "The new type for the relationship.") String newType,
            @Name(
                            value = "rels",
                            defaultValue = "[]",
                            description =
                                    "The relationships to apply the new name to. If this list is empty, all relationships with the old type will be renamed.")
                    List<Relationship> rels,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        batchSize = 100000 :: INTEGER,
                        concurrency :: INTEGER,
                        retries = 0 :: INTEGER,
                        parallel = true :: BOOLEAN,
                        batchMode = "BATCH" :: STRING
                    }
                    """)
                    Map<String, Object> config) {
        rels = rels.stream().map(r -> Util.rebind(tx, r)).collect(Collectors.toList());
        newType = Util.sanitize(newType);
        oldType = Util.sanitize(oldType);
        String cypherIterate = rels != null && !rels.isEmpty()
                ? "UNWIND $rels AS oldRel WITH oldRel WHERE type(oldRel)=\"" + oldType
                        + "\" RETURN oldRel,startNode(oldRel) as a,endNode(oldRel) as b"
                : "MATCH (a)-[oldRel:`" + oldType + "`]->(b) RETURN oldRel,a,b";
        String cypherAction = "CREATE(a)-[newRel:`" + newType + "`]->(b)" + "SET newRel+=oldRel DELETE oldRel";
        final Map<String, Object> params = MapUtil.map("rels", rels);
        Map<String, Object> parameters = getPeriodicConfig(config, params);
        return getResultOfBatchAndTotalWithInfo(
                newPeriodic().iterate(cypherIterate, cypherAction, parameters), null, oldType, null);
    }

    private Map<String, Object> getPeriodicConfig(Map<String, Object> config, Map<String, Object> params) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        if (params == null) {
            params = Collections.emptyMap();
        }
        final int batchSize = Util.toInteger(config.getOrDefault("batchSize", 100000));
        final int concurrency = Util.toInteger(
                config.getOrDefault("concurrency", Runtime.getRuntime().availableProcessors()));
        final int retries = Util.toInteger(config.getOrDefault("retries", 0));
        final boolean parallel = Util.toBoolean(config.getOrDefault("parallel", true));
        final String batchMode = config.getOrDefault("batchMode", "BATCH").toString();
        return MapUtil.map(
                "batchSize",
                batchSize,
                "retries",
                retries,
                "parallel",
                parallel,
                "batchMode",
                batchMode,
                "concurrency",
                concurrency,
                "params",
                params);
    }

    /**
     * Rename property of a node by creating a new one and deleting the old.
     */
    @Procedure(name = "apoc.refactor.rename.nodeProperty", mode = Mode.WRITE)
    @Description(
            "Renames the given property from `oldName` to `newName` for all `NODE` values.\n"
                    + "If a `LIST<NODE>` is provided, the renaming is applied to the `NODE` values within this `LIST<NODE>` only.")
    public Stream<BatchAndTotalResultWithInfo> nodeProperty(
            @Name(value = "oldName", description = "The property to rename.") String oldName,
            @Name(value = "newName", description = "The new name to give the property.") String newName,
            @Name(
                            value = "nodes",
                            defaultValue = "[]",
                            description =
                                    "The nodes to apply the new name to. If this list is empty, all nodes with the old property name will be renamed.")
                    List<Node> nodes,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        batchSize = 100000 :: INTEGER,
                        concurrency :: INTEGER,
                        retries = 0 :: INTEGER,
                        parallel = true :: BOOLEAN,
                        batchMode = "BATCH" :: STRING
                    }
                    """)
                    Map<String, Object> config) {
        nodes = nodes.stream().map(n -> Util.rebind(tx, n)).collect(Collectors.toList());
        oldName = Util.sanitize(oldName);
        newName = Util.sanitize(newName);
        String cypherIterate = nodes != null && !nodes.isEmpty()
                ? "UNWIND $nodes AS n WITH n WHERE n.`" + oldName + "` IS NOT NULL return n"
                : "match (n) where n.`" + oldName + "` IS NOT NULL return n";
        String cypherAction =
                "WITH n, n. `" + oldName + "` AS propVal REMOVE n.`" + oldName + "` SET n.`" + newName + "` = propVal";
        final Map<String, Object> params = MapUtil.map("nodes", nodes);
        Map<String, Object> parameters = getPeriodicConfig(config, params);
        return getResultOfBatchAndTotalWithInfo(
                newPeriodic().iterate(cypherIterate, cypherAction, parameters), null, null, oldName);
    }

    /**
     * Rename property of a relationship by creating a new one and deleting the old.
     */
    @Procedure(name = "apoc.refactor.rename.typeProperty", mode = Mode.WRITE)
    @Description(
            "Renames the given property from `oldName` to `newName` for all `RELATIONSHIP` values.\n"
                    + "If a `LIST<RELATIONSHIP>` is provided, the renaming is applied to the `RELATIONSHIP` values within this `LIST<RELATIONSHIP>` only.")
    public Stream<BatchAndTotalResultWithInfo> typeProperty(
            @Name(value = "oldName", description = "The property to rename.") String oldName,
            @Name(value = "newName", description = "The new name to give the property.") String newName,
            @Name(
                            value = "rels",
                            defaultValue = "[]",
                            description =
                                    "The relationships to apply the new name to. If this list is empty, all relationships with the old properties name will be renamed.")
                    List<Relationship> rels,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        batchSize = 100000 :: INTEGER,
                        concurrency :: INTEGER,
                        retries = 0 :: INTEGER,
                        parallel = true :: BOOLEAN,
                        batchMode = "BATCH" :: STRING
                    }
                    """)
                    Map<String, Object> config) {
        rels = rels.stream().map(r -> Util.rebind(tx, r)).collect(Collectors.toList());
        newName = Util.sanitize(newName);
        oldName = Util.sanitize(oldName);
        String cypherIterate = rels != null && !rels.isEmpty()
                ? "UNWIND $rels AS r WITH r WHERE r.`" + oldName + "` IS NOT NULL return r"
                : "match ()-[r]->() where r.`" + oldName + "` IS NOT NULL return r";
        String cypherAction =
                "WITH r, r. `" + oldName + "` AS propVal REMOVE r.`" + oldName + "` SET r.`" + newName + "`= propVal";
        final Map<String, Object> params = MapUtil.map("rels", rels);
        Map<String, Object> parameters = getPeriodicConfig(config, params);
        return getResultOfBatchAndTotalWithInfo(
                newPeriodic().iterate(cypherIterate, cypherAction, parameters), null, null, oldName);
    }

    /*
     * create a properly initialized Periodic instance by setting all the required @Context attributes
     */
    private Periodic newPeriodic() {
        Periodic periodic = new Periodic();
        periodic.db = this.db;
        periodic.log = this.log;
        periodic.terminationGuard = this.terminationGuard;
        periodic.pools = this.pools;
        periodic.tx = this.tx;
        return periodic;
    }

    /*
     * Create the response for rename apoc with impacted constraints and indexes
     */
    private Stream<BatchAndTotalResultWithInfo> getResultOfBatchAndTotalWithInfo(
            Stream<BatchAndTotalResult> iterate, String label, String rel, String prop) {
        List<String> constraints = new ArrayList<>();
        List<String> indexes = new ArrayList<>();

        if (label != null) {
            Iterable<ConstraintDefinition> constraintsForLabel =
                    transaction.schema().getConstraints(Label.label(label));
            constraintsForLabel.forEach((c) -> {
                constraints.add(c.toString());
            });
            Iterable<IndexDefinition> idxs = Util.getIndexes(transaction, Label.label(label));
            idxs.forEach((i) -> {
                indexes.add(i.toString());
            });
        }
        if (rel != null) {
            Iterable<ConstraintDefinition> constraintsForRel =
                    transaction.schema().getConstraints(RelationshipType.withName(rel));
            constraintsForRel.forEach((c) -> {
                constraints.add(c.toString());
            });
        }
        if (prop != null) {
            Iterable<ConstraintDefinition> constraintsForProps =
                    transaction.schema().getConstraints();
            constraintsForProps.forEach((c) -> {
                c.getPropertyKeys().forEach((p) -> {
                    if (p.equals(prop)) {
                        constraints.add(c.toString());
                    }
                });
            });
            Iterable<IndexDefinition> idxs = Util.getIndexes(transaction);
            idxs.forEach((i) -> {
                i.getPropertyKeys().forEach((p) -> {
                    if (p.equals(prop)) {
                        indexes.add(i.toString());
                    }
                });
            });
        }
        Optional<BatchAndTotalResult> targetLongList = iterate.findFirst();
        BatchAndTotalResultWithInfo result = new BatchAndTotalResultWithInfo(targetLongList, constraints, indexes);
        return Stream.of(result);
    }

    public class BatchAndTotalResultWithInfo {
        @Description("The number of batches the operation was run in.")
        public long batches;

        @Description("The total number of renamings performed.")
        public long total;

        @Description("The time taken to complete the operation.")
        public long timeTaken;

        @Description("The total number of committed operations.")
        public long committedOperations;

        @Description("The total number of failed operations.")
        public long failedOperations;

        @Description("The total number of failed batches.")
        public long failedBatches;

        @Description("The total number of retries.")
        public long retries;

        @Description("The collected error messages.")
        public Map<String, Long> errorMessages;

        @Description(
                """
                {
                     total :: INTEGER,
                     failed :: INTEGER,
                     committed :: INTEGER,
                     errors :: MAP
                }
                """)
        public Map<String, Object> batch;

        @Description(
                """
                {
                     total :: INTEGER,
                     failed :: INTEGER,
                     committed :: INTEGER,
                     errors :: MAP
                }
                """)
        public Map<String, Object> operations;

        @Description("Constraints associated with the given label or type.")
        public List<String> constraints;

        @Description("Indexes associated with the given label or type.")
        public List<String> indexes;

        public BatchAndTotalResultWithInfo(
                Optional<BatchAndTotalResult> batchAndTotalResult, List<String> constraints, List<String> indexes) {
            batchAndTotalResult.ifPresent(a -> {
                this.batches = a.batches;
                this.total = a.total;
                this.timeTaken = a.timeTaken;
                this.committedOperations = a.committedOperations;
                this.failedOperations = a.failedOperations;
                this.failedBatches = a.failedBatches;
                this.retries = a.retries;
                this.errorMessages = a.errorMessages;
                this.batch = a.batch;
                this.operations = a.operations;
            });
            this.constraints = constraints;
            this.indexes = indexes;
        }
    }
}
