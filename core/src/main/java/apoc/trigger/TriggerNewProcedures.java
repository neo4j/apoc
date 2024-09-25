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
package apoc.trigger;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import apoc.util.Util;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class TriggerNewProcedures {
    public static final String NON_SYS_DB_ERROR = "The procedure should be executed against a system database.";
    public static final String TRIGGER_NOT_ROUTED_ERROR = "No write operations are allowed directly on this database. "
            + "Writes must pass through the leader. The role of this server is: FOLLOWER";
    public static final String TRIGGER_BAD_TARGET_ERROR = "Triggers can only be installed on user databases.";
    public static final String DB_NOT_FOUND_ERROR = "The user database with name '%s' does not exist";

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Transaction tx;

    private void checkInSystemWriter() {
        TriggerHandlerNewProcedures.checkEnabled();

        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db)) {
            throw new RuntimeException(TRIGGER_NOT_ROUTED_ERROR);
        }
    }

    private void checkInSystem() {
        TriggerHandlerNewProcedures.checkEnabled();

        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(NON_SYS_DB_ERROR);
        }
    }

    private void checkTargetDatabase(String databaseName) {
        final Set<String> databases =
                tx.execute("SHOW DATABASES", Collections.emptyMap()).<String>columnAs("name").stream()
                        .collect(Collectors.toSet());
        if (!databases.contains(databaseName)) {
            throw new RuntimeException(String.format(DB_NOT_FOUND_ERROR, databaseName));
        }

        if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_BAD_TARGET_ERROR);
        }
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.install", mode = Mode.WRITE)
    @Description(
            "Eventually adds a trigger for a given database which is invoked when a successful transaction occurs.")
    public Stream<TriggerInfo> install(
            @Name(value = "databaseName", description = "The name of the database to add the trigger to.")
                    String databaseName,
            @Name(value = "name", description = "The name of the trigger to add.") String name,
            @Name(value = "statement", description = "The query to run when triggered.") String statement,
            @Name(
                            value = "selector",
                            description =
                                    "{ phase = \"before\" :: [\"before\", \"rollback\", \"after\", \"afterAsync\"] }")
                    Map<String, Object> selector,
            @Name(value = "config", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> config) {
        checkInSystemWriter();
        checkTargetDatabase(databaseName);
        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());

        return withUpdatingTransaction(
                databaseName,
                tx -> Stream.of(
                        TriggerHandlerNewProcedures.install(databaseName, name, statement, selector, params, tx)));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.drop", mode = Mode.WRITE)
    @Description("Eventually removes the given trigger.")
    public Stream<TriggerInfo> drop(
            @Name(value = "databaseName", description = "The name of the database to drop the trigger from.")
                    String databaseName,
            @Name(value = "name", description = "The name of the trigger to drop.") String name) {
        checkInSystemWriter();

        return withUpdatingTransaction(
                databaseName, tx -> Stream.ofNullable(TriggerHandlerNewProcedures.drop(databaseName, name, tx)));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.dropAll", mode = Mode.WRITE)
    @Description("Eventually removes all triggers from the given database.")
    public Stream<TriggerInfo> dropAll(
            @Name(value = "databaseName", description = "The name of the database to drop the triggers from.")
                    String databaseName) {
        checkInSystemWriter();

        return withUpdatingTransaction(
                databaseName, tx -> TriggerHandlerNewProcedures.dropAll(databaseName, tx).stream()
                        .sorted(Comparator.comparing(i -> i.name)));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.stop", mode = Mode.WRITE)
    @Description("Eventually stops the given trigger.")
    public Stream<TriggerInfo> stop(
            @Name(value = "databaseName", description = "The name of the database the trigger is on.")
                    String databaseName,
            @Name(value = "name", description = "The name of the trigger to drop.") String name) {
        checkInSystemWriter();

        return withUpdatingTransaction(databaseName, tx -> {
            final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, true, tx);
            return Stream.ofNullable(triggerInfo);
        });
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.start", mode = Mode.WRITE)
    @Description("Eventually restarts the given paused trigger.")
    public Stream<TriggerInfo> start(
            @Name(value = "databaseName", description = "The name of the database the trigger is on.")
                    String databaseName,
            @Name(value = "name", description = "The name of the trigger to resume.") String name) {
        checkInSystemWriter();

        return withUpdatingTransaction(databaseName, tx -> {
            final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, false, tx);
            return Stream.ofNullable(triggerInfo);
        });
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.show", mode = Mode.READ)
    @Description("Lists all eventually installed triggers for a database.")
    public Stream<TriggerInfo> show(
            @Name(value = "databaseName", description = "The name of the database to show triggers on.")
                    String databaseName) {
        checkInSystem();

        return TriggerHandlerNewProcedures.getTriggerNodesList(databaseName, tx);
    }

    public <T> T withUpdatingTransaction(String databaseName, Function<Transaction, T> action) {
        T result = null;
        try (Transaction tx = db.beginTx()) {
            result = action.apply(tx);
            tx.commit();
        }

        // Last update time needs to be after the installation commit happened to not risk missing updates
        try (final var tx = db.beginTx()) {
            TriggerHandlerNewProcedures.setLastUpdate(databaseName, tx);
            tx.commit();
        }
        return result;
    }
}
