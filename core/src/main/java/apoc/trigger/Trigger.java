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

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;

import apoc.ApocConfig;
import apoc.util.Util;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * @author mh
 * @since 20.09.16
 */
public class Trigger {

    public static final String NOT_ENABLED_ERROR = "Triggers have not been enabled."
            + " Set 'apoc.trigger.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public static final String SYS_DB_NON_WRITER_ERROR =
            """
            This instance is not allowed to write to the system database.
            Please open a session against a system database writer when using this procedure.
            """;

    @Context
    public GraphDatabaseAPI db;

    @Context
    public TriggerHandler triggerHandler;

    private void preprocessDeprecatedProcedures() {
        final String msgDeprecation =
                """
                Please note that the current procedure is deprecated,
                it is recommended to use the `apoc.trigger.install`, `apoc.trigger.drop`, `apoc.trigger.dropAll`, `apoc.trigger.stop`, and `apoc.trigger.start` procedures,
                instead of, respectively, `apoc.trigger.add`, `apoc.trigger.remove`, `apoc.trigger.removeAll`, `apoc.trigger.pause`, and `apoc.trigger.resume`.""";

        if (!Util.isWriteableInstance((GraphDatabaseAPI) apocConfig().getSystemDb())) {
            throw new RuntimeException(SYS_DB_NON_WRITER_ERROR + msgDeprecation);
        }
    }

    @Admin
    @Deprecated
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(name = "apoc.trigger.add", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.install")
    @Description("Adds a trigger to the given Cypher statement.\n"
            + "The selector for this procedure is {phase:'before/after/rollback/afterAsync'}.")
    public Stream<TriggerInfo> add(
            @Name(value = "name", description = "The name of the trigger to add.") String name,
            @Name(value = "statement", description = "The query to run when triggered.") String statement,
            @Name(
                            value = "selector",
                            description =
                                    "{ phase = \"before\" :: [\"before\", \"rollback\", \"after\", \"afterAsync\"] }")
                    Map<String, Object> selector,
            @Name(value = "config", defaultValue = "{}", description = "The parameters for the given Cypher statement.")
                    Map<String, Object> config) {
        checkEnabled(apocConfig());
        preprocessDeprecatedProcedures();

        Util.validateQuery(db, statement);
        Map<String, Object> params = (Map) config.getOrDefault("params", Collections.emptyMap());
        var query = Util.prefixQueryWithCheck("5", statement);
        Map<String, Object> removed = triggerHandler.add(name, query, selector, params);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(
                            name,
                            (String) removed.get("statement"),
                            (Map<String, Object>) removed.get("selector"),
                            (Map<String, Object>) removed.get("params"),
                            false,
                            false),
                    new TriggerInfo(name, statement, selector, params, true, false));
        }
        return Stream.of(new TriggerInfo(name, statement, selector, params, true, false));
    }

    @Admin
    @Deprecated
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(name = "apoc.trigger.remove", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.drop")
    @Description("Removes the given trigger.")
    public Stream<TriggerInfo> remove(
            @Name(value = "name", description = "The name of the trigger to drop.") String name) {
        checkEnabled(apocConfig());
        preprocessDeprecatedProcedures();

        Map<String, Object> removed = triggerHandler.remove(name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(
                name,
                (String) removed.get("statement"),
                (Map<String, Object>) removed.get("selector"),
                (Map<String, Object>) removed.get("params"),
                false,
                false));
    }

    @Admin
    @Deprecated
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(name = "apoc.trigger.removeAll", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.dropAll")
    @Description("Removes all previously added triggers.")
    public Stream<TriggerInfo> removeAll() {
        checkEnabled(apocConfig());
        preprocessDeprecatedProcedures();

        final var removed = triggerHandler.removeAll();
        if (removed == null) {
            return Stream.of(new TriggerInfo(null, null, null, false, false));
        }
        return removed.entrySet().stream().map(r -> TriggerInfo.from(r.getValue(), false, r.getKey()));
    }

    @Admin
    @Procedure(name = "apoc.trigger.list", mode = Mode.READ)
    @Description("Lists all currently installed triggers for the session database.")
    public Stream<TriggerInfo> list() {
        checkEnabled(apocConfig());
        return triggerHandler.list().entrySet().stream()
                .map((e) -> new TriggerInfo(
                        e.getKey(),
                        (String) e.getValue().get("statement"),
                        (Map<String, Object>) e.getValue().get("selector"),
                        (Map<String, Object>) e.getValue().get("params"),
                        true,
                        (Boolean) e.getValue().getOrDefault("paused", false)));
    }

    @Admin
    @Deprecated
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(name = "apoc.trigger.pause", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.stop")
    @Description("Pauses the given trigger.")
    public Stream<TriggerInfo> pause(
            @Name(value = "name", description = "The name of the trigger to pause.") String name) {
        checkEnabled(apocConfig());
        preprocessDeprecatedProcedures();

        Map<String, Object> paused = triggerHandler.updatePaused(name, true);

        return Stream.of(new TriggerInfo(
                name,
                (String) paused.get("statement"),
                (Map<String, Object>) paused.get("selector"),
                (Map<String, Object>) paused.get("params"),
                true,
                true));
    }

    @Admin
    @Deprecated
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(name = "apoc.trigger.resume", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.start")
    @Description("Resumes the given paused trigger.")
    public Stream<TriggerInfo> resume(
            @Name(value = "name", description = "The name of the trigger to resume.") String name) {
        checkEnabled(apocConfig());
        preprocessDeprecatedProcedures();

        Map<String, Object> resume = triggerHandler.updatePaused(name, false);

        return Stream.of(new TriggerInfo(
                name,
                (String) resume.get("statement"),
                (Map<String, Object>) resume.get("selector"),
                (Map<String, Object>) resume.get("params"),
                true,
                false));
    }

    private void checkEnabled(ApocConfig apocConfig) {
        if (!apocConfig.getConfig().getBoolean(APOC_TRIGGER_ENABLED)) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }
}
