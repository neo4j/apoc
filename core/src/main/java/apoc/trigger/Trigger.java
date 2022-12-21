package apoc.trigger;

import apoc.util.Util;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 20.09.16
 */

public class Trigger {
    // public for testing purpose
    public static final String SYS_NON_WRITER_ERROR = """
            This instance is not allowed to write to the database because it is a follower.
            Please use these procedures against the database leader.
            """;
    
    @Context
    public GraphDatabaseAPI db;

    @Context public TriggerHandler triggerHandler;

    @Context public Log log;

    private void preprocessDeprecatedProcedures() {
        final String msgDeprecation = """
                Please note that the current procedure is deprecated,
                it's recommended to use the `apoc.trigger.install`, `apoc.trigger.drop`, `apoc.trigger.dropAll`, `apoc.trigger.stop`, and `apoc.trigger.start` procedures,
                instead of, respectively, `apoc.trigger.add`, `apoc.trigger.remove`, `apoc.trigger.removeAll`, `apoc.trigger.pause`, and `apoc.trigger.resume`.""";

        log.warn(msgDeprecation);

        if (!Util.isWriteableInstance(db)) {
            throw new RuntimeException(SYS_NON_WRITER_ERROR + msgDeprecation);
        }
    }

    @Deprecated
    @Procedure(name = "apoc.trigger.add", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.install")
    @Description("Adds a trigger to the given Cypher statement.\n" +
            "The selector for this procedure is {phase:'before/after/rollback/afterAsync'}.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("kernelTransaction") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        preprocessDeprecatedProcedures();
        
        Util.validateQuery(db, statement);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = triggerHandler.add(name, statement, selector, params);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false),
                    new TriggerInfo(name,statement,selector, params,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, params,true, false));
    }

    @Deprecated
    @Procedure(name = "apoc.trigger.remove", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.drop")
    @Description("Removes the given trigger.")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> removed = triggerHandler.remove(name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }

    @Deprecated
    @Procedure(name = "apoc.trigger.removeAll", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.dropAll")
    @Description("Removes all previously added triggers.")
    public Stream<TriggerInfo> removeAll() {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> removed = triggerHandler.removeAll();
        if (removed == null) {
            return Stream.of(new TriggerInfo(null, null, null, false, false));
        }
        return removed.entrySet().stream().map(TriggerInfo::entryToTriggerInfo);
    }

    @Procedure(name = "apoc.trigger.list", mode = Mode.READ)
    @Description("Lists all installed triggers.")
    public Stream<TriggerInfo> list() {
        return triggerHandler.list().entrySet().stream()
                .map( (e) -> new TriggerInfo(e.getKey(),
                        (String)e.getValue().get("statement"),
                        (Map<String,Object>) e.getValue().get("selector"),
                        (Map<String, Object>) e.getValue().get("params"),
                        true,
                        (Boolean) e.getValue().getOrDefault("paused", false))
                );
    }

    @Deprecated
    @Procedure(name = "apoc.trigger.pause", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.stop")
    @Description("Pauses the given trigger.")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> paused = triggerHandler.updatePaused(name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    @Deprecated
    @Procedure(name = "apoc.trigger.resume", mode = Mode.WRITE, deprecatedBy = "apoc.trigger.start")
    @Description("Resumes the given paused trigger.")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        preprocessDeprecatedProcedures();
        
        Map<String, Object> resume = triggerHandler.updatePaused(name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }
}
