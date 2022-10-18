package apoc.trigger;

import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_ONLY;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.READ_WRITE;
import static org.neo4j.graphdb.QueryExecutionType.QueryType.WRITE;

/**
 * @author mh
 * @since 20.09.16
 */

public class Trigger {
    public static class TriggerInfo {
        public String name;
        public String query;
        public Map<String,Object> selector;
        public Map<String, Object> params;
        public boolean installed;
        public boolean paused;

        public TriggerInfo(String name, String query, Map<String, Object> selector, boolean installed, boolean paused) {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.installed = installed;
            this.paused = paused;
        }

        public TriggerInfo( String name, String query, Map<String,Object> selector, Map<String,Object> params, boolean installed, boolean paused )
        {
            this.name = name;
            this.query = query;
            this.selector = selector;
            this.params = params;
            this.installed = installed;
            this.paused = paused;
        }
    }

    @Context public GraphDatabaseService db;

    @Context public TriggerHandler triggerHandler;

    @Procedure(name = "apoc.trigger.add", mode = Mode.WRITE)
    @Description("Adds a trigger to the given Cypher statement.\n" +
            "The selector for this procedure is {phase:'before/after/rollback/afterAsync'}.")
    public Stream<TriggerInfo> add(@Name("name") String name, @Name("statement") String statement, @Name(value = "selector"/*, defaultValue = "{}"*/)  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        Util.validateQuery(db, statement,
                READ_ONLY, WRITE, READ_WRITE);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = triggerHandler.add(name, statement, selector, params);
        if (removed != null) {
            return Stream.of(
                    new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false),
                    new TriggerInfo(name,statement,selector, params,true, false));
        }
        return Stream.of(new TriggerInfo(name,statement,selector, params,true, false));
    }

    @Procedure(name = "apoc.trigger.remove", mode = Mode.WRITE)
    @Description("Removes the given trigger.")
    public Stream<TriggerInfo> remove(@Name("name")String name) {
        Map<String, Object> removed = triggerHandler.remove(name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(new TriggerInfo(name,(String)removed.get("statement"), (Map<String, Object>) removed.get("selector"), (Map<String, Object>) removed.get("params"),false, false));
    }

    @Procedure(name = "apoc.trigger.removeAll", mode = Mode.WRITE)
    @Description("Removes all previously added triggers.")
    public Stream<TriggerInfo> removeAll() {
        Map<String, Object> removed = triggerHandler.removeAll();
        if (removed == null) {
            return Stream.of(new TriggerInfo(null, null, null, false, false));
        }
        return removed.entrySet().stream().map(this::toTriggerInfo);
    }

    public TriggerInfo toTriggerInfo(Map.Entry<String, Object> e) {
        String name = e.getKey();
        if (e.getValue() instanceof Map) {
            try {
                Map<String, Object> value = (Map<String, Object>) e.getValue();
                return new TriggerInfo(name, (String) value.get("statement"), (Map<String, Object>) value.get("selector"), (Map<String, Object>) value.get("params"), false, false);
            } catch(Exception ex) {
                return new TriggerInfo(name, ex.getMessage(), null, false, false);
            }
        }
        return new TriggerInfo(name, null, null, false, false);
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

    @Procedure(name = "apoc.trigger.pause", mode = Mode.WRITE)
    @Description("Pauses the given trigger.")
    public Stream<TriggerInfo> pause(@Name("name")String name) {
        Map<String, Object> paused = triggerHandler.updatePaused(name, true);

        return Stream.of(new TriggerInfo(name,
                (String)paused.get("statement"),
                (Map<String,Object>) paused.get("selector"),
                (Map<String,Object>) paused.get("params"),true, true));
    }

    @Procedure(name = "apoc.trigger.resume", mode = Mode.WRITE)
    @Description("Resumes the given paused trigger.")
    public Stream<TriggerInfo> resume(@Name("name")String name) {
        Map<String, Object> resume = triggerHandler.updatePaused(name, false);

        return Stream.of(new TriggerInfo(name,
                (String)resume.get("statement"),
                (Map<String,Object>) resume.get("selector"),
                (Map<String,Object>) resume.get("params"),true, false));
    }

}
