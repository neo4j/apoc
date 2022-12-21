package apoc.trigger;

import apoc.SystemPropertyKeys;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
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

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class TriggerNewProcedures {
    // public for testing purpose
    public static final String TRIGGER_NOT_ROUTED_ERROR = "The procedure should be routed and executed against a writer system database";
    public static final String TRIGGER_BAD_TARGET_ERROR = "Triggers can only be installed on user databases.";

    @Context public GraphDatabaseAPI db;
    
    @Context public Log log;
    
    @Context public Transaction tx;

    private void checkInSystemWriter() {
        TriggerHandlerNewProcedures.checkEnabled();
        // routing check
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db)) {
            throw new RuntimeException(TRIGGER_NOT_ROUTED_ERROR);
        }
    }

    private void checkTargetDatabase(String databaseName) {
        if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_BAD_TARGET_ERROR);
        }
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.install(databaseName, name, statement, selector, config) | eventually adds a trigger for a given database which is invoked when a successful transaction occurs.")
    public Stream<TriggerInfo> install(@Name("databaseName") String databaseName, @Name("name") String name, @Name("statement") String statement, @Name(value = "selector")  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        checkInSystemWriter();
        checkTargetDatabase(databaseName);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        Map<String, Object> removed = TriggerHandlerNewProcedures.install(databaseName, name, statement, selector, params);
        if (removed.containsKey(SystemPropertyKeys.statement.name())) {
            return Stream.of(
                    TriggerInfo.from(removed, false),
                    new TriggerInfo( name, statement, selector, params, true, false));
        }
        return Stream.of(new TriggerInfo( name, statement, selector, params, true, false));
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.drop(databaseName, name) | eventually removes an existing trigger, returns the trigger's information")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemWriter();
        Map<String, Object> removed = TriggerHandlerNewProcedures.drop(databaseName, name);
        if (!removed.containsKey(SystemPropertyKeys.statement.name())) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(TriggerInfo.from(removed, false));
    }
    
    
    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.dropAll(databaseName) | eventually removes all previously added trigger, returns triggers' information")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemWriter();
        Map<String, Object> removed = TriggerHandlerNewProcedures.dropAll(databaseName);
        return removed.entrySet().stream().map(TriggerInfo::entryToTriggerInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.stop(databaseName, name) | eventually pauses the trigger")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemWriter();
        Map<String, Object> paused = TriggerHandlerNewProcedures.updatePaused(databaseName, name, true);

        return Stream.of(TriggerInfo.from(paused,true));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.WRITE)
    @Description("CALL apoc.trigger.start(databaseName, name) | eventually unpauses the paused trigger")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemWriter();
        Map<String, Object> resume = TriggerHandlerNewProcedures.updatePaused(databaseName, name, false);

        return Stream.of(TriggerInfo.from(resume, true));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Procedure(mode = Mode.READ)
    @Description("CALL apoc.trigger.show(databaseName) | it lists all eventually installed triggers for a database")
    public Stream<TriggerInfo> show(@Name("databaseName") String databaseName) {
        checkInSystemWriter();

        return TriggerHandlerNewProcedures.getTriggerNodesList(databaseName, tx)
                .stream()
                .map(trigger -> TriggerInfo.from(trigger, true)
                );
    }
}