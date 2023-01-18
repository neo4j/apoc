package apoc.trigger;

import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


public class TriggerNewProcedures {
    public static final String NON_SYS_DB_ERROR = "The procedure should be executed against a system database.";
    public static final String TRIGGER_NOT_ROUTED_ERROR = "The procedure should be routed and executed against a writer database.";
    public static final String TRIGGER_BAD_TARGET_ERROR = "Triggers can only be installed on user databases.";
    public static final String DB_NOT_FOUND_ERROR = "The user database with name '%s' does not exist";

    @Context public GraphDatabaseAPI db;
    
    @Context public Transaction tx;

    private void checkInSystem(boolean checkWriteable) {
        TriggerHandlerNewProcedures.checkEnabled();
        
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(NON_SYS_DB_ERROR);
        }
        
        if (checkWriteable && !Util.isWriteableInstance(db)) {
            throw new RuntimeException(TRIGGER_NOT_ROUTED_ERROR);
        }
    }

    private void checkTargetDatabase(String databaseName) {
        final Set<String> databases = tx.execute("SHOW DATABASES", Collections.emptyMap())
                .<String>columnAs("name")
                .stream()
                .collect(Collectors.toSet());
        if (!databases.contains(databaseName)) {
            throw new RuntimeException( String.format(DB_NOT_FOUND_ERROR, databaseName) );
        }

        if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
            throw new RuntimeException(TRIGGER_BAD_TARGET_ERROR);
        }
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("Eventually adds a trigger for a given database which is invoked when a successful transaction occurs.")
    public Stream<TriggerInfo> install(@Name("databaseName") String databaseName, @Name("name") String name, @Name("statement") String statement, @Name(value = "selector")  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        checkInSystem(true);
        checkTargetDatabase(databaseName);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        TriggerInfo removed = TriggerHandlerNewProcedures.install(databaseName, name, statement, selector, params);
        final TriggerInfo triggerInfo = new TriggerInfo(name, statement, selector, params, true, false);
        if (removed.query != null) {
            return Stream.of( removed, triggerInfo);
        }
        return Stream.of(triggerInfo);
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("Eventually removes an existing trigger, returns the trigger's information")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystem(true);
        final TriggerInfo removed = TriggerHandlerNewProcedures.drop(databaseName, name);
        if (removed == null) {
            return Stream.of(new TriggerInfo(name, null, null, false, false));
        }
        return Stream.of(removed);
    }
    
    
    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("Eventually removes all previously added trigger, returns triggers' information")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystem(true);
        return TriggerHandlerNewProcedures.dropAll(databaseName)
                .stream().sorted(Comparator.comparing(i -> i.name));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("Eventually pauses the trigger")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystem(true);

        return Stream.of(
                TriggerHandlerNewProcedures.updatePaused(databaseName, name, true));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.WRITE)
    @Description("Eventually unpauses the paused trigger")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystem(true);
        final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, false);

        return Stream.of(triggerInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(mode = Mode.READ)
    @Description("Lists all eventually installed triggers for a database")
    public Stream<TriggerInfo> show(@Name("databaseName") String databaseName) {
        checkInSystem(false);

        return TriggerHandlerNewProcedures.getTriggerNodesList(databaseName, tx);
    }
}