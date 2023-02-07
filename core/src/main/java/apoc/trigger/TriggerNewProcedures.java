package apoc.trigger;

import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
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
    public static final String TRIGGER_NOT_ROUTED_ERROR = "No write operations are allowed directly on this database. " +
            "Writes must pass through the leader. The role of this server is: FOLLOWER";
    public static final String TRIGGER_BAD_TARGET_ERROR = "Triggers can only be installed on user databases.";
    public static final String DB_NOT_FOUND_ERROR = "The user database with name '%s' does not exist";

    @Context public GraphDatabaseAPI db;
    
    @Context public Log log;
    
    @Context public Transaction tx;

    private void checkInSystemWriter() {
        TriggerHandlerNewProcedures.checkEnabled();
        
        if (!db.databaseName().equals(SYSTEM_DATABASE_NAME) || !Util.isWriteableInstance(db)) {
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
    @Procedure(name = "apoc.trigger.install", mode = Mode.WRITE)
    @Description("Eventually adds a trigger for a given database which is invoked when a successful transaction occurs.")
    public Stream<TriggerInfo> install(@Name("databaseName") String databaseName, @Name("name") String name, @Name("statement") String statement, @Name(value = "selector")  Map<String,Object> selector, @Name(value = "config", defaultValue = "{}") Map<String,Object> config) {
        checkInSystemWriter();
        checkTargetDatabase(databaseName);
        Map<String,Object> params = (Map)config.getOrDefault("params", Collections.emptyMap());
        
        TriggerInfo triggerInfo = TriggerHandlerNewProcedures.install(databaseName, name, statement, selector, params);
        return Stream.of(triggerInfo);
    }


    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.drop", mode = Mode.WRITE)
    @Description("Eventually removes the given trigger.")
    public Stream<TriggerInfo> drop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemWriter();
        final TriggerInfo removed = TriggerHandlerNewProcedures.drop(databaseName, name);
        return Stream.ofNullable(removed);
    }
    
    
    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.dropAll", mode = Mode.WRITE)
    @Description("Eventually removes all triggers from the given database.")
    public Stream<TriggerInfo> dropAll(@Name("databaseName") String databaseName) {
        checkInSystemWriter();
        return TriggerHandlerNewProcedures.dropAll(databaseName)
                .stream().sorted(Comparator.comparing(i -> i.name));
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.stop", mode = Mode.WRITE)
    @Description("Eventually stops the given trigger.")
    public Stream<TriggerInfo> stop(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemWriter();

        final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, true);
        return Stream.ofNullable(triggerInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.start", mode = Mode.WRITE)
    @Description("Eventually restarts the given paused trigger.")
    public Stream<TriggerInfo> start(@Name("databaseName") String databaseName, @Name("name")String name) {
        checkInSystemWriter();
        
        final TriggerInfo triggerInfo = TriggerHandlerNewProcedures.updatePaused(databaseName, name, false);
        return Stream.ofNullable(triggerInfo);
    }

    // TODO - change with @SystemOnlyProcedure
    @SystemProcedure
    @Admin
    @Procedure(name = "apoc.trigger.show", mode = Mode.READ)
    @Description("Lists all eventually installed triggers for a database.")
    public Stream<TriggerInfo> show(@Name("databaseName") String databaseName) {
        checkInSystemWriter();

        return TriggerHandlerNewProcedures.getTriggerNodesList(databaseName, tx);
    }
}