package apoc.cypher;

import apoc.ApocConfig;
import apoc.SystemLabels;
import apoc.util.Util;
import apoc.util.collection.Iterators;
import apoc.version.Version;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.Log;

import java.util.Collection;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static apoc.SystemPropertyKeys.database;

public class CypherInitializer implements AvailabilityListener {

    private final GraphDatabaseAPI db;
    private final Log userLog;
    private final DependencyResolver dependencyResolver;
    private final DatabaseManagementService databaseManagementService;
    private final DatabaseEventListeners databaseEventListeners;

    /**
     * indicates the status of the initializer, to be used for tests to ensure initializer operations are already done
     */
    private volatile boolean finished = false;

    public CypherInitializer(GraphDatabaseAPI db, Log userLog,
                             DatabaseManagementService databaseManagementService,
                             DatabaseEventListeners databaseEventListeners) {
        this.db = db;
        this.userLog = userLog;
        this.databaseManagementService = databaseManagementService;
        this.databaseEventListeners = databaseEventListeners;
        this.dependencyResolver = db.getDependencyResolver();
    }

    public boolean isFinished() {
        return finished;
    }

    @Override
    public void available() {
        // run initializers in a new thread
        // we need to wait until apoc procs are registered
        // unfortunately an AvailabilityListener is triggered before that
        Util.newDaemonThread(() -> {
            try {
                // Wait for database to become available, once it is available
                // procedures are also available by necessity.
                while (!db.isAvailable(100));

                // An initializer is attached to the lifecycle for each database. To ensure that this
                // check is performed **once** during the DBMS startup, we validate the version if and
                // only if we are the AvailabilityListener for the system database - since there is only
                // ever one of those.
                if (db.databaseId().isSystemDatabase() ) {
                    String neo4jVersion = org.neo4j.kernel.internal.Version.getNeo4jVersion();
                    final String apocVersion = Version.class.getPackage().getImplementationVersion();
                    if (isVersionDifferent(neo4jVersion, apocVersion))
                    {
                        userLog.warn( "The apoc version (%s) and the Neo4j DBMS versions %s are incompatible. \n" +
                                        "The two first numbers of both versions needs to be the same.",
                                apocVersion, neo4jVersion );
                    }
                }

                    // this block is already in system db, so we can directly execute system operation
                    // for backward compatibility: clear system nodes at start-up if the corresponding database name doesn't exist
                    // placed here instead of e.g. `ApocConfig` because at this point we are sure that all databases are recovered
                    cleanUpSystemNodes();

                    // create listener for each database
                    databaseEventListeners.registerDatabaseEventListener(new SystemFunctionalityListener());

                }

                Configuration config = dependencyResolver.resolveDependency(ApocConfig.class).getConfig();
                for (String query : collectInitializers(config)) {
                    try {
                        // we need to apply a retry strategy here since in systemdb we potentially conflict with
                        // creating constraints which could cause our query to fail with a transient error.
                        Util.retryInTx(userLog, db, tx -> Iterators.count(tx.execute(query)), 0, 5, retries -> { });
                        userLog.info("successfully initialized: " + query);
                    } catch (Exception e) {
                        userLog.error("error upon initialization, running: " + query, e);
                    }
                }
            } finally {
                finished = true;
            }
        }).start();
    }

    // the visibility is public only for testing purpose, it could be private otherwise
    public static boolean isVersionDifferent(String neo4jVersion, String apocVersion) {
        final String[] apocSplit = splitVersion(apocVersion);
        final String[] neo4jSplit = splitVersion(neo4jVersion);

        return !(apocSplit != null && neo4jSplit != null
                && apocSplit[0].equals(neo4jSplit[0])
                && apocSplit[1].equals(neo4jSplit[1]));
    }

    private static String[] splitVersion(String completeVersion) {
        if (StringUtils.isBlank(completeVersion)) {
            return null;
        }
        return completeVersion.split("[^\\d]");
    }

    private Collection<String> collectInitializers(Configuration config) {
        Map<String, String> initializers = new TreeMap<>();

        config.getKeys(ApocConfig.APOC_CONFIG_INITIALIZER + "." + db.databaseName())
                .forEachRemaining(key -> putIfNotBlank(initializers, key, config.getString(key)));

        return initializers.values();
    }

    private void putIfNotBlank(Map<String,String> map, String key, String value) {
        if ((value!=null) && (!value.isBlank())) {
            map.put(key, value);
        }
    }

    private void awaitApocProceduresRegistered() {
        while (!areProceduresRegistered("apoc")) {
            Util.sleep(100);
        }
    }

    private void awaitDbmsComponentsProcedureRegistered() {
        while (!areProceduresRegistered("dbms.components")) {
            Util.sleep(100);
        }
    }

    private boolean areProceduresRegistered(String procStart) {
        try {
            return procs.getAllProcedures().stream().anyMatch(signature -> signature.name().toString().startsWith(procStart));
        } catch (ConcurrentModificationException e) {
            // if a CME happens (possible during procedure scanning)
            // we return false and the caller will try again
            return false;
        }
    }

    @Override
    public void unavailable() {
        // intentionally empty
    }

    private void cleanUpSystemNodes() {

        List<String> databases = databaseManagementService.listDatabases();

        forEachSystemLabel((tx, label) -> {
                tx.findNodes(label).forEachRemaining(node -> {
                    boolean dbNonExistent = node.hasProperty(database.name())
                            && !databases.contains( (String) node.getProperty(database.name()) );
                    if (dbNonExistent) {
                        node.delete();
                    }
                });
        });
    }

    private class SystemFunctionalityListener implements DatabaseEventListener {

        @Override
        public void databaseDrop(DatabaseEventContext eventContext) {

            forEachSystemLabel((tx, label) -> {
                tx.findNodes(label, database.name(), eventContext.getDatabaseName())
                        .forEachRemaining(Node::delete);
            });
        }

        @Override
        public void databaseStart(DatabaseEventContext eventContext) {}

        @Override
        public void databaseShutdown(DatabaseEventContext eventContext) {}

        @Override
        public void databasePanic(DatabaseEventContext eventContext) {}

        @Override
        public void databaseCreate(DatabaseEventContext eventContext) {}
    }

    private void forEachSystemLabel(BiConsumer<Transaction, Label> consumer) {
        try (Transaction tx = db.beginTx()) {
            for (Label label: SystemLabels.values()) {
                consumer.accept(tx, label);
            }
            tx.commit();
        }
    }
}
