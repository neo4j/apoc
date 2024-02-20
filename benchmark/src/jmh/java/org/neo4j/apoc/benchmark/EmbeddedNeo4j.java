package org.neo4j.apoc.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EmbeddedNeo4j {

    public final GraphDatabaseService db;
    public final DatabaseManagementService managementService;
    public final Path directory;

    private EmbeddedNeo4j(GraphDatabaseService db, DatabaseManagementService managementService, Path directory) {
        this.db = db;
        this.managementService = managementService;
        this.directory = directory;
    }

    public static EmbeddedNeo4j start() throws IOException {
        final var path = Files.createTempDirectory("neo4j-bench");
        final var managementService = new DatabaseManagementServiceBuilder(path).build();

        final var db = managementService.database(DEFAULT_DATABASE_NAME);
        registerShutdownHook(managementService);
        return new EmbeddedNeo4j(db, managementService, path);
    }

    public void registerProcedure(Class<?>... procedures) {
        final var globalProcedures = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(GlobalProcedures.class);
        for (Class<?> procedure : procedures) {
            try {
                globalProcedures.registerProcedure(procedure);
                globalProcedures.registerFunction(procedure);
                globalProcedures.registerAggregationFunction(procedure);
            } catch (KernelException e) {
                throw new RuntimeException("Failed to register " + procedure, e);
            }
        }
    }

    private static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                managementService.shutdown();
            }
        });
    }
}
