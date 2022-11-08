package apoc.periodic;

import apoc.util.collection.Iterators;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;

import java.util.Map;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PeriodicTestUtils {
    public static void killPeriodicQueryAsync(DbmsRule db) {
        new Thread(() -> {
            int retries = 10;
            try {
                while (retries-- > 0 && !terminateQuery("apoc.periodic", db)) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }).start();
    }

    public static boolean terminateQuery(String pattern, GraphDatabaseAPI db) {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);
        long numberOfKilledTransactions = kernelTransactions.activeTransactions().stream()
                .filter(kernelTransactionHandle ->
                        kernelTransactionHandle.executingQuery().map(query -> query.rawQueryText().contains(pattern))
                                .orElse(false)
                )
                .map(kernelTransactionHandle -> kernelTransactionHandle.markForTermination(Status.Transaction.Terminated))
                .count();
        return numberOfKilledTransactions > 0;
    }

    public static void testTerminatePeriodicQuery(DbmsRule db, String periodicQuery) {
        killPeriodicQueryAsync(db);
        try {
            org.neo4j.test.assertion.Assert.assertEventually( () ->
                db.executeTransactionally(periodicQuery, Map.of(),
                    result -> {
                        Map<String, Object> row = Iterators.single(result);
                        return row.get("wasTerminated");
                    }),
                (value) -> true, 10L, TimeUnit.SECONDS);
        } catch(Exception tfe) {
            assertEquals(tfe.getMessage(),true, tfe.getMessage().contains("terminated"));
        }
    }
}
