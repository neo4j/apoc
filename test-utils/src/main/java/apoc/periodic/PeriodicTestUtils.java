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
package apoc.periodic;

import static org.junit.Assert.assertTrue;

import apoc.util.TransactionTestUtil;
import apoc.util.collection.Iterators;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;

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
                })
                .start();
    }

    public static boolean terminateQuery(String pattern, GraphDatabaseAPI db) {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class);
        long numberOfKilledTransactions = kernelTransactions.activeTransactions().stream()
                .filter(kernelTransactionHandle -> kernelTransactionHandle
                        .executingQuery()
                        .map(query -> query.rawQueryText().contains(pattern))
                        .orElse(false))
                .map(kernelTransactionHandle ->
                        kernelTransactionHandle.markForTermination(Status.Transaction.Terminated))
                .count();
        return numberOfKilledTransactions > 0;
    }

    public static void testTerminateWithCommand(DbmsRule db, String periodicQuery, String iterateQuery) {
        long timeBefore = System.currentTimeMillis();
        TransactionTestUtil.terminateTransactionAsync(db, 10L, iterateQuery);
        checkPeriodicTerminated(db, periodicQuery);
        TransactionTestUtil.lastTransactionChecks(db, periodicQuery, timeBefore);
    }

    private static void checkPeriodicTerminated(DbmsRule db, String periodicQuery) {
        try {
            org.neo4j.test.assertion.Assert.assertEventually(
                    () -> db.executeTransactionally(periodicQuery, Map.of(), result -> {
                        Map<String, Object> row = Iterators.single(result);
                        return (boolean) row.get("wasTerminated");
                    }),
                    (value) -> value,
                    15L,
                    TimeUnit.SECONDS);
        } catch (Exception tfe) {
            assertTrue(tfe.getMessage(), tfe.getMessage().contains("terminated"));
        }
    }
}
