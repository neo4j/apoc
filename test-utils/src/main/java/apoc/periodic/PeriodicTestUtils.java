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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
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

    public static void testTerminateInnerPeriodicQuery(DbmsRule db, String periodicQuery, String iterateQueryContains) {
        assertThat(periodicQuery).contains(iterateQueryContains);

        final var executor = Executors.newCachedThreadPool();
        try {
            // Start execution of periodic query in separate thread.
            final var periodicResult = executor.submit(() -> db.executeTransactionally(
                    periodicQuery, Map.of(), r -> r.stream().toList()));

            // Terminate the inner query
            awaitUntilAsserted(() -> {
                final var innerTxId = findInnerQueryTx(db, periodicQuery, iterateQueryContains);
                try (final var tx = db.beginTx()) {
                    assertThat(tx.execute("TERMINATE TRANSACTION $id", Map.of("id", innerTxId)).stream())
                            .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                            .containsEntry("message", "Transaction terminated.");
                }
            });

            // Assert that the outer query also terminated
            awaitUntilAsserted(() -> {
                final var state = periodicResult.state();
                switch (state) {
                    case FAILED -> assertThat(periodicResult.exceptionNow()).hasMessageContaining("terminated");
                    case SUCCESS -> assertThat(periodicResult.resultNow())
                            .singleElement(InstanceOfAssertFactories.map(String.class, Object.class))
                            .satisfiesAnyOf(
                                    row -> assertThat(row).containsEntry("wasTerminated", true),
                                    row -> assertThat(row)
                                            .extractingByKey("batchErrors")
                                            .asString()
                                            .contains("terminated"),
                                    row -> assertThat(row)
                                            .extractingByKey("commitErrors")
                                            .asString()
                                            .contains("terminated"),
                                    row -> assertThat(row)
                                            .extractingByKey("errorMessages")
                                            .asString()
                                            .contains("terminated"));
                    default -> fail("Unexpected state of periodic query execution " + state);
                }
            });

            // Assert there's no query still running that is not supposed to.
            try (final var tx = db.beginTx()) {
                assertThat(tx.execute("SHOW TRANSACTIONS YIELD transactionId, currentQuery").stream())
                        .allSatisfy(row -> assertThat(row)
                                .extractingByKey("currentQuery")
                                .asString()
                                .doesNotContain(iterateQueryContains)
                                .doesNotContain(periodicQuery));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static String findInnerQueryTx(GraphDatabaseService db, String notQuery, String queryContains) {
        final var showTxs = "SHOW TRANSACTIONS YIELD transactionId as txId, currentQuery as query";
        // Show all queries to make test failures easier to investigate.
        final var txs =
                db.executeTransactionally(showTxs, Map.of(), r -> r.stream().toList());
        final var innerTxIds = txs.stream()
                .filter(row -> {
                    final var query = row.get("query");
                    return query != null
                            && !query.equals(notQuery)
                            && !query.toString().toLowerCase(Locale.ROOT).contains("apoc.periodic.")
                            && query.toString().contains(queryContains);
                })
                .map(row -> row.get("txId").toString())
                .toList();

        assertThat(innerTxIds).describedAs("All txs:%n%s", txs).isNotEmpty();
        return innerTxIds.getFirst();
    }
}
