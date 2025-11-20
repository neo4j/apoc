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
package apoc.lock;

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(configurationCallback = "configure", createDatabasePerTest = false)
public class LockTest {

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.lock_acquisition_timeout, Duration.ofSeconds(1));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Lock.class);
    }

    @Test
    void shouldReadLockBlockAWrite() throws Exception {

        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            final Node n = Iterators.single(tx.execute("match (n) CALL apoc.lock.read.nodes([n]) return n")
                    .columnAs("n"));
            assertEquals(n, node);

            final Thread thread = new Thread(() -> {
                System.out.println(Instant.now().toString() + " pre-delete");

                // TransactionFailure due to lock timeout
                assertThrows(TransactionFailureException.class, () -> testCall(db, "MATCH (n) DELETE n", row -> {}));

                System.out.println(Instant.now().toString() + " delete");
            });
            thread.start();
            thread.join(5000L);

            // the blocked thread didn't do any work, so we still have nodes
            long count = Iterators.count(tx.execute("match (n) return n").columnAs("n"));
            assertEquals(1, count);

            tx.commit();
        }
    }
}
