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
package apoc.coll;

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.io.ByteUnit.mebiBytes;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

/**
 * Proves that {@code apoc.coll.combinations} charges the whole result cardinality to the transaction memory tracker
 * before it builds anything. See {@code apoc.text.StringsMemoryTrackingTest} for why the limit is 4 MiB.
 */
@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
class CollMemoryTrackingTest {

    private static final String LOW_LIMIT = "4m";

    @Inject
    GraphDatabaseAPI db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfigRaw(Map.of("db.memory.transaction.max", LOW_LIMIT));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Coll.class);
    }

    @AfterEach
    void restoreLimit() {
        setTransactionMemoryLimit(mebiBytes(4));
    }

    @Test
    @Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void combinationsIsChargedUpFront() {
        // C(40, 20) is 137,846,528,820 combinations, more than an ArrayList can hold. A slow pass here would mean
        // the charge is incremental rather than up front, which is the failure mode the fix exists to avoid.
        assertMemoryLimitExceeded("RETURN apoc.coll.combinations(range(1, 40), 20) AS value");
        assertMemoryLimitExceeded("RETURN apoc.coll.combinations(range(1, 40), 1, 40) AS value");
        assertMemoryLimitExceeded("RETURN apoc.coll.combinations(range(1, 25), 12) AS value");
    }

    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void chargeIsProportionalToTheResultCardinality() {
        // C(20, 10) is 184,756 sublists, too much for a 4 MiB budget and comfortable within a 256 MiB one,
        // so the charge cannot be a constant
        assertMemoryLimitExceeded("RETURN apoc.coll.combinations(range(1, 20), 10) AS value");

        setTransactionMemoryLimit(mebiBytes(256));
        testCall(
                db,
                "RETURN apoc.coll.combinations(range(1, 20), 10) AS value",
                row -> assertEquals(184756, ((List<?>) row.get("value")).size()));
    }

    @Test
    void smallCombinationsAreUnaffected() {
        testCall(
                db,
                "RETURN apoc.coll.combinations([1,2,3], 2) AS value",
                row -> assertEquals(List.of(List.of(1L, 2L), List.of(1L, 3L), List.of(2L, 3L)), row.get("value")));
    }

    private void setTransactionMemoryLimit(long bytes) {
        db.getDependencyResolver()
                .resolveDependency(Config.class)
                .setDynamic(memory_transaction_max_size, bytes, getClass().getSimpleName());
    }

    private void assertMemoryLimitExceeded(String query) {
        QueryExecutionException e =
                assertThrows(QueryExecutionException.class, () -> testCall(db, query, row -> {}), query);
        assertTrue(
                TestUtil.hasCauses(e, MemoryLimitExceededException.class),
                query + " should fail with MemoryLimitExceededException, but failed with: " + e.getMessage());
        // A charge of a few megabytes breaches db.memory.transaction.max, a multi-gigabyte one breaches the
        // dbms.memory.transaction.total.max pool first; either way the message names the setting the operator tunes
        assertTrue(
                e.getMessage().contains("memory.transaction"),
                query + " should name the breached setting, but failed with: " + e.getMessage());
    }
}
