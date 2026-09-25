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
package apoc.text;

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.io.ByteUnit.mebiBytes;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
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
 * Proves that the size estimates in {@code apoc.text} are charged to the transaction memory tracker, as opposed to the
 * argument validation quietly doing all the work.
 *
 * <p>The database runs with a deliberately low {@code db.memory.transaction.max}, so oversized calls are refused
 * before they allocate anything and the test is safe to run in CI. A freshly created tracker already reports ~2 MiB in
 * use, because the implementation pre-registers memory in its internal pools, so a limit at or below that would fail
 * every call indiscriminately.
 */
@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
class StringsMemoryTrackingTest {

    private static final String LOW_LIMIT = "4m";

    @Inject
    GraphDatabaseAPI db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfigRaw(Map.of("db.memory.transaction.max", LOW_LIMIT));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Strings.class);
    }

    @AfterEach
    void restoreLimit() {
        setTransactionMemoryLimit(mebiBytes(4));
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void repeatIsCharged() {
        assertMemoryLimitExceeded("RETURN apoc.text.repeat('a', 10000000) AS value");
        assertMemoryLimitExceeded("RETURN apoc.text.repeat('a', 2000000000) AS value");
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void padIsCharged() {
        assertMemoryLimitExceeded("RETURN apoc.text.lpad('a', 10000000, '-') AS value");
        assertMemoryLimitExceeded("RETURN apoc.text.rpad('a', 10000000, '-') AS value");
        assertMemoryLimitExceeded("RETURN apoc.text.lpad('a', 2147483647, '-') AS value");
        assertMemoryLimitExceeded("RETURN apoc.text.rpad('a', 2147483647, '-') AS value");
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void randomIsCharged() {
        assertMemoryLimitExceeded("RETURN apoc.text.random(10000000) AS value");
        assertMemoryLimitExceeded("RETURN apoc.text.random(2147483647) AS value");
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void formatIsChargedIncrementally() {
        assertMemoryLimitExceeded("RETURN apoc.text.format('%2000000000s', ['a']) AS value");
        // Each specifier on its own fits, so failing here can only come from accounting across the whole output
        testCall(
                db,
                "RETURN apoc.text.format('%400000s', ['a']) AS value",
                row -> assertEquals(400000, ((String) row.get("value")).length()));
        assertMemoryLimitExceeded("RETURN apoc.text.format('%400000s%400000s%400000s', ['a', 'b', 'c']) AS value");
    }

    @Test
    void formatSemanticsAreUnchanged() {
        testCall(
                db,
                "RETURN apoc.text.format('%d %s', [1, 'a']) AS value",
                row -> assertEquals("1 a", row.get("value")));
        testCall(
                db,
                "RETURN apoc.text.format('%1$s%1$s%1$s', ['x']) AS value",
                row -> assertEquals("xxx", row.get("value")));
    }

    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void chargeIsProportionalToTheRequestedSize() {
        // The same call that fails above succeeds once the budget is raised, so the charge cannot be a constant
        setTransactionMemoryLimit(mebiBytes(512));
        testCall(
                db,
                "RETURN apoc.text.repeat('a', 10000000) AS value",
                row -> assertEquals(10000000, ((String) row.get("value")).length()));
        testCall(
                db,
                "RETURN apoc.text.lpad('a', 10000000, '-') AS value",
                row -> assertEquals(10000000, ((String) row.get("value")).length()));
    }

    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void chargeIsReleasedWhenTheTransactionEnds() {
        setTransactionMemoryLimit(mebiBytes(512));
        for (int i = 0; i < 3; i++) {
            testCall(
                    db,
                    "RETURN apoc.text.repeat('a', 10000000) AS value",
                    row -> assertEquals(10000000, ((String) row.get("value")).length()));
        }
    }

    private void setTransactionMemoryLimit(long bytes) {
        db.getDependencyResolver()
                .resolveDependency(Config.class)
                .setDynamic(memory_transaction_max_size, bytes, getClass().getSimpleName());
    }

    /** The call must be refused by the transaction memory budget, with a message naming the breached setting. */
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
