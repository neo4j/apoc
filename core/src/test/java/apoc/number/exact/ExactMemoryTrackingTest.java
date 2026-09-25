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
package apoc.number.exact;

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;
import static org.neo4j.io.ByteUnit.mebiBytes;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.math.BigInteger;
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
 * Proves that the four arithmetic functions in {@code apoc.number.exact} charge the eventual plain-string length
 * before doing the arithmetic. See {@code apoc.text.StringsMemoryTrackingTest} for why the limit is 4 MiB.
 */
@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
class ExactMemoryTrackingTest {

    private static final String LOW_LIMIT = "4m";

    @Inject
    GraphDatabaseAPI db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfigRaw(Map.of("db.memory.transaction.max", LOW_LIMIT));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Exact.class);
    }

    @AfterEach
    void restoreLimit() {
        setTransactionMemoryLimit(mebiBytes(4));
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void hugeExponentsAreRejected() {
        // `1e1000000000` parses in microseconds, but toPlainString() on it expands into a two-gigabyte string.
        // mul and div share the sink even though the ticket names only add and sub.
        for (String function : List.of("add", "sub", "mul", "div")) {
            assertMemoryLimitExceeded("RETURN apoc.number.exact." + function + "('1e1000000000','1') AS value");
        }
        assertMemoryLimitExceeded("RETURN apoc.number.exact.add('1e9000000','1') AS value");
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void extremeScalesAreRejected() {
        // The reproduction from SURF-1429
        assertMemoryLimitExceeded("RETURN apoc.number.exact.add('1e2000000000','0') AS value");
        // Scale Integer.MIN_VALUE, whose int absolute value is still negative
        assertMemoryLimitExceeded("RETURN apoc.number.exact.add('1e2147483648','0') AS value");
        // A bounded precision caps the unscaled digits, not the scale, so it must not bypass the charge
        assertMemoryLimitExceeded("RETURN apoc.number.exact.mul('1e1000000000','1', 10) AS value");
        assertMemoryLimitExceeded("RETURN apoc.number.exact.div('1e1000000000','1', 10) AS value");
    }

    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void chargeIsProportionalToTheExponent() {
        assertMemoryLimitExceeded("RETURN apoc.number.exact.add('1e1000000','1') AS value");

        setTransactionMemoryLimit(mebiBytes(256));
        testCall(
                db,
                "RETURN apoc.number.exact.add('1e1000000','1') AS value",
                row -> assertEquals(1000001, ((String) row.get("value")).length()));
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void exactQuotientLongerThanBothOperandsIsCharged() {
        // 1 / 2^k has k digits, but 2^k has only ~0.3 * k, so a charge of both operands' lengths undercounts it.
        // With ~420,000 divisor digits that undercount still fits in 4 MiB next to the tracked parameter, while
        // the quotient's 1,400,000 digits do not.
        String divisor = BigInteger.ONE.shiftLeft(1_400_000).toString();
        assertMemoryLimitExceeded("RETURN apoc.number.exact.div('1', $divisor) AS value", Map.of("divisor", divisor));

        testCall(
                db,
                "RETURN apoc.number.exact.div('1', '1048576') AS value",
                row -> assertEquals("0.00000095367431640625", row.get("value")));
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void toIntegerAndToFloatAreNotCharged() {
        // Deliberately left alone: neither materialises a plain string, so neither has anything to charge
        testCall(
                db,
                "RETURN apoc.number.exact.toInteger('1e1000000000') AS value",
                row -> assertEquals(0L, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.exact.toFloat('1e1000000000') AS value",
                row -> assertEquals(Double.POSITIVE_INFINITY, row.get("value")));
    }

    @Test
    void smallArithmeticIsUnaffected() {
        testCall(db, "RETURN apoc.number.exact.add('1','1') AS value", row -> assertEquals("2", row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.exact.add('1e1000','1') AS value",
                row -> assertEquals(1001, ((String) row.get("value")).length()));
    }

    private void setTransactionMemoryLimit(long bytes) {
        db.getDependencyResolver()
                .resolveDependency(Config.class)
                .setDynamic(memory_transaction_max_size, bytes, getClass().getSimpleName());
    }

    private void assertMemoryLimitExceeded(String query) {
        assertMemoryLimitExceeded(query, Map.of());
    }

    private void assertMemoryLimitExceeded(String query, Map<String, Object> params) {
        QueryExecutionException e =
                assertThrows(QueryExecutionException.class, () -> testCall(db, query, params, row -> {}), query);
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
