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
package apoc.neighbors;

import static apoc.ApocConfig.APOC_MAX_HOPS;
import static apoc.ApocConfig.DEFAULT_MAX_HOPS;
import static apoc.ApocConfig.apocConfig;
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
import org.junit.jupiter.api.AfterAll;
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
 * Proves that the per-hop bitmaps of {@code apoc.neighbors} are charged to the transaction memory tracker, so a deep
 * search with a legal distance is still bounded - the case {@code apoc.max.hops} alone cannot cover. See
 * {@code apoc.text.StringsMemoryTrackingTest} for why the limit is 4 MiB; below 2 MiB even starting a transaction is
 * refused, before any procedure runs.
 *
 * <p>Roaring bitmaps are compact, about 130 bytes per hop on a simple path, so the path is 100,000 hops long (about
 * 13 MB charged) and {@code apoc.max.hops} is raised to match for the duration of this class.
 */
@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure", createDatabasePerTest = false)
class NeighborsMemoryTrackingTest {

    private static final String LOW_LIMIT = "4m";
    private static final int PATH_LENGTH = 100_000;

    @Inject
    GraphDatabaseAPI db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfigRaw(Map.of("db.memory.transaction.max", LOW_LIMIT));
    }

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Neighbors.class);
        apocConfig().setProperty(APOC_MAX_HOPS, PATH_LENGTH);
        setTransactionMemoryLimit(mebiBytes(256));
        db.executeTransactionally(
                "CREATE (:Start) WITH 1 AS one UNWIND range(1, $length) AS i CREATE (:Step {i: i})",
                Map.of("length", PATH_LENGTH));
        db.executeTransactionally("MATCH (start:Start), (s:Step {i: 1}) CREATE (start)-[:NEXT]->(s)");
        db.executeTransactionally("MATCH (s:Step) WITH s ORDER BY s.i WITH collect(s) AS steps "
                + "UNWIND range(0, size(steps) - 2) AS i WITH steps[i] AS a, steps[i + 1] AS b "
                + "CREATE (a)-[:NEXT]->(b)");
        setTransactionMemoryLimit(mebiBytes(4));
    }

    @AfterAll
    void restoreMaxHops() {
        apocConfig().setProperty(APOC_MAX_HOPS, DEFAULT_MAX_HOPS);
    }

    @AfterEach
    void restoreLimit() {
        setTransactionMemoryLimit(mebiBytes(4));
    }

    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void deepSearchIsChargedPerHop() {
        // Too much for a 4 MiB budget and comfortable within a 256 MiB one, so the charge cannot be a constant
        for (String procedure : List.of("tohop.count", "byhop.count", "athop.count")) {
            String query = "MATCH (n:Start) CALL apoc.neighbors." + procedure + "(n, 'NEXT>', $distance) YIELD value "
                    + "RETURN value";
            setTransactionMemoryLimit(mebiBytes(4));
            assertMemoryLimitExceeded(query);

            setTransactionMemoryLimit(mebiBytes(256));
            testCall(db, query, Map.of("distance", PATH_LENGTH), row -> {});
        }
    }

    @Test
    void shallowSearchIsUnaffected() {
        testCall(
                db,
                "MATCH (n:Start) CALL apoc.neighbors.byhop.count(n, 'NEXT>', 3) YIELD value RETURN value",
                row -> assertEquals(List.of(1L, 1L, 1L), row.get("value")));
    }

    private void setTransactionMemoryLimit(long bytes) {
        db.getDependencyResolver()
                .resolveDependency(Config.class)
                .setDynamic(memory_transaction_max_size, bytes, getClass().getSimpleName());
    }

    private void assertMemoryLimitExceeded(String query) {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> testCall(db, query, Map.of("distance", PATH_LENGTH), row -> {}),
                query);
        assertTrue(
                TestUtil.hasCauses(e, MemoryLimitExceededException.class),
                query + " should fail with MemoryLimitExceededException, but failed with: " + e.getMessage());
        assertTrue(
                e.getMessage().contains("memory.transaction"),
                query + " should name the breached setting, but failed with: " + e.getMessage());
    }
}
