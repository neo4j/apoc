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
package apoc.agg;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension
public class CollAggregationTest {
    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, CollAggregation.class);
    }

    @Test
    public void testNth() {
        testCall(
                db,
                "UNWIND RANGE(0,10) as value RETURN apoc.agg.nth(value, 0) as first, apoc.agg.nth(value, 3) as third,apoc.agg.nth(value, -1) as last",
                (row) -> {
                    assertEquals(0L, row.get("first"));
                    assertEquals(3L, row.get("third"));
                    assertEquals(10L, row.get("last"));
                });
    }

    @Test
    public void testFirst() {
        testCall(
                db,
                "UNWIND RANGE(0,10) as value RETURN apoc.agg.first(value) as first",
                (row) -> assertEquals(0L, row.get("first")));
        testCall(
                db,
                "UNWIND [null,42,43,null] as value RETURN apoc.agg.first(value) as first",
                (row) -> assertEquals(42L, row.get("first")));
    }

    @Test
    public void testLast() {
        testCall(
                db,
                "UNWIND RANGE(0,10) as value RETURN apoc.agg.last(value) as last",
                (row) -> assertEquals(10L, row.get("last")));
        testCall(
                db,
                "UNWIND [null,41,null,42,null] as value RETURN apoc.agg.last(value) as last",
                (row) -> assertEquals(42L, row.get("last")));
    }

    @Test
    public void testSlice() {
        testCall(
                db,
                "UNWIND RANGE(0,10) as value RETURN apoc.agg.slice(value,1,3) as slice",
                (row) -> assertEquals(asList(1L, 2L, 3L), row.get("slice")));
        testCall(
                db,
                "UNWIND RANGE(0,3) as value RETURN apoc.agg.slice(value) as slice",
                (row) -> assertEquals(asList(0L, 1L, 2L, 3L), row.get("slice")));
        testCall(
                db,
                "UNWIND RANGE(0,3) as value RETURN apoc.agg.slice(value,2) as slice",
                (row) -> assertEquals(asList(2L, 3L), row.get("slice")));
        testCall(
                db,
                "UNWIND [null,41,null,42,null,43,null] as value RETURN apoc.agg.slice(value,1,3) as slice",
                (row) -> assertEquals(asList(42L, 43L), row.get("slice")));
    }
}
