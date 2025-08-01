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
package apoc.bitwise;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension()
public class BitwiseOperationsTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, BitwiseOperations.class);
    }

    public static final String BITWISE_CALL = "return apoc.bitwise.op($a,$op,$b) as value";

    public void testOperation(String op, long expected, int a, int b) {
        Map<String, Object> params = map("a", a, "op", op, "b", b);
        testCall(db, BITWISE_CALL, params, (row) -> assertEquals(expected, row.get("value"), "operation " + op));
    }

    @Test
    public void testOperations() {
        int a = 0b0011_1100;
        int b = 0b0000_1101;
        testOperation("&", 12L, a, b);
        testOperation("AND", 12L, a, b);
        testOperation("OR", 61L, a, b);
        testOperation("|", 61L, a, b);
        testOperation("^", 49L, a, b);
        testOperation("XOR", 49L, a, b);
        testOperation("~", -61L, a, b);
        testOperation("NOT", -61L, a, b);
    }

    @Test
    public void testOperations2() {
        int a = 0b0011_1100;
        int b = 2;
        testOperation("<<", 240L, a, b);
        testOperation("left shift", 240L, a, b);
        testOperation(">>", 15L, a, b);
        testOperation("right shift", 15L, a, b);
        testOperation("right shift unsigned", 15L, a, b);
        testOperation(">>>", 15L, a, b);
    }
}
