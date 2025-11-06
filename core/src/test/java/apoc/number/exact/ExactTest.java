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
import static org.junit.jupiter.api.Assertions.assertNull;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension()
public class ExactTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Exact.class);
    }

    @Test
    public void testAdd() {
        testCall(
                db,
                "return apoc.number.exact.add('1213669989','1238126387') as value",
                row -> assertEquals("2451796376", row.get("value")));
    }

    @Test
    public void testAddNull() {
        testCall(db, "return apoc.number.exact.add(null,'1238126387') as value", row -> assertNull(row.get("value")));
    }

    @Test
    public void testSub() {
        testCall(
                db,
                "return apoc.number.exact.sub('1238126387','1213669989') as value",
                row -> assertEquals("24456398", row.get("value")));
    }

    @Test
    public void testMul() {
        testCall(
                db,
                "return apoc.number.exact.mul('550058444','662557', 15, 'HALF_DOWN') as value",
                row -> assertEquals("364445072481308", row.get("value")));
    }

    @Test
    public void testDiv() {
        testCall(
                db,
                "return apoc.number.exact.div('550058444','662557', 18, 'HALF_DOWN') as value",
                row -> assertEquals("830.205467605051339", row.get("value")));
    }

    @Test
    public void testToInteger() {
        testCall(
                db,
                "return apoc.number.exact.toInteger('504238974', 5, 'HALF_DOWN') as value",
                row -> assertEquals(504238974L, row.get("value")));
    }

    @Test
    public void testToFloat() {
        testCall(
                db,
                "return apoc.number.exact.toFloat('50423.1656', 10, null) as value",
                row -> assertEquals(50423.1656, row.get("value")));
    }

    @Test
    public void testToExact() {
        testCall(
                db,
                "return apoc.number.exact.toExact(521468545698447) as value",
                row -> assertEquals(Long.valueOf("521468545698447"), row.get("value")));
    }

    @Test
    public void testPrec() {
        testCall(
                db,
                "return apoc.number.exact.mul('550058444','662557', 5, 'HALF_DOWN') as value",
                row -> assertEquals("364450000000000", row.get("value")));
    }

    @Test
    public void testRound() {
        testCall(
                db,
                "return apoc.number.exact.mul('550058444','662557', 10, 'DOWN') as value",
                row -> assertEquals("364445072400000", row.get("value")));
    }

    @Test
    public void testMulWithoutOptionalParams() {
        testCall(
                db,
                "return apoc.number.exact.mul('550058444','662557') as value",
                row -> assertEquals("364445072481308", row.get("value")));
    }

    @Test
    public void testAddScientificNotation() {
        testCall(
                db,
                "return apoc.number.exact.add('1E6','1E6') as value",
                row -> assertEquals("2000000", row.get("value")));
    }
}
