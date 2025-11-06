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
package apoc.number;

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
public class NumbersTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Numbers.class);
    }

    @Test
    public void testFormat() {
        testCall(db, "RETURN apoc.number.format(12345) AS value", row -> assertEquals("12,345", row.get("value")));
        testCall(db, "RETURN apoc.number.format('aaa') AS value", row -> assertNull(row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.format(12345, '', 'it') AS value",
                row -> assertEquals("12.345", row.get("value")));
        testCall(db, "RETURN apoc.number.format(12345, '', 'apoc') AS value", row -> assertNull(row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.format(12345, '#,##0.00;(#,##0.00)') AS value",
                row -> assertEquals("12,345.00", row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.format(12345, '#,##0.00;(#,##0.00)', 'it') AS value",
                row -> assertEquals("12.345,00", row.get("value")));
        testCall(
                db, "RETURN apoc.number.format(12345.67) AS value", row -> assertEquals("12,345.67", row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.format(12345.67, '', 'it') AS value",
                row -> assertEquals("12.345,67", row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.format(12345.67, '#,##0.00;(#,##0.00)') AS value",
                row -> assertEquals("12,345.67", row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.format(12345.67, '#,##0.00;(#,##0.00)', 'it') AS value",
                row -> assertEquals("12.345,67", row.get("value")));
    }

    @Test
    public void testParseInt() {
        testCall(db, "RETURN apoc.number.parseInt('12,345') AS value", row -> assertEquals(12345L, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.parseInt('12.345', '' ,'it') AS value",
                row -> assertEquals(12345L, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.parseInt('12,345', '#,##0.00;(#,##0.00)') AS value",
                row -> assertEquals(12345L, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.parseInt('12.345', '#,##0.00;(#,##0.00)', 'it') AS value",
                row -> assertEquals(12345L, row.get("value")));
        testCall(db, "RETURN apoc.number.parseInt('aaa') AS value", row -> assertNull(row.get("value")));
        testCall(db, "RETURN apoc.number.parseInt(null) AS value", row -> assertNull(row.get("value")));
    }

    // Parse Double

    @Test
    public void testParseFloat() {
        testCall(
                db,
                "RETURN apoc.number.parseFloat('12,345.67') AS value",
                row -> assertEquals(12345.67, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.parseFloat('12.345,67', '', 'it') AS value",
                row -> assertEquals(12345.67, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.parseFloat('12,345.67', '#,##0.00;(#,##0.00)') AS value",
                row -> assertEquals(12345.67, row.get("value")));
        testCall(
                db,
                "RETURN apoc.number.parseFloat('12.345,67', '#,##0.00;(#,##0.00)', 'it') AS value",
                row -> assertEquals(12345.67, row.get("value")));
        testCall(db, "RETURN apoc.number.parseFloat('aaa') AS value", row -> assertNull(row.get("value")));
        testCall(db, "RETURN apoc.number.parseFloat(null) AS value", row -> assertNull(row.get("value")));
    }
}
