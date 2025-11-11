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
package apoc.help;

import static apoc.util.Util.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.create.Create;
import apoc.diff.Diff;
import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(createDatabasePerTest = false)
public class HelpTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Help.class, BitwiseOperations.class, Coll.class, Diff.class, Create.class);
    }

    @Test
    public void infoCypher5() {
        TestUtil.testCall(db, "CYPHER 5 CALL apoc.help($text)", map("text", "bitwise"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.bitwise.op", row.get("name"));
            assertTrue(((String) row.get("text")).contains("bitwise operation"));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(
                db,
                "CYPHER 5 CALL apoc.help($text)",
                map("text", "operation+"),
                (row) -> assertEquals("apoc.bitwise.op", row.get("name")));
        TestUtil.testCall(db, "CYPHER 5 CALL apoc.help($text)", map("text", "toSet"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.coll.toSet", row.get("name"));
            assertTrue(((String) row.get("text")).contains("unique `LIST<ANY>`"));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(db, "CYPHER 5 CALL apoc.help($text)", map("text", "diff.nodes"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.diff.nodes", row.get("name"));
            assertTrue(((String) row.get("text"))
                    .contains("Returns a `MAP` detailing the differences between the two given `NODE` values."));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(db, "CYPHER 5 CALL apoc.help($text)", map("text", "apoc.create.uuids"), (row) -> {
            assertEquals("procedure", row.get("type"));
            assertEquals("apoc.create.uuids", row.get("name"));
            assertTrue(((String) row.get("text")).contains("Returns a stream of UUIDs."));
            assertTrue(((Boolean) row.get("isDeprecated")));
        });
    }

    @Test
    public void infoCypher25() {
        TestUtil.testCall(db, "CYPHER 25 CALL apoc.help($text)", map("text", "bitwise"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.bitwise.op", row.get("name"));
            assertTrue(((String) row.get("text")).contains("bitwise operation"));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(
                db,
                "CYPHER 25 CALL apoc.help($text)",
                map("text", "operation+"),
                (row) -> assertEquals("apoc.bitwise.op", row.get("name")));
        TestUtil.testCall(db, "CYPHER 25 CALL apoc.help($text)", map("text", "toSet"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.coll.toSet", row.get("name"));
            assertTrue(((String) row.get("text")).contains("unique `LIST<ANY>`"));
            assertTrue(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(db, "CYPHER 25 CALL apoc.help($text)", map("text", "diff.nodes"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.diff.nodes", row.get("name"));
            assertTrue(((String) row.get("text"))
                    .contains("Returns a `MAP` detailing the differences between the two given `NODE` values."));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
    }

    @Test
    public void indicateCore() {
        TestUtil.testCall(
                db,
                "CALL apoc.help($text)",
                map("text", "coll.zipToRows"),
                (row) -> assertEquals(true, row.get("core")));
    }
}
