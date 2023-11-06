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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.create.Create;
import apoc.diff.Diff;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 06.11.16
 */
public class HelpTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, Help.class, BitwiseOperations.class, Coll.class, Diff.class, Create.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void info() {
        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "bitwise"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.bitwise.op", row.get("name"));
            assertTrue(((String) row.get("text")).contains("bitwise operation"));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(
                db,
                "CALL apoc.help($text)",
                map("text", "operation+"),
                (row) -> assertEquals("apoc.bitwise.op", row.get("name")));
        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "toSet"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.coll.toSet", row.get("name"));
            assertTrue(((String) row.get("text")).contains("unique `LIST<ANY>`"));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "diff.nodes"), (row) -> {
            assertEquals("function", row.get("type"));
            assertEquals("apoc.diff.nodes", row.get("name"));
            assertTrue(((String) row.get("text"))
                    .contains("Returns a `MAP` detailing the differences between the two given `NODE` values."));
            assertFalse(((Boolean) row.get("isDeprecated")));
        });
        TestUtil.testCall(db, "CALL apoc.help($text)", map("text", "apoc.create.uuids"), (row) -> {
            assertEquals("procedure", row.get("type"));
            assertEquals("apoc.create.uuids", row.get("name"));
            assertTrue(((String) row.get("text")).contains("Returns a stream of UUIDs."));
            assertTrue(((Boolean) row.get("isDeprecated")));
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
