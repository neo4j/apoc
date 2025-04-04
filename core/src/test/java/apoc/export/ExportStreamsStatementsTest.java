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
package apoc.export;

import static apoc.ApocConfig.EXPORT_TO_FILE_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.json.ExportJson;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ExportStreamsStatementsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    newBuilder("internal.dbms.debug.track_cursor_close", BOOL, false)
                            .build(),
                    false)
            .withSetting(
                    newBuilder("internal.dbms.debug.trace_cursors", BOOL, false).build(), false);

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportCSV.class, ExportCypher.class, ExportJson.class);
        db.executeTransactionally(
                "CREATE (f:User:Customer {name:'Foo', age:42})-[:BOUGHT]->(b:Product {name:'Apple Watch Series 4'})");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void shouldStreamCSVData() {
        String expected = String.format("\"_id\",\"_labels\",\"age\",\"name\",\"_start\",\"_end\",\"_type\"%n"
                + "\"0\",\":Customer:User\",\"42\",\"Foo\",,,%n"
                + "\"1\",\":Product\",\"\",\"Apple Watch Series 4\",,,%n"
                + ",,,,\"0\",\"1\",\"BOUGHT\"%n");
        String statement = "CALL apoc.export.csv.all(null,{stream:true})";
        TestUtil.testCall(db, statement, (res) -> assertEquals(expected, res.get("data")));
    }

    @Test
    public void shouldNotExportCSVData() {
        String statement = "CALL apoc.export.csv.all('file.csv', {})";

        RuntimeException e = assertThrows(RuntimeException.class, () -> TestUtil.testCall(db, statement, (res) -> {}));

        String expectedMessage = "Failed to invoke procedure `apoc.export.csv.all`: "
                + "Caused by: java.lang.RuntimeException: " + EXPORT_TO_FILE_ERROR;
        assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    public void shouldStreamCypherStatements() {
        String expected = String.format(":begin%n"
                + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n"
                + ":commit%n"
                + "CALL db.awaitIndexes(300);%n"
                + ":begin%n"
                + "UNWIND [{_id:1, properties:{name:\"Apple Watch Series 4\"}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Product;%n"
                + "UNWIND [{_id:0, properties:{name:\"Foo\", age:42}}] AS row%n"
                + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:User:Customer;%n"
                + ":commit%n"
                + ":begin%n"
                + "UNWIND [{start: {_id:0}, end: {_id:1}, properties:{}}] AS row%n"
                + "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n"
                + "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})%n"
                + "CREATE (start)-[r:BOUGHT]->(end) SET r += row.properties;%n"
                + ":commit%n"
                + ":begin%n"
                + "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n"
                + ":commit%n"
                + ":begin%n"
                + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;%n"
                + ":commit%n");
        String statement = "CALL apoc.export.cypher.all(null,{streamStatements:true})";
        TestUtil.testCall(db, statement, (res) -> assertEquals(expected, res.get("cypherStatements")));
    }

    @Test
    public void shouldNotExportCypherStatements() {
        String statement = "CALL apoc.export.cypher.all('file.cypher', {})";

        RuntimeException e = assertThrows(RuntimeException.class, () -> TestUtil.testCall(db, statement, (res) -> {}));
        String expectedMessage = "Failed to invoke procedure `apoc.export.cypher.all`: "
                + "Caused by: java.lang.RuntimeException: " + EXPORT_TO_FILE_ERROR;
        assertEquals(expectedMessage, e.getMessage());
    }
}
