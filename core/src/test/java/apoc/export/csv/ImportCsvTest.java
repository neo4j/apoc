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
package apoc.export.csv;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import apoc.csv.CsvTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.TestUtil;
import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.Values;

public class ImportCsvTest {
    public static final String BASE_URL_FILES = "src/test/resources/csv-inputs";
    private static final ZoneId DEFAULT_TIMEZONE = ZoneId.of("Asia/Tokyo");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.db_temporal_timezone, DEFAULT_TIMEZONE)
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    new File(BASE_URL_FILES).toPath().toAbsolutePath());

    final Map<String, String> testCsvs = Collections.unmodifiableMap(Stream.of(
                    new AbstractMap.SimpleEntry<>(
                            "array",
                            """
                            :ID|name:STRING[]
                            1|John;Bob;Alice
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-basic-affiliated-with",
                            """
                            :START_ID,:END_ID
                            1,3
                            2,4
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-basic-companies",
                            """
                            companyId:ID,name:STRING
                            4,Neo4j
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-basic-persons",
                            """
                            personId:ID,name:STRING
                            1,John
                            2,Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-basic-unis",
                            """
                            uniId:ID,name:STRING
                            3,TU Munich
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-idspaces-affiliated-with",
                            """
                            :START_ID(Person),:END_ID(Organisation)
                            1,1
                            2,2
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-idspaces-companies",
                            """
                            companyId:ID(Organisation),name:STRING
                            2,Neo4j
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-idspaces-persons",
                            """
                            personId:ID(Person),name:STRING
                            1,John
                            2,Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "custom-ids-idspaces-unis",
                            """
                            uniId:ID(Organisation),name:STRING
                            1,TU Munich
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "id-idspaces",
                            """
                            :ID(Person)|name:STRING
                            1|John
                            2|Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "id-idspaces-with-dash",
                            """
                            :ID(Person-Id)|name:STRING
                            1|John
                            2|Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "id",
                            """
                            id:ID|name:STRING
                            1|John
                            2|Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "csvPoint",
                            """
                                    :ID,location:point{crs:WGS-84}
                                    1,"{latitude:55.6121514, longitude:12.9950357}"
                                    2,"{y:51.507222, x:-0.1275}"
                                    3,"{latitude:37.554167, longitude:-122.313056, height: 100, crs:'WGS-84-3D'}"
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "nodesMultiTypes",
                            """
                                    :ID(MultiType-ID)|date1:datetime{timezone:Europe/Stockholm}|date2:datetime|foo:string|joined:date|active:boolean|points:int
                                    1|2018-05-10T10:30|2018-05-10T12:30|Joe Soap|2017-05-05|true|10
                                    2|2018-05-10T10:30[Europe/Berlin]|2018-05-10T12:30[Europe/Berlin]|Jane Doe|2017-08-21|true|15
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "emptyDate",
                            """
                                    id:ID,:LABEL,str:STRING,int:INT,date:DATE
                                    1,Lab,hello,1,2020-01-01
                                    2,Lab,world,2,2020-01-01
                                    3,Lab,,,
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "relMultiTypes",
                            """
                                    :START_ID(MultiType-ID)|:END_ID(MultiType-ID)|prop1:IGNORE|prop2:time{timezone:+02:00}[]|foo:int|time:duration[]|baz:localdatetime[]|bar:localtime[]
                                    1|2|a|15:30|1|P14DT16H12M|2020-01-01T00:00:00|11:00:00
                                    2|1|b|15:30+01:00|2|P5M1.5D|2021|12:00:00
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "id-with-duplicates",
                            """
                            id:ID|name:STRING
                            1|John
                            1|Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "ignore-nodes",
                            """
                            :ID|firstname:STRING|lastname:IGNORE|age:INT
                            1|John|Doe|25
                            2|Jane|Doe|26
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "ignore-relationships",
                            """
                            :START_ID|:END_ID|prop1:IGNORE|prop2:INT
                            1|2|a|3
                            2|1|b|6
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "label",
                            """
                            :ID|:LABEL|name:STRING
                            1|Student;Employee|John
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "knows",
                            """
                            :START_ID,:END_ID,since:INT
                            1,2,2016
                            10,11,2014
                            11,12,2013"""),
                    new AbstractMap.SimpleEntry<>(
                            "persons",
                            """
                            :ID,name:STRING,speaks:STRING[]
                            1,John,"en,fr"
                            2,Jane,"en,de\""""),
                    new AbstractMap.SimpleEntry<>(
                            "quoted",
                            """
                            id:ID|:LABEL|name:STRING
                            '1'|'Student:Employee'|'John'
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "rel-on-ids-idspaces",
                            """
                            :START_ID(Person)|:END_ID(Person)|since:INT
                            1|2|2016
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "rel-on-ids",
                            """
                            x:START_ID|:END_ID|since:INT
                            1|2|2016
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "rel-type",
                            """
                            :START_ID|:END_ID|:TYPE|since:INT
                            1|2|FRIENDS_WITH|2016
                            2|1||2016
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "typeless",
                            """
                            :ID|name
                            1|John
                            2|Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "personsWithoutIdField",
                            """
                            name:STRING
                            John
                            Jane
                            """),
                    new AbstractMap.SimpleEntry<>(
                            "emptyInteger",
                            """
                                    :ID(node_space_1),:LABEL,str_attribute:STRING,int_attribute:INT,int_attribute_array:INT[],double_attribute_array:FLOAT[]
                                    n1,Thing,once upon a time,1,"2;3","2.3;3.5"
                                    n2,Thing,,2,"4;5","2.6;3.6"
                                    n3,Thing,,,,
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "emptyArray",
                            """
                                    id:ID,:LABEL,arr:STRING[],description:STRING
                                    1,Arrays,a;b;c;d;e,normal,
                                    2,Arrays,,withNull
                                    3,Arrays,a;;c;;e,withEmptyItem
                                    4,Arrays,a; ;c; ;e,withBlankItem
                                    5,Arrays, ,withWhiteSpace
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "nodesWithSpecialCharInID",
                            """
                                    node_code:ID(node_code),:LABEL
                                    806^04^150\\\\^123456,Person
                                    2,Cat
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "relsWithSpecialCharInID",
                            """
                                    :START_ID(node_code),:END_ID(node_code),:TYPE
                                    806^04^150\\\\^123456,2,FRIENDS_WITH
                                    """),
                    new AbstractMap.SimpleEntry<>(
                            "withDifferentTypes",
                            """
                            id:ID|name:STRING|age:double|chipID:long|:LABEL
                            1|Maja|0.5|1236|Cat
                            2|Pelle|0.5|1345|Cat
                            """))
            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)));

    @Before
    public void setUp() throws IOException {
        for (Map.Entry<String, String> entry : testCsvs.entrySet()) {
            CsvTestUtil.saveCsvFile(entry.getKey(), entry.getValue());
        }

        TestUtil.registerProcedure(db, ImportCsv.class, ExportCSV.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testImportCsvLargeFile() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [], $config)",
                map("nodeFile", "file:/largeFile.csv", "config", map("batchSize", 100L)),
                (r) -> assertEquals(664850L, r.get("nodes")));
    }

    @Test
    public void testImportCsvTerminate() {
        checkTerminationGuard(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [], $config)",
                map("nodeFile", "file:/largeFile.csv", "config", map("batchSize", 100L)));
    }

    @Test
    public void testNodesWithIds() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/id.csv", "config", map("delimiter", '|', "stringIds", false)),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertTrue(names.containsAll(List.of("Jane", "John")));

        List<Long> ids = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.id AS id ORDER BY id");
        assertTrue(ids.containsAll(List.of(1L, 2L)));
    }

    @Test
    public void testImportCsvWithSkipLines() {
        // skip only-header (default config)
        testSkipLine(1L, 2);

        // skip header and another one
        testSkipLine(2L, 1);

        // skip header and another two (no result because the file has 3 lines)
        testSkipLine(3L, 0);
    }

    private void testSkipLine(long skipLine, int nodes) {
        TestUtil.testCall(
                db,
                "call apoc.import.csv([{fileName: 'id-idspaces.csv', labels: ['SkipLine']}], [], $config)",
                map("config", map("delimiter", '|', "skipLines", skipLine)),
                (r) -> assertEquals((long) nodes, r.get("nodes")));

        TestUtil.testCallCount(db, "MATCH (n:SkipLine) RETURN n", nodes);

        db.executeTransactionally("MATCH (n:SkipLine) DETACH DELETE n");
    }

    @Test
    public void issue2826WithImportCsv() {
        db.executeTransactionally("CREATE (n:Person {name: 'John'})");
        db.executeTransactionally("CREATE CONSTRAINT unique_person FOR (n:Person) REQUIRE n.name IS UNIQUE");
        try {
            TestUtil.testCall(
                    db,
                    "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                    map("file", "file:/id.csv", "config", map("delimiter", '|')),
                    (r) -> fail());
        } catch (RuntimeException e) {
            String expected = "Failed to invoke procedure `apoc.import.csv`: "
                    + "Caused by: IndexEntryConflictException{propertyValues=( String(\"John\") ), addedEntityId=-1, existingEntityId=0}";
            assertEquals(expected, e.getMessage());
        }

        // should return only 1 node due to constraint exception
        TestUtil.testCall(
                db,
                "MATCH (n:Person) RETURN properties(n) AS props",
                r -> assertEquals(Map.of("name", "John"), r.get("props")));

        db.executeTransactionally("DROP CONSTRAINT unique_person");
    }

    @Test
    public void testNodesAndRelsWithMultiTypes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile",
                        "file:/nodesMultiTypes.csv",
                        "relFile",
                        "file:/relMultiTypes.csv",
                        "config",
                        map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                });
        TestUtil.testCall(
                db,
                "MATCH p=(start:Person)-[rel {foo: 1}]->(end:Person)-[relSecond {foo:2}]->(start) RETURN start, end, rel, relSecond",
                r -> {
                    final Map<String, Object> expectedStart = Map.of(
                            "joined",
                            LocalDate.of(2017, 5, 5),
                            "foo",
                            "Joe Soap",
                            "active",
                            true,
                            "date2",
                            ZonedDateTime.of(2018, 5, 10, 12, 30, 0, 0, DEFAULT_TIMEZONE),
                            "date1",
                            ZonedDateTime.of(2018, 5, 10, 10, 30, 0, 0, ZoneId.of("Europe/Stockholm")),
                            "points",
                            10L,
                            "__csv_id",
                            "1");
                    assertEquals(expectedStart, ((NodeEntity) r.get("start")).getAllProperties());

                    final Map<String, Object> expectedEnd = Map.of(
                            "joined",
                            LocalDate.of(2017, 8, 21),
                            "foo",
                            "Jane Doe",
                            "active",
                            true,
                            "date2",
                            ZonedDateTime.of(2018, 5, 10, 12, 30, 0, 0, ZoneId.of("Europe/Berlin")),
                            "date1",
                            ZonedDateTime.of(2018, 5, 10, 10, 30, 0, 0, ZoneId.of("Europe/Berlin")),
                            "points",
                            15L,
                            "__csv_id",
                            "2");
                    assertEquals(expectedEnd, ((NodeEntity) r.get("end")).getAllProperties());

                    final RelationshipEntity rel = (RelationshipEntity) r.get("rel");
                    assertEquals(DurationValue.parse("P14DT16H12M"), ((DurationValue[]) rel.getProperty("time"))[0]);
                    final List<Object> expectedTime = List.of(OffsetTime.of(15, 30, 0, 0, ZoneOffset.of("+02:00")));
                    assertEquals(expectedTime, asList((OffsetTime[]) rel.getProperty("prop2")));
                    final List<LocalDateTime> expectedBaz = List.of(LocalDateTime.of(2020, 1, 1, 0, 0));
                    assertEquals(expectedBaz, asList((LocalDateTime[]) rel.getProperty("baz")));
                    assertEquals(LocalTime.of(11, 0, 0), ((LocalTime[]) rel.getProperty("bar"))[0]);
                    assertEquals(1L, rel.getProperty("foo"));
                    final RelationshipEntity relSecond = (RelationshipEntity) r.get("relSecond");
                    assertEquals(DurationValue.parse("P5M1.5D"), ((DurationValue[]) relSecond.getProperty("time"))[0]);
                    final List<Object> expectedTimeRelSecond =
                            List.of(OffsetTime.of(15, 30, 0, 0, ZoneOffset.of("+01:00")));
                    assertEquals(expectedTimeRelSecond, asList((OffsetTime[]) relSecond.getProperty("prop2")));
                    final List<LocalDateTime> expectedBazRelSecond = List.of(LocalDateTime.of(2021, 1, 1, 0, 0));
                    assertEquals(expectedBazRelSecond, asList((LocalDateTime[]) relSecond.getProperty("baz")));
                    assertEquals(LocalTime.of(12, 0, 0), ((LocalTime[]) relSecond.getProperty("bar"))[0]);
                    assertEquals(2L, relSecond.getProperty("foo"));
                });
    }

    @Test
    public void testNodesWithPoints() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Point']}], [], {})",
                map("file", "file:/csvPoint.csv"),
                (r) -> {
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });
        TestUtil.testResult(db, "MATCH (n:Point) RETURN n ORDER BY n.id", r -> {
            final ResourceIterator<Node> iterator = r.columnAs("n");
            final NodeEntity first = (NodeEntity) iterator.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, 12.9950357, 55.6121514),
                    first.getProperty("location"));
            final NodeEntity second = (NodeEntity) iterator.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.WGS_84, -0.1275, 51.507222),
                    second.getProperty("location"));
            final NodeEntity third = (NodeEntity) iterator.next();
            assertEquals(
                    Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, -122.313056, 37.554167, 100D),
                    third.getProperty("location"));
        });
    }

    @Test
    public void testCallAsString() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv(" + "[{fileName: 'file:/quoted.csv', labels: ['Person']}], "
                        + "[], "
                        + "{delimiter: '|', arrayDelimiter: ':', quotationCharacter: '\\'', stringIds: false})",
                map(),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        Assert.assertEquals(
                "John",
                TestUtil.<String>singleResultFirstColumn(
                        db, "MATCH (n:Person:Student:Employee) RETURN n.name AS name ORDER BY name"));

        long id = TestUtil.<Long>singleResultFirstColumn(
                db, "MATCH (n:Person:Student:Employee) RETURN n.id AS id ORDER BY id");
        Assert.assertEquals(1L, id);
    }

    @Test
    public void testNodesWithIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/id-idspaces.csv", "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertTrue(names.containsAll(List.of("Jane", "John")));
    }

    @Test
    public void testNodesWithIdSpacesWithDoubleDash() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file://id-idspaces-with-dash.csv", "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertTrue(names.containsAll(List.of("Jane", "John")));
    }

    @Test
    public void testNodesWithIdSpacesWithTripleDash() {
        db.executeTransactionally(
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:///id-idspaces-with-dash.csv", "config", map("delimiter", '|')),
                Result::resultAsString);
    }

    @Test
    public void testNodesWithIdSpacesWithDash() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/id-idspaces-with-dash.csv", "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertTrue(names.containsAll(List.of("Jane", "John")));
    }

    @Test
    public void testCustomLabels() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/label.csv", "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });
        List<String> names =
                TestUtil.firstColumn(db, "MATCH (n) UNWIND labels(n) AS label RETURN label ORDER BY label");
        assertTrue(names.containsAll(List.of("Employee", "Person", "Student")));
    }

    @Test
    public void testArray() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/array.csv", "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names =
                TestUtil.firstColumn(db, "MATCH (n:Person) UNWIND n.name AS name RETURN name ORDER BY name");
        assertTrue(names.containsAll(List.of("Alice", "Bob", "John")));
    }

    @Test
    public void testDefaultTypedField() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/typeless.csv", "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertTrue(names.containsAll(List.of("Jane", "John")));
    }

    @Test
    public void testCsvWithoutIdField() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map("file", "file:/personsWithoutIdField.csv", "config", map()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        List<String> names = TestUtil.firstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name");
        assertEquals(List.of("Jane", "John"), names);
    }

    @Test
    public void testCustomRelationshipTypes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile", "file:/id.csv",
                        "relFile", "file:/rel-type.csv",
                        "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                });

        Assert.assertEquals(
                "John Jane",
                TestUtil.singleResultFirstColumn(
                        db,
                        "MATCH (p1:Person)-[:FRIENDS_WITH]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
        Assert.assertEquals(
                "Jane John",
                TestUtil.singleResultFirstColumn(
                        db,
                        "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
    }

    @Test
    public void testWithSpecialEscapes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'FRIENDS_WITH'}], {ignoreDuplicateNodes: true})",
                map(
                        "nodeFile", "file:/nodesWithSpecialCharInID.csv",
                        "relFile", "file:/relsWithSpecialCharInID.csv"),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });

        Assert.assertEquals(
                "806^04^150\\\\^123456 2",
                TestUtil.singleResultFirstColumn(
                        db,
                        "MATCH (p1:Person)-[:FRIENDS_WITH]->(p2:Person) RETURN p1.node_code + ' ' + p2.node_code AS pair ORDER BY pair"));
    }

    @Test
    public void testEmptyDate() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: 'file:/emptyDate.csv', labels: ['Entity']}], [], {date: {nullValues: ['']}})",
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (node:Entity:Lab) RETURN node ORDER BY node.id", r -> {
            final Node firstNode = (Node) r.next().get("node");
            final Map<String, Object> expectedFirstNode =
                    Map.of("date", LocalDate.of(2020, 1, 1), "int", 1L, "id", "1", "str", "hello");
            assertEquals(expectedFirstNode, firstNode.getAllProperties());
            final Node secondNode = (Node) r.next().get("node");
            final Map<String, Object> expectedSecondNode =
                    Map.of("date", LocalDate.of(2020, 1, 1), "int", 2L, "id", "2", "str", "world");
            assertEquals(expectedSecondNode, secondNode.getAllProperties());
            final Node thirdNode = (Node) r.next().get("node");
            assertEquals(Map.of("str", "", "id", "3"), thirdNode.getAllProperties());
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testEmptyInteger() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: 'file:/emptyInteger.csv', labels: ['entity']}], [], {ignoreBlankString: true})",
                r -> assertEquals(3L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (node:Thing) RETURN node ORDER BY node.int_attribute", r -> {
            final Node firstNode = (Node) r.next().get("node");
            final Map<String, Object> firstProps = firstNode.getAllProperties();
            assertEquals(1L, firstProps.get("int_attribute"));
            assertArrayEquals(new long[] {2L, 3L}, (long[]) firstProps.get("int_attribute_array"));
            assertArrayEquals(new double[] {2.3D, 3.5D}, (double[]) firstProps.get("double_attribute_array"), 0);
            final Node secondNode = (Node) r.next().get("node");
            final Map<String, Object> secondProps = secondNode.getAllProperties();
            assertEquals(2L, secondProps.get("int_attribute"));
            assertArrayEquals(new long[] {4L, 5L}, (long[]) secondProps.get("int_attribute_array"));
            assertArrayEquals(new double[] {2.6D, 3.6D}, (double[]) secondProps.get("double_attribute_array"), 0);
            final Node thirdNode = (Node) r.next().get("node");
            final Map<String, Object> thirdProps = thirdNode.getAllProperties();
            assertNull(thirdProps.get("int_attribute"));
            assertNull(thirdProps.get("int_attribute_array"));
            assertNull(thirdProps.get("double_attribute_array"));
            assertNull(thirdProps.get("str_attribute"));
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testEmptyArray() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: 'file:/emptyArray.csv', labels:[]}], [], $conf)",
                map("conf", map("ignoreEmptyCellArray", true)),
                r -> assertEquals(5L, r.get("nodes")));

        TestUtil.testResult(db, "MATCH (node:Arrays) RETURN node ORDER BY node.id", r -> {
            final Map<String, Object> propsOne = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("normal", propsOne.get("description"));
            assertArrayEquals(new String[] {"a", "b", "c", "d", "e"}, (String[]) propsOne.get("arr"));

            final Map<String, Object> propsTwo = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withNull", propsTwo.get("description"));
            assertFalse(propsTwo.containsKey("arr"));

            final Map<String, Object> propsThree = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withEmptyItem", propsThree.get("description"));
            assertArrayEquals(new String[] {"a", "", "c", "", "e"}, (String[]) propsThree.get("arr"));

            final Map<String, Object> propsFour = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withBlankItem", propsFour.get("description"));
            assertArrayEquals(new String[] {"a", " ", "c", " ", "e"}, (String[]) propsFour.get("arr"));

            final Map<String, Object> propsFive = ((Node) r.next().get("node")).getAllProperties();
            assertEquals("withWhiteSpace", propsFive.get("description"));
            assertArrayEquals(new String[] {" "}, (String[]) propsFive.get("arr"));

            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testRelationshipWithoutIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile", "file:/id.csv",
                        "relFile", "file:/rel-on-ids.csv",
                        "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });
        Assert.assertEquals(
                "John Jane",
                TestUtil.singleResultFirstColumn(
                        db,
                        "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
    }

    @Test
    public void testRelationshipWithIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                map(
                        "nodeFile", "file:/id-idspaces.csv",
                        "relFile", "file:/rel-on-ids-idspaces.csv",
                        "config", map("delimiter", '|')),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });
        Assert.assertEquals(
                "John Jane",
                TestUtil.singleResultFirstColumn(
                        db,
                        "MATCH (p1:Person)-[:KNOWS]->(p2:Person) RETURN p1.name + ' ' + p2.name AS pair ORDER BY pair"));
    }

    @Test
    public void testRelationshipWithCustomIdNames() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv(" + "["
                        + "  {fileName: $personFile, labels: ['Person']},"
                        + "  {fileName: $companyFile, labels: ['Company']},"
                        + "  {fileName: $universityFile, labels: ['University']}"
                        + "],"
                        + "["
                        + "  {fileName: $relFile, type: 'AFFILIATED_WITH'}"
                        + "],"
                        + " $config)",
                map(
                        "personFile", "file:/custom-ids-basic-persons.csv",
                        "companyFile", "file:/custom-ids-basic-companies.csv",
                        "universityFile", "file:/custom-ids-basic-unis.csv",
                        "relFile", "file:/custom-ids-basic-affiliated-with.csv",
                        "config", map()),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                });

        List<String> pairs = TestUtil.firstColumn(
                db, "MATCH (p:Person)-[:AFFILIATED_WITH]->(org) RETURN p.name + ' ' + org.name AS pair ORDER BY pair");
        assertTrue(pairs.containsAll(List.of("Jane Neo4j", "John TU Munich")));
    }

    @Test
    public void testRelationshipWithCustomIdNamesAndIdSpaces() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv(" + "["
                        + "  {fileName: $personFile, labels: ['Person']},"
                        + "  {fileName: $companyFile, labels: ['Company']},"
                        + "  {fileName: $universityFile, labels: ['University']}"
                        + "],"
                        + "["
                        + "  {fileName: $relFile, type: 'AFFILIATED_WITH'}"
                        + "],"
                        + " $config)",
                map(
                        "personFile", "file:/custom-ids-idspaces-persons.csv",
                        "companyFile", "file:/custom-ids-idspaces-companies.csv",
                        "universityFile", "file:/custom-ids-idspaces-unis.csv",
                        "relFile", "file:/custom-ids-idspaces-affiliated-with.csv",
                        "config", map()),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                });

        List<String> pairs = TestUtil.firstColumn(
                db, "MATCH (p:Person)-[:AFFILIATED_WITH]->(org) RETURN p.name + ' ' + org.name AS pair ORDER BY pair");
        assertTrue(pairs.containsAll(List.of("Jane Neo4j", "John TU Munich")));
    }

    @Test
    public void ignoreFieldType() {
        final String query =
                "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)";
        final Map<String, Object> config = map(
                "nodeFile",
                "file:/ignore-nodes.csv",
                "relFile",
                "file:/ignore-relationships.csv",
                "config",
                map("delimiter", '|', "batchSize", 1));
        commonAssertionIgnoreFieldType(config, query, true);
    }

    @Test
    public void ignoreFieldTypeWithByteArrayFile() {
        final Map<String, Object> config = map(
                "nodeFile",
                fileToBinary(new File(BASE_URL_FILES, "ignore-nodes.csv"), CompressionAlgo.GZIP.name()),
                "relFile",
                fileToBinary(new File(BASE_URL_FILES, "ignore-relationships.csv"), CompressionAlgo.GZIP.name()),
                "config",
                map("delimiter", '|', "batchSize", 1, COMPRESSION, CompressionAlgo.GZIP.name()));
        final String query =
                "CALL apoc.import.csv([{data: $nodeFile, labels: ['Person']}], [{data: $relFile, type: 'KNOWS'}], $config)";
        commonAssertionIgnoreFieldType(config, query, false);
    }

    @Test
    public void ignoreFieldTypeWithBothBinaryAndFileUrl() throws IOException {
        FileUtils.writeByteArrayToFile(
                new File(BASE_URL_FILES, "ignore-relationships.csv.zz"),
                fileToBinary(new File(BASE_URL_FILES, "ignore-relationships.csv"), CompressionAlgo.DEFLATE.name()));

        final Map<String, Object> config = map(
                "nodeFile",
                fileToBinary(new File(BASE_URL_FILES, "ignore-nodes.csv"), CompressionAlgo.DEFLATE.name()),
                "relFile",
                "file:/ignore-relationships.csv.zz",
                "config",
                map("delimiter", '|', "batchSize", 1, COMPRESSION, CompressionAlgo.DEFLATE.name()));
        final String query =
                "CALL apoc.import.csv([{data: $nodeFile, labels: ['Person']}], [{data: $relFile, type: 'KNOWS'}], $config)";
        commonAssertionIgnoreFieldType(config, query, false);
    }

    private void commonAssertionIgnoreFieldType(Map<String, Object> config, String query, boolean isFile) {
        TestUtil.testCall(db, query, config, (r) -> {
            assertEquals(2L, r.get("nodes"));
            assertEquals(2L, r.get("relationships"));
            assertEquals(isFile ? "progress.csv" : null, r.get("file"));
            assertEquals(isFile ? "file" : "file/binary", r.get("source"));
            assertEquals(8L, r.get("properties"));
        });

        List<Long> ages = TestUtil.firstColumn(db, "MATCH (p:Person) RETURN p.age AS age ORDER BY age");
        assertTrue(ages.containsAll(List.of(25L, 26L)));

        List<String> pairs = TestUtil.firstColumn(
                db,
                """
                MATCH (p1:Person)-[k:KNOWS]->(p2:Person)
                WHERE p1.lastname IS NULL
                  AND p2.lastname IS NULL
                  AND k.prop1 IS NULL
                RETURN p1.firstname + ' ' + p1.age + ' <' + k.prop2 + '> ' + p2.firstname + ' ' + p2.age AS pair ORDER BY pair""");
        assertTrue(pairs.containsAll(List.of("Jane 26 <6> John 25", "John 25 <3> Jane 26")));
    }

    @Test
    public void testNoDuplicateEndpointsCreated() {
        // some of the endpoints of the edges in 'knows.csv' do not exist,
        // hence this should throw an exception
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> db.executeTransactionally(
                        "CALL apoc.import.csv([{fileName: $nodeFile, labels: ['Person']}], [{fileName: $relFile, type: 'KNOWS'}], $config)",
                        map(
                                "nodeFile",
                                "file:/persons.csv",
                                "relFile",
                                "file:/knows.csv",
                                "config",
                                map("stringIds", false))));
        assertTrue(e.getMessage().contains("Node for id space __CSV_DEFAULT_IDSPACE and id 10 not found"));
    }

    @Test
    public void testIgnoreDuplicateNodes() {
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> db.executeTransactionally(
                        "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                        map(
                                "file",
                                "file:/id-with-duplicates.csv",
                                "config",
                                map("delimiter", '|', "stringIds", false, "ignoreDuplicateNodes", false))));
        assertTrue(e.getMessage().contains("Duplicate node with id 1 found on line 2"));
    }

    @Test
    public void testLoadDuplicateNodes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file",
                        "file:/id-with-duplicates.csv",
                        "config",
                        map("delimiter", '|', "stringIds", false, "ignoreDuplicateNodes", true)),
                (r) -> {
                    assertEquals(1L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        Assert.assertEquals(
                "John", TestUtil.singleResultFirstColumn(db, "MATCH (n:Person) RETURN n.name AS name ORDER BY name"));

        long id = TestUtil.<Long>singleResultFirstColumn(db, "MATCH (n:Person) RETURN n.id AS id ORDER BY id");
        Assert.assertEquals(1L, id);
    }

    @Test
    public void testDifferentDataTypes() {
        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Person']}], [], $config)",
                map(
                        "file",
                        "file:/withDifferentTypes.csv",
                        "config",
                        map("delimiter", '|', "ignoreDuplicateNodes", true)),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        TestUtil.testResult(
                db, "MATCH (n:Cat) RETURN n.name AS name, n.age AS age, n.chipID AS chipID ORDER BY name", (res) -> {
                    Map<String, Object> r = res.next();
                    assertEquals("Maja", r.get("name"));
                    assertEquals(0.5, r.get("age"));
                    assertEquals(1236L, r.get("chipID"));
                    r = res.next();
                    assertEquals("Pelle", r.get("name"));
                    assertEquals(0.5, r.get("age"));
                    assertEquals(1345L, r.get("chipID"));
                });

        db.executeTransactionally("MATCH (n:Cat) DETACH DELETE n");
    }

    @Test
    public void testRoundTripWithTypes() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        db.executeTransactionally(
                """
        CREATE (:Cat {
                        name: 'Maja',
                        age: 0.5,
                        chipID: 1236,
                        location: point({latitude: 13.1, longitude: 33.46789}),
                        isFluffy: true,
                        born: date('2024-05-10')
                        })
        CREATE (:Cat {
                        name: 'Pelle',
                        age: 0.5,
                        chipID: 1345,
                        location: point({latitude: 13.1, longitude: 33.46789}),
                        isFluffy: false,
                        born: date('2024-05-10')
                        })
        """);

        // In separate files
        String fileNameStart = "exportedData";
        String fileName = fileNameStart + ".csv";
        TestUtil.testCall(
                db,
                "CALL apoc.export.csv.all($file,{bulkImport: true})",
                map("file", fileName),
                (r) -> assertEquals(fileName, r.get("file")));

        // REMOVE DATA
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        TestUtil.testCall(
                db,
                "CALL apoc.import.csv([{fileName: $file, labels: ['Cat']}], [], $config)",
                map("file", fileNameStart + ".nodes.Cat.csv", "config", map("ignoreDuplicateNodes", true)),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(0L, r.get("relationships"));
                });

        TestUtil.testResult(
                db,
                """
                    MATCH (n:Cat)
                    RETURN n.name AS name,
                            n.age AS age,
                            n.chipID AS chipID,
                            n.isFluffy AS isFluffy,
                            n.friends AS friends,
                            n.location AS location,
                            n.born AS born
                    ORDER BY name
                """,
                (res) -> {
                    Map<String, Object> r = res.next();
                    assertEquals("Maja", r.get("name"));
                    assertEquals(0.5, r.get("age"));
                    assertEquals(1236L, r.get("chipID"));
                    assertEquals(true, r.get("isFluffy"));
                    assertEquals(
                            Values.pointValue(CoordinateReferenceSystem.WGS_84, 33.46789D, 13.1D), r.get("location"));
                    assertEquals(LocalDate.of(2024, 5, 10), r.get("born"));

                    r = res.next();
                    assertEquals("Pelle", r.get("name"));
                    assertEquals(0.5, r.get("age"));
                    assertEquals(1345L, r.get("chipID"));
                    assertEquals(false, r.get("isFluffy"));
                    assertEquals(
                            Values.pointValue(CoordinateReferenceSystem.WGS_84, 33.46789D, 13.1D), r.get("location"));
                    assertEquals(LocalDate.of(2024, 5, 10), r.get("born"));
                });

        db.executeTransactionally("MATCH (n:Cat) DETACH DELETE n");
    }
}
