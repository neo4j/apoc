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
package apoc.index;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.util.TestUtil;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

@ImpermanentEnterpriseDbmsExtension
class SchemaIndexTest {

    private static final String SCHEMA_DISTINCT_COUNT_ORDERED =
            """
            CALL apoc.schema.properties.distinctCount($label, $key)
            YIELD label, key, value, count
            RETURN * ORDER BY label, key, value""";
    private static final String FULL_TEXT_LABEL = "FullTextOne";
    private static final String SCHEMA_LABEL = "SchemaTest";

    @Inject
    GraphDatabaseService db;

    private static List<String> personNames;
    private static List<String> personAddresses;
    private static List<Long> personAges;
    private static List<Long> personIds;
    private static final int firstPerson = 1;
    private static final int lastPerson = 200;

    @BeforeAll
    void setup() {
       TestUtil.registerProcedure(db, SchemaIndex.class);
    }

    @BeforeEach
    void testSetUp() {
       db.executeTransactionally(
                "CREATE (city:City {name:'London'}) WITH city UNWIND range(" + firstPerson + "," + lastPerson
                        + ") as id CREATE (:Person {name:'name'+id, id:id, age:id % 100, address:id+'Main St.'})-[:LIVES_IN]->(city)");

        db.executeTransactionally(
                """
                CYPHER 25
                CREATE (:FullTextOne {prop1: "Michael", prop2: 111}),
                    (:FullTextOne {prop1: "AA", prop2: 1}),
                    (:FullTextOne {prop1: "EE", prop2: 111}),
                    (:FullTextOne {prop1: "Ryan", prop2: 1}),
                    (:FullTextOne {prop1: "UU", prop2: "Ryan"}),
                    (:FullTextOne {prop1: "Ryan", prop2: 1}),
                    (:FullTextOne {prop1: "Ryan", prop3: 'qwerty'}),
                    (:FullTextTwo {prop1: "Ryan"}),
                    (:FullTextTwo {prop1: "omega"}),
                    (:FullTextTwo {prop1: "Ryan", prop3: 'abcde'}),
                    (:SchemaTest {prop1: 'a', prop2: 'bar'}),
                    (:SchemaTest {prop1: 'b', prop2: 'foo'}),
                    (:SchemaTest {prop1: 'c', prop2: 'bar'}),
                    (:Label {prop: vector([1, 3, 14], 3, INT)}),
                    (:Label {prop: vector([1, 3, 14], 3, INT)}),
                    (:Label {prop: vector([1, 3, 15], 3, INT)}),
                    (:Label {prop: [1, 3, 14]}),
                    (:Label {prop: [1, 3, 14]}),
                    (:Label {prop: [1, 3, 14]}),
                    (:Label {prop: [1, 3, 15]}),
                    (:Label {prop: [1.0, 2.3, 3.14]}),
                    (:Label {prop: [1.0, 2.3, 3.14]}),
                    (:Label {prop: [1.0, 2.3, 3.15]}),
                    (:Label {prop: ["1", "3", "14"]}),
                    (:Label {prop: ["1", "3", "14"]}),
                    (:Label {prop: ["1", "3", "15"]}),
                    (:Label {prop: [true, false, true]}),
                    (:Label {prop: [true, false, true]}),
                    (:Label {prop: point({ x: 2.3, y: 4.5, crs: 'cartesian'})})
                """);

        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.age)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.address)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE");
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally("CREATE INDEX rel_range_index_name FOR ()-[r:KNOWS]-() ON (r.since)");
        db.executeTransactionally("CREATE (f:Foo {bar:'three'}), (f2a:Foo {bar:'four'}), (f2b:Foo {bar:'four'})");
        personIds = LongStream.range(firstPerson, lastPerson + 1).boxed().collect(Collectors.toList());
        personNames = IntStream.range(firstPerson, lastPerson + 1)
                .mapToObj(Integer::toString)
                .map(i -> "name" + i)
                .sorted()
                .collect(Collectors.toList());
        personAddresses = IntStream.range(firstPerson, lastPerson + 1)
                .mapToObj(Integer::toString)
                .map(i -> i + "Main St.")
                .sorted()
                .collect(Collectors.toList());
        personAges = IntStream.range(firstPerson, lastPerson + 1)
                .map(i -> i % 100)
                .sorted()
                .distinct()
                .mapToObj(Long::valueOf)
                .collect(Collectors.toList());
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.SECONDS);
            tx.commit();
        }
    }

    @Test
    public void testDistinctPropertiesOnFirstIndex() {
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person", "key", "name"),
                (row) ->
                        assertEquals(new HashSet<>(personNames), new HashSet<>((Collection<String>) row.get("value"))));
    }

    @Test
    @Timeout(5)
    public void testDistinctWithoutIndexWaitingShouldNotHangs() {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne FOR (n:FullTextOne) ON EACH [n.prop1]");
        // executing the apoc.schema.properties.distinct without CALL db.awaitIndexes() will throw an "Index is still
        // populating" exception

        db.executeTransactionally(
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                Result::resultAsString,
                Duration.ofSeconds(10));

        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
    }

    @Test
    @Timeout(5)
    public void testFulltextAndRangeIndexOnSameSchema() {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne FOR (n:FullTextOne) ON EACH [n.prop1]");
        db.executeTransactionally("CREATE RANGE INDEX range FOR (n:FullTextOne) ON n.prop1");
        db.executeTransactionally("CALL db.awaitIndexes()");
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                row -> assertEqualsInAnyOrder(Set.of("AA", "EE", "UU", "Ryan", "Michael"), (List) row.get("value")));

        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
        db.executeTransactionally("DROP INDEX range");
    }

    @Test
    @Timeout(5)
    public void testRangeAndFulltextIndexOnSameSchema() {
        db.executeTransactionally("CREATE RANGE INDEX range FOR (n:FullTextOne) ON n.prop1");
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne FOR (n:FullTextOne) ON EACH [n.prop1]");
        db.executeTransactionally("CALL db.awaitIndexes()");
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                row -> assertEqualsInAnyOrder(Set.of("AA", "EE", "UU", "Ryan", "Michael"), (List) row.get("value")));

        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
        db.executeTransactionally("DROP INDEX range");
    }

    @Test
    @Timeout(5)
    public void testRangeIndexForListAndVectors() {
        db.executeTransactionally("CREATE INDEX range FOR (n:Label) ON n.prop");
        db.executeTransactionally("CALL db.awaitIndexes()");
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Label", "key", "prop"),
                row -> assertEqualsInAnyOrder(
                        Set.of(
                                List.of("1", "3", "14"),
                                List.of("1", "3", "15"),
                                List.of(1L, 3L, 14L),
                                List.of(1L, 3L, 15L),
                                List.of(1.0, 2.3, 3.14),
                                List.of(1.0, 2.3, 3.15),
                                Values.int64Vector(1, 3, 14),
                                Values.int64Vector(1, 3, 15),
                                List.of(true, false, true),
                                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.3, 4.5)),
                        (List) row.get("value")));

        db.executeTransactionally("DROP INDEX range");
    }

    @Test
    @Timeout(5)
    public void testNoIndexForListAndVectors() {
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Label", "key", "prop"),
                row -> assertEqualsInAnyOrder(
                        Set.of(
                                List.of("1", "3", "14"),
                                List.of("1", "3", "15"),
                                List.of(1L, 3L, 14L),
                                List.of(1L, 3L, 15L),
                                List.of(1.0, 2.3, 3.14),
                                List.of(1.0, 2.3, 3.15),
                                Values.int64Vector(1, 3, 14),
                                Values.int64Vector(1, 3, 15),
                                List.of(true, false, true),
                                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.3, 4.5)),
                        (List) row.get("value")));
    }

    @Test
    @Timeout(5)
    public void shouldIgnoreOtherIndexTypes() {
        // Only range indexes can be used to scan for all properties, otherwise some property types will not be covered
        db.executeTransactionally("CREATE POINT INDEX point FOR (n:Label) ON n.prop");
        db.executeTransactionally("CREATE TEXT INDEX text FOR (n:Label) ON n.prop");
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltext FOR (n:Label) ON EACH [n.prop]");
        db.executeTransactionally("CREATE VECTOR INDEX vector FOR (n:Label) ON n.prop");
        db.executeTransactionally("CALL db.awaitIndexes()");
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Label", "key", "prop"),
                row -> assertEqualsInAnyOrder(
                        Set.of(
                                List.of("1", "3", "14"),
                                List.of("1", "3", "15"),
                                List.of(1L, 3L, 14L),
                                List.of(1L, 3L, 15L),
                                List.of(1.0, 2.3, 3.14),
                                List.of(1.0, 2.3, 3.15),
                                Values.int64Vector(1, 3, 14),
                                Values.int64Vector(1, 3, 15),
                                List.of(true, false, true),
                                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.3, 4.5)),
                        (List) row.get("value")));

        db.executeTransactionally("DROP INDEX point");
        db.executeTransactionally("DROP INDEX text");
        db.executeTransactionally("DROP INDEX fulltext");
        db.executeTransactionally("DROP INDEX vector");
    }

    @Test
    @Timeout(5)
    public void testDistinctWithVoidIndexShouldNotHangs() {
        db.executeTransactionally("create index VoidIndex for (n:VoidIndex) on (n.myProp)");

        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "VoidIndex", "key", "myProp"),
                row -> assertEquals(emptyList(), row.get("value")));

        db.executeTransactionally("drop index VoidIndex");
    }

    @Test
    @Timeout(5)
    public void testDistinctWithCompositeIndexShouldNotHangs() {
        db.executeTransactionally("create index EmptyLabel for (n:EmptyLabel) on (n.one)");
        db.executeTransactionally("create index EmptyCompositeLabel for (n:EmptyCompositeLabel) on (n.two, n.three)");

        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "EmptyLabel", "key", "one"),
                row -> assertEquals(emptyList(), row.get("value")));

        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "EmptyCompositeLabel", "key", "two"),
                row -> assertEquals(emptyList(), row.get("value")));

        db.executeTransactionally("drop index EmptyLabel");
        db.executeTransactionally("drop index EmptyCompositeLabel");
    }

    @Test
    @Timeout(5)
    public void testDistinctWithCompositeIndexWithMixedRepeatedProps() {
        db.executeTransactionally("create index SchemaTest for (n:SchemaTest) on (n.prop1, n.prop2)");
        db.executeTransactionally("CALL db.awaitIndexes()");

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", SCHEMA_LABEL, "key", "prop2"), res -> {
            assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("bar"), 2L, res);
            assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("foo"), 1L, res);
            assertFalse(res.hasNext());
        });

        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", SCHEMA_LABEL, "key", "prop2"),
                row -> assertEqualsInAnyOrder(Set.of("bar", "foo"), (List) row.get("value")));

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "", "key", ""), (result) -> {
            extractEverything(result);
            assertFalse(result.hasNext());
        });

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", SCHEMA_LABEL, "key", ""), (result) -> {
            extractedSchemaTest(result);
            assertFalse(result.hasNext());
        });

        db.executeTransactionally("drop index SchemaTest");
    }

    @Test
    @Timeout(5)
    public void testDistinctCountWithRangeIndexForListAndVectors() {
        db.executeTransactionally("CREATE INDEX range FOR (n:Label) ON n.prop");
        db.executeTransactionally("CALL db.awaitIndexes()");
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "Label", "key", "prop"), res -> {
            extractedLabel(res);
            assertFalse(res.hasNext());
        });
        db.executeTransactionally("DROP INDEX range");
    }

    @Test
    @Timeout(5)
    public void testDistinctCountWithoutIndexForListAndVectors() {
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "Label", "key", "prop"), res -> {
            extractedLabel(res);
            assertFalse(res.hasNext());
        });
    }

    @Test
    @Timeout(5)
    public void distinctCountShouldIgnoreOtherIndexTypes() {
        // Only range indexes can be used to scan for all properties, otherwise some property types will not be covered
        db.executeTransactionally("CREATE POINT INDEX point FOR (n:Label) ON n.prop");
        db.executeTransactionally("CREATE TEXT INDEX text FOR (n:Label) ON n.prop");
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltext FOR (n:Label) ON EACH [n.prop]");
        db.executeTransactionally("CREATE VECTOR INDEX vector FOR (n:Label) ON n.prop");
        db.executeTransactionally("CALL db.awaitIndexes()");

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "Label", "key", "prop"), res -> {
            extractedLabel(res);
            assertFalse(res.hasNext());
        });

        db.executeTransactionally("DROP INDEX point");
        db.executeTransactionally("DROP INDEX text");
        db.executeTransactionally("DROP INDEX fulltext");
        db.executeTransactionally("DROP INDEX vector");
    }

    @Test
    @Timeout(5)
    public void testDistinctWithFullTextIndexShouldNotHangs() {
        db.executeTransactionally("CREATE FULLTEXT INDEX FullTextOneProp1 FOR (n:FullTextOne) ON EACH [n.prop1]");
        db.executeTransactionally("CALL db.awaitIndexes");

        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                row -> assertEqualsInAnyOrder(Set.of("AA", "EE", "UU", "Ryan", "Michael"), (List) row.get("value")));

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", FULL_TEXT_LABEL, "key", "prop1"), res -> {
            extractedFullTextFullTextOneProp1(res);
            assertFalse(res.hasNext());
        });

        db.executeTransactionally("DROP INDEX FullTextOneProp1");
    }

    @Test
    @Timeout(5)
    public void testWithDifferentIndexesAndSameLabelProp() {
        db.executeTransactionally("CREATE FULLTEXT INDEX FullTextOneProp1 FOR (n:FullTextOne) ON EACH [n.prop1]");
        db.executeTransactionally("CREATE RANGE INDEX RangeProp1 FOR (n:FullTextOne) ON (n.prop1)");
        db.executeTransactionally("CALL db.awaitIndexes");

        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", FULL_TEXT_LABEL, "key", "prop1"),
                row -> assertEqualsInAnyOrder(Set.of("AA", "EE", "UU", "Ryan", "Michael"), (List) row.get("value")));

        // in this case the procedure returns distinct rows though we have 2 different analogues indexes
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", FULL_TEXT_LABEL, "key", "prop1"), res -> {
            extractedFullTextFullTextOneProp1(res);
            assertFalse(res.hasNext());
        });

        db.executeTransactionally("DROP INDEX FullTextOneProp1");
        db.executeTransactionally("DROP INDEX RangeProp1");
    }

    @Test
    @Timeout(5)
    public void testDistinctWithMultiLabelFullTextIndexShouldNotHangs() {
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX fulltextComposite FOR (n:FullTextOne|FullTextTwo) ON EACH [n.prop1,n.prop3]");
        db.executeTransactionally("CALL db.awaitIndexes");

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", FULL_TEXT_LABEL, "key", "prop1"), res -> {
            extractedFullTextFullTextOneProp1(res);
            assertFalse(res.hasNext());
        });

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "", "key", ""), (result) -> {
            extractEverything(result);
            assertFalse(result.hasNext());
        });

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", FULL_TEXT_LABEL, "key", ""), (result) -> {
            extractedFullTextFullTextOneProp1(result);
            extractedFullTextFullTextOneProp2(result);
            extractedFullTextFullTextOneProp3(result);
            assertFalse(result.hasNext());
        });

        db.executeTransactionally("DROP INDEX fulltextComposite");
    }

    @Test
    @Timeout(5)
    public void testDistinctWithNoPreviousNodesShouldNotHangs() {
        db.executeTransactionally("CREATE INDEX LabelNotExistent FOR (n:LabelNotExistent) ON n.prop");

        testCall(
                db,
                """
                        CREATE (:LabelNotExistent {prop:2})
                        WITH *
                        CALL apoc.schema.properties.distinct("LabelNotExistent", "prop")
                        YIELD value RETURN *""",
                r -> assertEquals(emptyList(), r.get("value")));

        db.executeTransactionally("DROP INDEX LabelNotExistent");
    }

    @Test
    public void testPropertiesDistinctDoesntReturnRelIndexes() {
        testCall(
                db,
                "CALL apoc.schema.properties.distinct(\"\", $key)",
                map("key", "since"), // since is a relationship prop index
                (row) -> assertEquals(new HashSet<>(), new HashSet<>((Collection<String>) row.get("value"))));
    }

    @Test
    public void testDistinctPropertiesOnSecondIndex() {
        testCall(
                db,
                "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person", "key", "address"),
                (row) -> assertEquals(
                        new HashSet<>(personAddresses), new HashSet<>((Collection<String>) row.get("value"))));
    }

    @Test
    public void testDistinctCountPropertiesOnFirstIndex() {
        String label = "Person";
        String key = "name";
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", label, "key", key), (result) -> {
            assertDistinctCountProperties("Person", "name", personNames, 1L, result);
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnSecondIndex() {
        String label = "Person";
        String key = "address";
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", label, "key", key), (result) -> {
            assertDistinctCountProperties("Person", "address", personAddresses, 1L, result);
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabel() {
        String key = "name";
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "", "key", key), (result) -> {
            assertDistinctCountProperties("City", "name", List.of("London"), 1L, result);
            assertDistinctCountProperties("Person", "name", personNames, 1L, result);
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyKey() {
        String label = "Person";
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", label, "key", ""), (result) -> {
            extractedPerson(result);
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabelAndEmptyKey() {
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED, map("label", "", "key", ""), (result) -> {
            assertTrue(result.hasNext());
            extractEverything(result);
            assertFalse(result.hasNext());
        });
    }

    private <T> void assertDistinctCountProperties(
            String label, String key, Collection<T> values, Long counts, Result result) {

        values.forEach(value -> {
            assertTrue(result.hasNext());
            Map<String, Object> map = result.next();
            assertEquals(label, map.get("label"));
            assertEquals(key, map.get("key"));

            // Convert arrays to lists to be able to do equality checks
            switch (map.get("value")) {
                case long[] array -> assertEquals(
                        value, Arrays.stream(array).boxed().toList());
                case double[] array -> assertEquals(
                        value, Arrays.stream(array).boxed().toList());
                case boolean[] array -> assertEquals(
                        value,
                        IntStream.range(0, array.length).mapToObj(i -> array[i]).toList());
                case Object[] array -> assertEquals(value, Arrays.stream(array).toList());
                default -> assertEquals(value, map.get("value"));
            }

            assertEquals(counts, map.get("count"));
        });
    }

    private void extractedFullTextFullTextOneProp1(Result res) {
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop1", List.of("AA", "EE", "Michael"), 1L, res);
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop1", List.of("Ryan"), 3L, res);
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop1", List.of("UU"), 1L, res);
    }

    private void extractedFullTextFullTextOneProp2(Result res) {
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop2", List.of("Ryan"), 1L, res);
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop2", List.of(1L), 3L, res);
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop2", List.of(111L), 2L, res);
    }

    private void extractedFullTextFullTextOneProp3(Result res) {
        assertDistinctCountProperties(FULL_TEXT_LABEL, "prop3", List.of("qwerty"), 1L, res);
    }

    private void extractedSchemaTest(Result result) {
        assertDistinctCountProperties(SCHEMA_LABEL, "prop1", List.of("a", "b", "c"), 1L, result);
        assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("bar"), 2L, result);
        assertDistinctCountProperties(SCHEMA_LABEL, "prop2", List.of("foo"), 1L, result);
    }

    private void extractedFoo(Result result) {
        assertDistinctCountProperties("Foo", "bar", List.of("four"), 2L, result);
        assertDistinctCountProperties("Foo", "bar", List.of("three"), 1L, result);
    }

    private void extractedPerson(Result result) {
        assertDistinctCountProperties("Person", "address", personAddresses, 1L, result);
        assertDistinctCountProperties("Person", "age", personAges, 2L, result);
        assertDistinctCountProperties("Person", "id", personIds, 1L, result);
        assertDistinctCountProperties("Person", "name", personNames, 1L, result);
    }

    private void extractedLabel(Result result) {
        assertDistinctCountProperties("Label", "prop", List.of(List.of("1", "3", "14")), 2L, result);
        assertDistinctCountProperties("Label", "prop", List.of(List.of("1", "3", "15")), 1L, result);
        assertDistinctCountProperties("Label", "prop", List.of(List.of(true, false, true)), 2L, result);
        assertDistinctCountProperties("Label", "prop", List.of(List.of(1.0, 2.3, 3.14)), 2L, result);
        assertDistinctCountProperties("Label", "prop", List.of(List.of(1.0, 2.3, 3.15)), 1L, result);
        assertDistinctCountProperties("Label", "prop", List.of(List.of(1L, 3L, 14L)), 3L, result);
        assertDistinctCountProperties("Label", "prop", List.of(List.of(1L, 3L, 15L)), 1L, result);
        assertDistinctCountProperties("Label", "prop", List.of(Values.int64Vector(1, 3, 14)), 2L, result);
        assertDistinctCountProperties("Label", "prop", List.of(Values.int64Vector(1, 3, 15)), 1L, result);
        assertDistinctCountProperties(
                "Label", "prop", List.of(Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 2.3, 4.5)), 1L, result);
    }

    private void extractEverything(Result result) {
        assertDistinctCountProperties("City", "name", List.of("London"), 1L, result);
        extractedFoo(result);
        extractedFullTextFullTextOneProp1(result);
        extractedFullTextFullTextOneProp2(result);
        extractedFullTextFullTextOneProp3(result);
        assertDistinctCountProperties("FullTextTwo", "prop1", List.of("Ryan"), 2L, result);
        assertDistinctCountProperties("FullTextTwo", "prop1", List.of("omega"), 1L, result);
        assertDistinctCountProperties("FullTextTwo", "prop3", List.of("abcde"), 1L, result);
        extractedLabel(result);
        extractedPerson(result);
        extractedSchemaTest(result);
    }

    private void assertEqualsInAnyOrder(Set<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size());
        Set<Object> actualSet = new HashSet<>();

        // Convert arrays to lists to be able to do equality checks
        for (Object o : actual) {
            switch (o) {
                case long[] array -> actualSet.add(Arrays.stream(array).boxed().toList());
                case double[] array -> actualSet.add(
                        Arrays.stream(array).boxed().toList());
                case boolean[] array -> actualSet.add(
                        IntStream.range(0, array.length).mapToObj(i -> array[i]).toList());
                case Object[] array -> actualSet.add(Arrays.stream(array).toList());
                default -> actualSet.add(o);
            }
        }
        assertEquals(expected, actualSet);
    }
}
