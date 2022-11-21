package apoc.index;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 23.05.16
 */
public class
SchemaIndexTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static List<String> personNames;
    private static List<String> personAddresses;
    private static List<Long> personAges;
    private static List<Long> personIds;
    private static final int firstPerson = 1;
    private static final int lastPerson = 200;

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, SchemaIndex.class);
        db.executeTransactionally("CREATE (city:City {name:'London'}) WITH city UNWIND range("+firstPerson+","+lastPerson+") as id CREATE (:Person {name:'name'+id, id:id, age:id % 100, address:id+'Main St.'})-[:LIVES_IN]->(city)");
        // dataset for fulltext indexes
        db.executeTransactionally("""
                CREATE (:Label1 {prop1: "Michael", prop2: 111}),
                    (:Label1 {prop1: "AA", prop2: 1}),
                    (:Label1 {prop1: "EE", prop2: 111}),
                    (:Label1 {prop1: "Ryan", prop2: 1}),
                    (:Label1 {prop1: "UU", prop2: 111}),
                    (:Label1 {prop1: "Ryan", prop2: 1}),
                    (:Label1 {prop1: "Ryan", prop3: 'qwerty'}),
                    (:Label2 {prop1: "Ryan", prop3: 'abcde'})""");
        //
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.age)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.address)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE");
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally("CREATE INDEX rel_range_index_name FOR ()-[r:KNOWS]-() ON (r.since)");
        db.executeTransactionally("CREATE (f:Foo {bar:'three'}), (f2a:Foo {bar:'four'}), (f2b:Foo {bar:'four'})");
        personIds = LongStream.range(firstPerson, lastPerson+1).boxed().collect(Collectors.toList());
        personNames = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> "name"+i).sorted().collect(Collectors.toList());
        personAddresses = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> i+"Main St.").sorted().collect(Collectors.toList());
        personAges = IntStream.range(firstPerson, lastPerson+1)
                .map(i -> i % 100)
                .sorted()
                .distinct()
                .mapToObj(Long::new).collect(Collectors.toList());
        try (Transaction tx=db.beginTx()) {
            tx.schema().awaitIndexesOnline(2,TimeUnit.SECONDS);
            tx.commit();
        }
    }

    @Test
    public void testDistinctPropertiesOnFirstIndex() {
        testCall(db,"CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person","key", "name"),
                (row) -> assertEquals(new HashSet<>(personNames), new HashSet<>((Collection<String>) row.get("value")))
        );
    }


    @Test(timeout = 5000L)
    public void testDistinctWithoutIndexWaitingShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextLabel1 FOR (n:Label1) ON EACH [n.prop1]");
        // executing the apoc.schema.properties.distinct without CALL db.awaitIndexes() will throw an "Index is still populating" exception
        
        // todo - assertions
        db.executeTransactionally("CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Label1","key", "prop1"),
                r -> r.resultAsString(),
                Duration.ofSeconds(10));

        db.executeTransactionally("DROP INDEX fulltextLabel1");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithFullTextIndexShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextLabel1 FOR (n:Label1) ON EACH [n.prop1]");

        db.executeTransactionally("CALL db.awaitIndexes()");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Label1", "key", "prop1"), 
                row -> assertEquals(Set.of("AA", "EE", "UU", "Ryan", "Michael"), Set.copyOf((List)row.get("value"))) 
        );

        testResult(db, "CALL apoc.schema.properties.distinctCount($label, $key)",
                map("label", "Label1", "key", "prop1"),
                res -> {
                    assertDistinctCountProperties("Label1", "prop1", List.of("AA", "EE", "UU"), () -> 1L, res);
                    assertDistinctCountProperties("Label1", "prop1", List.of("Ryan"), () -> 3L, res);
                    assertDistinctCountProperties("Label1", "prop1", List.of("Michael"), () -> 1L, res);
                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("DROP INDEX fulltextLabel1");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithMultiLabelFullTextIndexShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextLabel1 FOR (n:Label1|Label2) ON EACH [n.prop1,n.prop3]");
        // insert a non-fulltext index, with the same label and prop
        db.executeTransactionally("CREATE RANGE INDEX FOR (n:One) ON (n.prop1)");
        
        db.executeTransactionally("CALL db.awaitIndexes");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Label1", "key", "prop1"),
                row -> assertEquals(Set.of("AA", "EE", "UU", "Ryan", "Michael", "abcde", "qwerty"), Set.copyOf((List)row.get("value")))
        );

        testResult(db, "CALL apoc.schema.properties.distinctCount($label, $key) " +
                        "YIELD label, key, value, count RETURN * ORDER BY label",
                map("label", "Label1", "key", "prop1"),
                res -> {
                    // todo - more assertions
                    res.close();
                });
        
        db.executeTransactionally("DROP INDEX fulltextLabel1");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithNoPreviousNodesShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE INDEX FOR (n:LabelNotExistent) ON n.prop");
        
        testCall(db, """
                        CREATE (:LabelNotExistent {prop:2})
                        WITH *
                        CALL apoc.schema.properties.distinct("LabelNotExistent", "prop")
                        YIELD value RETURN *""", 
                r -> assertEquals(Collections.emptyList(), r.get("value"))
        );
    }

    @Test
    public void testPropertiesDistinctDoesntReturnRelIndexes() {
        testCall(db, "CALL apoc.schema.properties.distinct(\"\", $key)",
                map("key", "since"), // since is a relationship prop index
                (row) -> assertEquals(new HashSet<>(), new HashSet<>((Collection<String>) row.get("value")))
        );
    }

    @Test
    public void testDistinctPropertiesOnSecondIndex() {
        testCall(db,"CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person","key", "address"),
                (row) -> assertEquals(new HashSet<>(personAddresses), new HashSet<>((Collection<String>) row.get("value")))
        );
    }

    @Test
    public void testDistinctCountPropertiesOnFirstIndex() {
        String label = "Person";
        String key = "name";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnSecondIndex() {
        String label = "Person";
        String key = "address";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabel() {
        String key = "name";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label","","key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyKey() {
        String label = "Person";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY key,value",
                map("label",label,"key",""),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertDistinctCountProperties("Person", "age", personAges, () -> 2L, result);
                    assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabelAndEmptyKey() {
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY label,key,value",
                map("label","","key",""),
                (result) -> {
                    assertTrue(result.hasNext());
                    assertEquals(map("label","Foo","key","bar","value","four","count",2L),result.next());
                    assertEquals(map("label","Foo","key","bar","value","three","count",1L),result.next());
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertDistinctCountProperties("Person", "age", personAges, () -> 2L, result);
                    assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    private <T> void assertDistinctCountProperties(String label, String key, Collection<T> values, Supplier<Long> counts, Result result) {
        Iterator<T> valueIterator = values.iterator();

        while (valueIterator.hasNext()) {
            assertTrue(result.hasNext());
            Map<String,Object> map = result.next();
            assertEquals(label, map.get("label"));
            assertEquals(key, map.get("key"));
            assertEquals(valueIterator.next(), map.get("value"));
            assertEquals(counts.get(), map.get("count"));
        }
    }
}
