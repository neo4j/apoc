package apoc.convert;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 29.05.16
 */
public class ConvertTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void initDb() {
        TestUtil.registerProcedure(db, Convert.class);
    }

    @AfterClass
    public static void teardown() {
       db.shutdown();
    }

    @Test
    public void testToMap() {
        testCall(db, "return apoc.convert.toMap($a) as value", map("a", map("a", "b")), r -> assertEquals(map("a", "b"), r.get("value")));
        testCall(db, "return apoc.convert.toMap($a) as value", map("a", null), r -> assertEquals(null, r.get("value")));
        final Map<String, Object> props = map("name", "John", "age", 39);
        testCall(db, "create (n) set n=$props return apoc.convert.toMap(n) as value", map("props", props), r -> assertEquals(props, r.get("value")));
    }

    @Test
    public void testToList() {
        testCall(db, "return apoc.convert.toList($a) as value", map("a", null), r -> assertEquals(null, r.get("value")));
        testCall(db, "return apoc.convert.toList($a) as value", map("a", new Object[]{"a"}), r -> assertEquals(singletonList("a"), r.get("value")));
        testCall(db, "return apoc.convert.toList($a) as value", map("a", singleton("a")), r -> assertEquals(singletonList("a"), r.get("value")));
        testCall(db, "return apoc.convert.toList($a) as value", map("a", singletonList("a")), r -> assertEquals(singletonList("a"), r.get("value")));
        testCall(db, "return apoc.convert.toList($a) as value", map("a", singletonList("a").iterator()), r -> assertEquals(singletonList("a"), r.get("value")));
    }

    @Test
    public void testToNode() {
        testCall(db, "CREATE (n) WITH [n] as x RETURN apoc.convert.toNode(x[0]) as node",
                r -> assertEquals(true, r.get("node") instanceof Node));
        testCall(db, "RETURN apoc.convert.toNode(null) AS node", r -> assertEquals(null, r.get("node")));
    }

    @Test
    public void testToRelationship() {
        testCall(db, "CREATE (n)-[r:KNOWS]->(m) WITH [r] as x RETURN apoc.convert.toRelationship(x[0]) AS rel",
                r -> assertEquals(true, r.get("rel") instanceof Relationship));
        testCall(db, "RETURN apoc.convert.toRelationship(null) AS rel", r -> assertEquals(null, r.get("rel")));
    }

    @Test
    public void testToSet() {
        testCall(db, "return apoc.convert.toSet($a) as value", map("a", null), r -> assertEquals(null, r.get("value")));
        testCall(db, "return apoc.convert.toSet($a) as value", map("a", new Object[]{"a"}), r -> assertEquals(singletonList("a"), r.get("value")));
        testCall(db, "return apoc.convert.toSet($a) as value", map("a", singleton("a")), r -> assertEquals(singletonList("a"), r.get("value")));
        testCall(db, "return apoc.convert.toSet($a) as value", map("a", singletonList("a")), r -> assertEquals(singletonList("a"), r.get("value")));
        testCall(db, "return apoc.convert.toSet($a) as value", map("a", singletonList("a").iterator()), r -> assertEquals(singletonList("a"), r.get("value")));
    }

    @Test
    public void testToNodeList() {
        testCall(db, "CREATE (n) WITH [n] as x RETURN apoc.convert.toNodeList(x) as nodes",
                r -> {
					assertEquals(true, r.get("nodes") instanceof List);
					List<Node> nodes = (List<Node>) r.get("nodes");
					assertEquals(true, nodes.get(0) instanceof Node);
                });
    }

    @Test
    public void testToRelationshipList() {
        testCall(db, "CREATE (n)-[r:KNOWS]->(m) WITH [r] as x  RETURN apoc.convert.toRelationshipList(x) as rels",
                r -> {
					assertEquals(true, r.get("rels") instanceof List);
					List<Relationship> rels = (List<Relationship>) r.get("rels");
					assertEquals(true, rels.get(0) instanceof Relationship);
                });
    }
}
