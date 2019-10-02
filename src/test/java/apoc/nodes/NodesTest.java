package apoc.nodes;

import apoc.create.Create;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

/**
 * @author mh
 * @since 18.08.16
 */
public class NodesTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("apoc.*"));

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Nodes.class, Create.class);
    }

    @Test
    public void isDense() throws Exception {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) WITH f UNWIND range(1,100) as id CREATE (f)-[:SELF]->(f)");

        TestUtil.testCall(db, "MATCH (n) WITH n, apoc.nodes.isDense(n) as dense " +
                        "WHERE n:Foo AND dense OR n:Bar AND NOT dense RETURN count(*) as c",
                (row) -> assertEquals(2L, row.get("c")));
    }
    @Test
    public void link() throws Exception {
        db.executeTransactionally("UNWIND range(1,10) as id CREATE (n:Foo {id:id}) WITH collect(n) as nodes call apoc.nodes.link(nodes,'BAR') RETURN size(nodes) as len");

        long len = TestUtil.singleResultFirstColumn( db,"MATCH (n:Foo {id:1})-[r:BAR*9]->() RETURN size(r) as len");
        assertEquals(9L, len);
    }
    @Test
    public void delete() throws Exception {
        db.executeTransactionally("UNWIND range(1,100) as id CREATE (n:Foo {id:id})-[:X]->(n)");

        long count = TestUtil.singleResultFirstColumn(db, "MATCH (n:Foo) WITH collect(n) as nodes call apoc.nodes.delete(nodes,1) YIELD value as count RETURN count");
        assertEquals(100L, count);

        count = TestUtil.singleResultFirstColumn(db, "MATCH (n:Foo) RETURN count(*) as count");
        assertEquals(0L, count);
    }

    @Test
    public void types() throws Exception {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(f) CREATE (f)-[:X]->(f)");
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n) AS value", (r) -> assertEquals(asSet("X","Y"), asSet(((List)r.get("value")).toArray())));
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X') AS value", (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(db, "MATCH (n:Foo) RETURN apoc.node.relationship.types(n,'X|Z') AS value", (r) -> assertEquals(asList("X"), r.get("value")));
        TestUtil.testCall(db, "MATCH (n:Bar) RETURN apoc.node.relationship.types(n) AS value", (r) -> assertEquals(Collections.emptyList(), r.get("value")));
        TestUtil.testCall(db, "RETURN apoc.node.relationship.types(null) AS value", (r) -> assertEquals(null, r.get("value")));
    }

    @Test
    public void hasRelationhip() throws Exception {
        db.executeTransactionally("CREATE (:Foo)-[:Y]->(:Bar),(n:FooBar) WITH n UNWIND range(1,100) as _ CREATE (n)-[:X]->(n)");
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'Y') AS value",(r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'Y>') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'<Y') AS value", (r)-> assertEquals(false,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationship.exists(n,'X') AS value", (r)-> assertEquals(false,r.get("value")));

        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'Y') AS value",(r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'Y>') AS value", (r)-> assertEquals(false,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'<Y') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationship.exists(n,'X') AS value", (r)-> assertEquals(false,r.get("value")));

        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'X') AS value",(r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'X>') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'<X') AS value", (r)-> assertEquals(true,r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationship.exists(n,'Y') AS value", (r)-> assertEquals(false,r.get("value")));
    }
    @Test

    public void hasRelationhips() throws Exception {
        db.executeTransactionally("CREATE (:Foo)-[:Y]->(:Bar),(n:FooBar) WITH n UNWIND range(1,100) as _ CREATE (n)-[:X]->(n)");
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'Y') AS value",(r)-> assertEquals(map("Y",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'Y>') AS value", (r)-> assertEquals(map("Y>",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'<Y') AS value", (r)-> assertEquals(map("<Y",false),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Foo) RETURN apoc.node.relationships.exist(n,'X') AS value", (r)-> assertEquals(map("X",false),r.get("value")));

        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'Y') AS value",(r)-> assertEquals(map("Y",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'Y>') AS value", (r)-> assertEquals(map("Y>",false),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'<Y') AS value", (r)-> assertEquals(map("<Y",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:Bar) RETURN apoc.node.relationships.exist(n,'X') AS value", (r)-> assertEquals(map("X",false),r.get("value")));

        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'X') AS value",(r)-> assertEquals(map("X",true    ),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'X>') AS value", (r)-> assertEquals(map("X>",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'<X') AS value", (r)-> assertEquals(map("<X",true),r.get("value")));
        TestUtil.testCall(db,"MATCH (n:FooBar) RETURN apoc.node.relationships.exist(n,'Y') AS value", (r)-> assertEquals(map("Y",false),r.get("value")));
    }

    @Test
    public void testConnected() throws Exception {
        db.executeTransactionally("CREATE (st:StartThin),(et:EndThin),(ed:EndDense)");
        int relCount = 20;
        for (int rel=0;rel<relCount;rel++) {
            db.executeTransactionally("MATCH (st:StartThin),(et:EndThin),(ed:EndDense) " +
                            " CREATE (st)-[:REL"+rel+"]->(et) " +
                            " WITH * UNWIND RANGE(1,{count}) AS id CREATE (st)-[:REL"+rel+"]->(ed)",
                    map("count",relCount-rel));
        }

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL10>') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(s,e,'REL15>|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL10>') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(s,e,'REL15>|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL10<') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndThin)  RETURN apoc.nodes.connected(e,s,'REL15<|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s) as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL3') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL10<') as value", (r) -> assertEquals(true, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL20') as value", (r) -> assertEquals(false, r.get("value")));
        TestUtil.testCall(db, "MATCH (s:StartThin),(e:EndDense) RETURN apoc.nodes.connected(e,s,'REL15<|REL20') as value", (r) -> assertEquals(true, r.get("value")));

        // todo inverse e,s then also incoming
    }

    @Test
    public void testDegreeTypeAndDirection() {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<X') as in, apoc.node.degree(f, 'Y>') as out", (r) -> {
            assertEquals(1l, r.get("in"));
            assertEquals(2l, r.get("out"));
        });

    }

    @Test
    public void testDegreeMultiple() {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<X|Y') as all", (r) -> {
            assertEquals(3l, r.get("all"));
        });

    }

    @Test
    public void testDegreeTypeOnly() {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, 'X') as in, apoc.node.degree(f, 'Y') as out", (r) -> {
            assertEquals(2l, r.get("in"));
            assertEquals(2l, r.get("out"));
        });

    }

    @Test
    public void testDegreeDirectionOnly() {
        db.executeTransactionally("CREATE (f:Foo) CREATE (b:Bar) CREATE (f)-[:Y]->(b) CREATE (f)-[:X]->(b) CREATE (f)<-[:X]-(b)");

        TestUtil.testCall(db, "MATCH (f:Foo),(b:Bar)  RETURN apoc.node.degree(f, '<') as in, apoc.node.degree(f, '>') as out", (r) -> {
            assertEquals(1l, r.get("in"));
            assertEquals(2l, r.get("out"));
        });

    }

    @Test
    public void testDegreeInOutDirectionOnly() {
        db.executeTransactionally("CREATE (a:Person{name:'test'}) CREATE (b:Person) CREATE (c:Person) CREATE (d:Person) CREATE (a)-[:Rel1]->(b) CREATE (a)-[:Rel1]->(c) CREATE (a)-[:Rel2]->(d) CREATE (a)-[:Rel1]->(b) CREATE (a)<-[:Rel2]-(b) CREATE (a)<-[:Rel2]-(c) CREATE (a)<-[:Rel2]-(d) CREATE (a)<-[:Rel1]-(d)");

        TestUtil.testCall(db, "MATCH (a:Person{name:'test'})  RETURN apoc.node.degree.in(a) as in, apoc.node.degree.out(a) as out", (r) -> {
            assertEquals(4l, r.get("in"));
            assertEquals(4l, r.get("out"));
        });

    }

    @Test
    public void testDegreeInOutType() {
        db.executeTransactionally("CREATE (a:Person{name:'test'}) CREATE (b:Person) CREATE (c:Person) CREATE (d:Person) CREATE (a)-[:Rel1]->(b) CREATE (a)-[:Rel1]->(c) CREATE (a)-[:Rel2]->(d) CREATE (a)-[:Rel1]->(b) CREATE (a)<-[:Rel2]-(b) CREATE (a)<-[:Rel2]-(c) CREATE (a)<-[:Rel2]-(d) CREATE (a)<-[:Rel1]-(d)");

        TestUtil.testCall(db, "MATCH (a:Person{name:'test'})  RETURN apoc.node.degree.in(a, 'Rel1') as in1, apoc.node.degree.out(a, 'Rel1') as out1, apoc.node.degree.in(a, 'Rel2') as in2, apoc.node.degree.out(a, 'Rel2') as out2", (r) -> {
            assertEquals(1l, r.get("in1"));
            assertEquals(3l, r.get("out1"));
            assertEquals(3l, r.get("in2"));
            assertEquals(1l, r.get("out2"));
        });

    }

    @Test
    public void testId() {
        assertTrue(TestUtil.<Long>singleResultFirstColumn(db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.id(f) AS id") >= 0);
        assertTrue(TestUtil.<Long>singleResultFirstColumn(db, "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.id(node) AS id") < 0);
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.node.id(null) AS id"));
    }
    @Test
    public void testRelId() {
        assertTrue(TestUtil.<Long>singleResultFirstColumn(db, "CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.id(rel) AS id") >= 0);
        assertTrue(TestUtil.<Long>singleResultFirstColumn(db, "CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.id(rel) AS id") < 0);
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.rel.id(null) AS id"));
    }
    @Test
    public void testLabels() {
        assertEquals(singletonList("Foo"), TestUtil.<List<String>>singleResultFirstColumn(db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.node.labels(f) AS labels"));
        assertEquals(singletonList("Foo"), TestUtil.<List<String>>singleResultFirstColumn(db, "CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.node.labels(node) AS labels"));
        assertNull(TestUtil.singleResultFirstColumn(db, "RETURN apoc.node.labels(null) AS labels"));
    }

    @Test
    public void testProperties() {
        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db, "CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f) AS props"));
        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node) AS props"));

        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel) AS props"));
        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel) AS props"));

        assertNull(TestUtil.singleResultFirstColumn(db,"RETURN apoc.any.properties(null) AS props"));
    }

    @Test
    public void testSubProperties() {
        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['foo']) AS props"));
        assertEquals(emptyMap(), TestUtil.singleResultFirstColumn(db,"CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(f,['bar']) AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.properties(null,['foo']) AS props"));
        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['foo']) AS props"));
        assertEquals(emptyMap(), TestUtil.singleResultFirstColumn(db,"CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.properties(node,['bar']) AS props"));

        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['foo']) AS props"));
        assertEquals(emptyMap(), TestUtil.singleResultFirstColumn(db,"CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.any.properties(rel,['bar']) AS props"));
        assertEquals(singletonMap("foo","bar"), TestUtil.singleResultFirstColumn(db,"CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['foo']) AS props"));
        assertEquals(emptyMap(), TestUtil.singleResultFirstColumn(db,"CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.properties(rel,['bar']) AS props"));

        assertNull(TestUtil.singleResultFirstColumn(db,"RETURN apoc.any.properties(null,['foo']) AS props"));
    }

    @Test
    public void testProperty() {
        assertEquals("bar", TestUtil.singleResultFirstColumn(db,"RETURN apoc.any.property({foo:'bar'},'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"RETURN apoc.any.property({foo:'bar'},'bar') AS props"));

        assertEquals("bar", TestUtil.singleResultFirstColumn(db,"CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"CREATE (f:Foo {foo:'bar'}) RETURN apoc.any.property(f,'bar') AS props"));

        assertEquals("bar", TestUtil.singleResultFirstColumn(db,"CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"CREATE (f)-[r:REL {foo:'bar'}]->(f) RETURN apoc.any.property(r,'bar') AS props"));

        assertEquals("bar", TestUtil.singleResultFirstColumn(db,"CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"CALL apoc.create.vNode(['Foo'],{foo:'bar'}) YIELD node RETURN apoc.any.property(node,'bar') AS props"));

        assertEquals("bar", TestUtil.singleResultFirstColumn(db,"CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.any.property(rel,'bar') AS props"));

        assertNull(TestUtil.singleResultFirstColumn(db,"RETURN apoc.any.property(null,'foo') AS props"));
        assertNull(TestUtil.singleResultFirstColumn(db,"RETURN apoc.any.property(null,null) AS props"));
    }

    @Test
    public void testRelType() {
        assertEquals("REL", TestUtil.singleResultFirstColumn(db,"CREATE (f)-[rel:REL {foo:'bar'}]->(f) RETURN apoc.rel.type(rel) AS type"));

        assertEquals("REL", TestUtil.singleResultFirstColumn(db,"CREATE (f) WITH f CALL apoc.create.vRelationship(f,'REL',{foo:'bar'},f) YIELD rel RETURN apoc.rel.type(rel) AS type"));

        assertNull(TestUtil.singleResultFirstColumn(db,"RETURN apoc.rel.type(null) AS type"));
    }

    @Test
    public void testMergeSelfRelationship() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, selfRel: true, countMerge: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();

                    assertMerge(map,
                            Util.map("name","b1", "count", 2), label, //FROM
                            Util.map("count", 1), "KNOWS", //REL
                            Util.map("name", "b1", "count", 2), label); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeSelfRelationshipInverted() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("BLabel"), label("ALabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([c,p],{mergeVirtualRels:true, selfRel: true, countMerge: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(map,
                            Util.map("name","a1", "count", 2), label, //FROM
                            Util.map("count", 1), "KNOWS", //REL
                            Util.map("name", "a1", "count", 2), label); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeNotSelfRelationship() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE (a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, countMerge: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();

                    assertEquals(Util.map("name","b1", "count", 2), ((VirtualNode)map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode)map.get("from")));
                    assertNull(((VirtualRelationship) map.get("rel")));
                    assertNull(((Node) map.get("to")));
                    assertFalse(r.hasNext());
                   
                });
    }

    @Test
    public void testMergeWithRelationshipDirection() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE " +
                "(a1:ALabel {name:'a1'})-[:KNOWS]->(b1:BLabel {name:'b1'})," +
                "(a1)<-[:KNOWS]-(b2:CLabel {name:'c1'})");

        Set<Label> label = asSet(label("ALabel"), label("BLabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel)-[r:KNOWS]->(c:BLabel) WITH p,c " +
                        "CALL apoc.nodes.collapse([p,c],{mergeVirtualRels:true, selfRel: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();

                    assertEquals(Util.map("name","c1"), ((Node)map.get("from")).getAllProperties());
                    assertEquals(asList(label("CLabel")), ((Node)map.get("from")).getLabels());
                    assertEquals(Collections.emptyMap(), ((VirtualRelationship)map.get("rel")).getAllProperties());
                    assertEquals("KNOWS", ((VirtualRelationship)map.get("rel")).getType().name());
                    assertEquals(Util.map("name", "b1", "count", 2), ((Node)map.get("to")).getAllProperties());
                    assertEquals(label, labelSet((Node)map.get("to")));
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertEquals(Util.map("name","b1", "count", 2), ((VirtualNode)map.get("from")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode)map.get("from")));
                    assertEquals(Util.map("count", 1), ((VirtualRelationship)map.get("rel")).getAllProperties());
                    assertEquals("KNOWS", ((VirtualRelationship)map.get("rel")).getType().name());
                    assertEquals(Util.map("name", "b1", "count", 2), ((VirtualNode)map.get("to")).getAllProperties());
                    assertEquals(label, labelSet((VirtualNode)map.get("to")));
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeRelationship() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE " +
                "(a1:ALabel {name:'a1'})-[:HAS_REL]->(b1:BLabel {name:'b1'})," +
                "(a2:ALabel {name:'a2'})-[:HAS_REL]->(b1)," +
                "(a4:ALabel {name:'a4'})-[:HAS_REL]->(b4:BLabel {name:'b4'})");

        Set<Label> label = asSet(label("ALabel"));

        TestUtil.testResult(db,
                "MATCH (p:ALabel{name:'a4'}), (p1:ALabel{name:'a2'}), (p2:ALabel{name:'a1'}) WITH p, p1, p2 " +
                        "CALL apoc.nodes.collapse([p, p1, p2],{mergeVirtualRels:true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(map,
                            Util.map("name", "a1", "count", 3), label, //FROM
                            Collections.emptyMap(), "HAS_REL", //REL
                            Util.map("name", "b4"), asSet(label("BLabel"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "a1", "count", 3), label, //FROM
                            Util.map("count", 1), "HAS_REL", //REL
                            Util.map("name", "b1"), asSet(label("BLabel"))); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergePersonEmployee() {
        db.executeTransactionally("MATCH (n) detach delete (n)");
        db.executeTransactionally("CREATE " +
                "(:Person {name:'mike'})-[:LIVES_IN]->(:City{name:'rome'}), " +
                "(:Employee{name:'mike'})-[:WORKS_FOR]->(:Company{name:'Larus'}), " +
                "(:Person {name:'kate'})-[:LIVES_IN]->(:City{name:'london'}), " +
                "(:Employee{name:'kate'})-[:WORKS_FOR]->(:Company{name:'Neo'})");

        Set<Label> label = asSet(label("Collapsed"), label("Person"), label("Employee"));

        TestUtil.testResult(db,
                "MATCH (p:Person)-[r:LIVES_IN]->(c:City), (e:Employee)-[w:WORKS_FOR]->(m:Company) WITH p,r,c,e,w,m WHERE p.name = e.name " +
                        "CALL apoc.nodes.collapse([p,e],{properties:'combine', mergeVirtualRels:true, countMerge: true, collapsedLabel: true}) yield from, rel, to " +
                        "return from, rel, to", (r) -> {
                    Map<String, Object> map = r.next();
                    assertMerge(map,
                            Util.map("name", "mike", "count", 2), label, //FROM
                            Collections.emptyMap(), "LIVES_IN", //REL
                            Util.map("name", "rome"), asSet(label("City"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "mike", "count", 2), label, //FROM
                            Collections.emptyMap(), "WORKS_FOR", //REL
                            Util.map("name", "Larus"), asSet(label("Company"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "kate","count", 2), label, //FROM
                            Collections.emptyMap(), "LIVES_IN", //REL
                            Util.map("name", "london"), asSet(label("City"))); //TO
                    assertTrue(r.hasNext());
                    map = r.next();
                    assertMerge(map,
                            Util.map("name", "kate", "count", 2), label, //FROM
                            Collections.emptyMap(), "WORKS_FOR", //REL
                            Util.map("name", "Neo"), asSet(label("Company"))); //TO
                    assertFalse(r.hasNext());
                });
    }

    @Test
    public void testMergeVirtualNode() {
        db.executeTransactionally("CREATE \n" +
                "(p:Person {name: 'John'})-[:LIVES_IN]->(c:City{name:'London'}),\n" +
                "(p1:Person {name: 'Mike'})-[:LIVES_IN]->(c),\n" +
                "(p2:Person {name: 'Kate'})-[:LIVES_IN]->(c),\n" +
                "(p3:Person {name: 'Budd'})-[:LIVES_IN]->(c),\n" +
                "(p4:Person {name: 'Alex'})-[:LIVES_IN]->(c),\n" +
                "(p1)-[:KNOWS]->(p),\n" +
                "(p2)-[:KNOWS]->(p1),\n" +
                "(p2)-[:KNOWS]->(p3),\n" +
                "(p4)-[:KNOWS]->(p3)\n");

        Set<Label> label = asSet(label("City"), label("Person"));

        TestUtil.testResult(db, "MATCH (p:Person)-[:LIVES_IN]->(c:City)\n" +
                "WITH c, c + collect(p) as subgraph\n" +
                "CALL apoc.nodes.collapse(subgraph,{properties:'discard', mergeVirtualRels:true, countMerge: true}) yield from, rel, to return from, rel, to", null, result -> {
            Map<String, Object> map = result.next();

            assertEquals(Util.map("name","London", "count", 6), ((VirtualNode)map.get("from")).getAllProperties());
            assertEquals(label, labelSet((VirtualNode) map.get("from")));
            assertNull(map.get("rel"));
            assertNull(map.get("to"));
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testMergeVirtualNodeBOTH() {
        db.executeTransactionally("CREATE \n" +
                "(p:Person {name: 'John'})-[:LIVES_IN]->(c:City{name:'London'})," +
                "(c)-[:LIVES_IN]->(p)");

        Set<Label> label = asSet(label("City"), label("Person"));

        TestUtil.testResult(db, "MATCH (p:Person)-[:LIVES_IN]->(c:City)-[:LIVES_IN]->(b:Person)\n" +
                "CALL apoc.nodes.collapse([c,p,b],{mergeVirtualRels:true, countMerge: true, selfRel: true}) yield from, rel, to return from, rel, to", null, result -> {
            Map<String, Object> map = result.next();

            assertMerge(map,
                    Util.map("name", "John", "count", 2), label, //FROM
                    Util.map("count", 3), "LIVES_IN", //REL
                    Util.map("name", "John", "count", 2), label); //TO
            assertFalse(result.hasNext());
        });
    }

    private static void assertMerge(Map<String, Object> map,
                                    Map<String, Object> fromProperties, Set<Label> fromLabel,
                                    Map<String, Object> relProperties, String relType,
                                    Map<String, Object> toProperties, Set<Label> toLabel
    ) {
        assertEquals(fromProperties, ((VirtualNode)map.get("from")).getAllProperties());
        assertEquals(fromLabel, labelSet((VirtualNode)map.get("from")));
        assertEquals(relProperties, ((VirtualRelationship)map.get("rel")).getAllProperties());
        assertEquals(relType, ((VirtualRelationship)map.get("rel")).getType().name());
        assertEquals(toProperties, ((Node)map.get("to")).getAllProperties());
        assertEquals(toLabel, labelSet((Node)map.get("to")));
    }

    private static Set<Label> labelSet(Node node) {
	   return Iterables.asSet(node.getLabels());
    }
}