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
package apoc.create;

import apoc.coll.Coll;
import apoc.path.Paths;
import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.result.VirtualNode.ERROR_NODE_NULL;
import static apoc.result.VirtualRelationship.ERROR_END_NODE_NULL;
import static apoc.result.VirtualRelationship.ERROR_START_NODE_NULL;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.Label.label;

public class CreateTest {

    public static final Label PERSON = Label.label("Person");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db,Create.class, Paths.class, Coll.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testCreateNode() {
        testCall(db, "CALL apoc.create.node(['Person'],{name:'John'})",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertTrue(node.hasLabel(label("Person")));
                    assertEquals("John", node.getProperty("name"));
                });
    }

    @Test
    public void testCreateNodeWithArrayProps() {
        testCall(db, "CALL apoc.create.node(['Person'],{name:['John','Doe'],kids:[],age:[32,10]})",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertTrue(node.hasLabel(label("Person")));
                    assertArrayEquals(new String[]{"John", "Doe"}, (String[]) node.getProperty("name"));
                    assertArrayEquals(new String[]{}, (String[]) node.getProperty("kids"));
                    assertArrayEquals(new long[]{32, 10}, (long[]) node.getProperty("age"));
                });
    }

    @Test
    public void testCreateAddLabel() {
        testResult(db, """
                        CREATE (n), (m), (o)
                        WITH n, m, o
                        CALL apoc.create.addLabels([id(n), m, elementId(o)], ['Foo', 'Bar'])
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testCreateSetLabel() {
        testResult(db, """
                        CREATE (n:Baz), (m:Boo), (o:Bop)
                        WITH n, m, o
                        CALL apoc.create.setLabels([id(n), m, elementId(o)], ['Foo', 'Bar'])
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testCreateRemoveLabel() {
        testResult(db, """
                        CREATE (n:Foo:Bar:Boo), (m:Foo:Bar:Boo), (o:Foo:Bar:Boo)
                        WITH n, m, o
                        CALL apoc.create.removeLabels([id(n), m, elementId(o)], ['Boo'])
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    row = result.next();
                    assertEquals(List.of(label("Foo"), label("Bar")), ((Node) row.get("node")).getLabels());
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testSetProperty() {
        testResult(db, """
                        CREATE (n), (m), (o)
                        WITH n, m, o
                        CALL apoc.create.setProperty([id(n), m, elementId(o)],'name','John')
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
                    row = result.next();
                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
                    row = result.next();
                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testRemoveProperty() {
        testResult(db, """
                        CREATE (n:Foo {name:'foo'}), (m:Foo {name:'bar'}), (o:Foo {name:'baz'})
                        WITH n, m, o
                        CALL apoc.create.setProperty([n, id(m), elementId(o)],'name',null)
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertFalse(((Node) row.get("node")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Node) row.get("node")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Node) row.get("node")).hasProperty("name"));
                    assertFalse(result.hasNext());
                });
        testResult(db, """
                        CREATE (n:Foo {name:'foo'}), (m:Foo {name:'bar'}), (o:Foo {name:'baz'})
                        WITH n, m, o
                        CALL apoc.create.removeProperties([n, id(m), elementId(o)],['name'])
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertFalse(((Node) row.get("node")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Node) row.get("node")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Node) row.get("node")).hasProperty("name"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testRemoveRelProperty() {
        testResult(db, """
                        CREATE ()-[r1:TEST {name:'foo'}]->(), ()-[r2:TEST {name:'bar'}]->(), ()-[r3:TEST {name:'baz'}]->()
                        WITH r1, r2, r3
                        CALL apoc.create.setRelProperty([r1, id(r2), elementId(r3)], 'name', null)
                        YIELD rel
                        RETURN rel""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertFalse(((Relationship) row.get("rel")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Relationship) row.get("rel")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Relationship) row.get("rel")).hasProperty("name"));
                    assertFalse(result.hasNext());
                });
        testResult(db, """
                        CREATE ()-[r1:TEST {name:'foo'}]->(), ()-[r2:TEST {name:'bar'}]->(), ()-[r3:TEST {name:'baz'}]->()
                        WITH r1, r2, r3
                        CALL apoc.create.removeRelProperties([r1, id(r2), elementId(r3)], ['name'])
                        YIELD rel
                        RETURN rel""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertFalse(((Relationship) row.get("rel")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Relationship) row.get("rel")).hasProperty("name"));
                    row = result.next();
                    assertFalse(((Relationship) row.get("rel")).hasProperty("name"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testSetRelProperties() {
        testResult(db, """
                        CREATE ()-[r:X]->(), ()-[r2:Y]->(), ()-[r3:Y]->()
                        WITH r, r2, r3
                        CALL apoc.create.setRelProperties([id(r), r2, elementId(r3)], ['name','age'], ['John',42])
                        YIELD rel
                        RETURN rel""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    Relationship r = (Relationship) row.get("rel");
                    assertEquals("John", r.getProperty("name"));
                    assertEquals(42L, r.getProperty("age"));
                    row = result.next();
                    r = (Relationship) row.get("rel");
                    assertEquals("John", r.getProperty("name"));
                    assertEquals(42L, r.getProperty("age"));
                    row = result.next();
                    r = (Relationship) row.get("rel");
                    assertEquals("John", r.getProperty("name"));
                    assertEquals(42L, r.getProperty("age"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testSetRelProperty() {
        testResult(db, """
                        CREATE (n)-[r:X]->(m), (m)-[r2:Y]->(n), (m)-[r3:Z]->(n)
                        WITH r, r2, r3
                        CALL apoc.create.setRelProperty([id(r), r2, elementId(r3)], 'name', 'John')
                        YIELD rel RETURN rel""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals("John", ((Relationship) row.get("rel")).getProperty("name"));
                    row = result.next();
                    assertEquals("John", ((Relationship) row.get("rel")).getProperty("name"));
                    row = result.next();
                    assertEquals("John", ((Relationship) row.get("rel")).getProperty("name"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testSetProperties() {
        testResult(db, """
                        CREATE (n), (m), (o)
                        WITH n, m, o
                        CALL apoc.create.setProperties([id(n), m, elementId(o)], ['name','age'], ['John',42])
                        YIELD node
                        RETURN node""",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
                    assertEquals(42L, ((Node) row.get("node")).getProperty("age"));
                    row = result.next();
                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
                    assertEquals(42L, ((Node) row.get("node")).getProperty("age"));
                    row = result.next();
                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
                    assertEquals(42L, ((Node) row.get("node")).getProperty("age"));
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testVirtualNode() {
        testCall(db, "CALL apoc.create.vNode(['Person'],{name:'John'})",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertTrue(node.hasLabel(label("Person")));
                    assertEquals("John", node.getProperty("name"));
                });
    }

    @Test
    public void testVirtualNodeFunction() {
        testCall(db, "RETURN apoc.create.vNode(['Person'],{name:'John'}) as node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertTrue(node.hasLabel(label("Person")));
                    assertEquals("John", node.getProperty("name"));
                });
    }

    @Test
    public void testCreateNodes() {
        testResult(db, "CALL apoc.create.nodes(['Person'],[{name:'John'},{name:'Jane'}])",
                (res) -> {
                    Node node = (Node) res.next().get("node");
                    assertTrue(node.hasLabel(PERSON));
                    assertEquals("John", node.getProperty("name"));

                    node = (Node) res.next().get("node");
                    assertTrue(node.hasLabel(PERSON));
                    assertEquals("Jane", node.getProperty("name"));
                });
    }

    @Test
    public void testCreateRelationship() {
        testCall(db, "CREATE (n),(m) WITH n,m CALL apoc.create.relationship(n,'KNOWS',{since:2010}, m) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertTrue(rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                });
    }

    @Test
    public void testCreateVirtualRelationship() {
        testCall(db, "CREATE (n),(m) WITH n,m CALL apoc.create.vRelationship(n,'KNOWS',{since:2010}, m) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertTrue(rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                });
    }

    @Test
    public void testCreateVirtualRelationshipFunction() {
        testCall(db, "CREATE (n),(m) WITH n,m RETURN apoc.create.vRelationship(n,'KNOWS',{since:2010}, m) AS rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertTrue(rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                });
    }

    @Test
    public void testCreatePattern() {
        testCall(db, "CALL apoc.create.virtualPath(['Person'],{name:'John'},'KNOWS',{since:2010},['Person'],{name:'Jane'})",
                (row) -> {
                    Node john = (Node) row.get("from");
                    assertTrue(john.hasLabel(PERSON));
                    assertEquals("John", john.getProperty("name"));
                    Relationship rel = (Relationship) row.get("rel");
                    assertTrue(rel.isType(RelationshipType.withName("KNOWS")));
                    assertEquals(2010L, rel.getProperty("since"));
                    Node jane = (Node) row.get("to");
                    assertTrue(jane.hasLabel(PERSON));
                    assertEquals("Jane", jane.getProperty("name"));
                });
    }

    @Test
    public void testVirtualFromNodeFunction() {
        testCall(db, """
                        CREATE (n:Person{name:'Vincent', born: 1974} )
                        RETURN apoc.create.virtual.fromNode(n, ['name']) AS node
                        """,
                (row) -> {
                    Node node = (Node) row.get("node");

                    assertTrue(node.hasLabel(label("Person")));
                    assertEquals("Vincent", node.getProperty("name"));
                    assertNull(node.getProperty("born"));
                });
    }
    
    @Test
    public void testVirtualFromNodeShouldNotEditOriginalOne() {
        db.executeTransactionally("CREATE (n:Person {name:'toUpdate'})");

        testCall(db, """
                        MATCH (n:Person {name:'toUpdate'})
                        WITH apoc.create.virtual.fromNode(n, ['name']) as nVirtual
                        CALL apoc.create.setProperty(nVirtual, 'ajeje', 0) YIELD node RETURN node
                        """,
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals("toUpdate", node.getProperty("name"));
                    assertEquals(0L, node.getProperty("ajeje"));
                });
        
        testCall(db, """
                        MATCH (n:Person {name:'toUpdate'})
                        WITH apoc.create.virtual.fromNode(n, ['name']) as node
                        SET node.ajeje = 0 RETURN node""",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals("toUpdate", node.getProperty("name"));
                    assertFalse(node.hasProperty("ajeje"));
                });
        
        testCall(db, "MATCH (node:Person {name:'toUpdate'}) RETURN node",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals("toUpdate", node.getProperty("name"));
                    assertFalse(node.hasProperty("ajeje"));
                });
    }
    
    @Test
    public void testClonePathShouldNotEditOriginalOne() {
        db.executeTransactionally("CREATE (n:Person {name:'toUpdate'})-[:MY_REL]->(:Another {alpha: 0})");
        testCall(db, """
                        MATCH p=(n:Person {name:'toUpdate'})-[:MY_REL]->(:Another {alpha: 0})
                        WITH p CALL apoc.create.clonePathToVirtual(p) YIELD path WITH nodes(path) AS nodes, relationships(path) as rels
                        WITH nodes[0] as start, nodes[1] as end, rels[0] as rel
                        CALL apoc.create.setProperty(start, 'ajeje', 0) YIELD node as startUpdated
                        WITH startUpdated, end, rel
                        CALL apoc.create.setProperties(end, ['brazorf'], ['abc']) YIELD node as endUpdated
                        WITH startUpdated, endUpdated, rel
                        CALL apoc.create.setRelProperty(rel, 'foo', 'bar') YIELD rel as relFirstSet
                        WITH startUpdated, endUpdated, relFirstSet
                        CALL apoc.create.setRelProperties(relFirstSet, ['baz'], ['bar']) YIELD rel as relUpdated
                        RETURN startUpdated, endUpdated, relUpdated""",
                (row) -> {
                    Node start = (Node) row.get("startUpdated");
                    assertEquals("toUpdate", start.getProperty("name"));
                    assertEquals(0L, start.getProperty("ajeje"));
                    
                    Node end = (Node) row.get("endUpdated");
                    assertEquals(0L, end.getProperty("alpha"));
                    assertEquals("abc", end.getProperty("brazorf"));
                    
                    Relationship rel = (Relationship) row.get("relUpdated");
                    assertEquals("bar", rel.getProperty("foo"));
                    assertEquals("bar", rel.getProperty("baz"));
                });
        
        testCall(db, """
                        MATCH p=(n:Person {name:'toUpdate'})-[:MY_REL]->(:Another {alpha: 0})
                        WITH p CALL apoc.create.clonePathToVirtual(p) YIELD path WITH nodes(path) AS nodes, relationships(path) as rels
                        WITH nodes[0] as start, nodes[1] as end, rels[0] as rel
                        SET start.ajeje = 0, end.brazorf = 'abc', rel.foo = 'bar'
                        RETURN start, end, rel""",
                (row) -> {
                    Node start = (Node) row.get("start");
                    assertEquals("toUpdate", start.getProperty("name"));
                    assertFalse(start.hasProperty("ajeje"));
                    Node end = (Node) row.get("end");
                    assertTrue(end.hasLabel(label("Another")));
                    assertEquals(0L, end.getProperty("alpha"));
                    assertFalse(end.hasProperty("brazorf"));
                    
                    Relationship rel = (Relationship) row.get("rel");
                    assertFalse(rel.hasProperty("foo"));
                });

        testCall(db, "MATCH (start:Person {name:'toUpdate'}), (end:Another {alpha: 0})  RETURN start, end",
                (row) -> {
                    Node start = (Node) row.get("start");
                    assertFalse(start.hasProperty("ajeje"));
                    Node end = (Node) row.get("end");
                    assertFalse(end.hasProperty("brazorf"));
                });
    }

    @Test
    public void testClonePathWithMixedDirectionRelationships() {
        // rel `:b` is to the left, rel `d` is to the right
        db.executeTransactionally("CREATE (:a {id: 1})<-[:b {id: 10}]-(:c {id: 2})-[:d {id: 20}]->(:e {id: 3})");

        testCall(db, """
                MATCH p = (:a)<-[:b]-(:c)-[:d]->(:e)
                CALL apoc.create.clonePathToVirtual(p) YIELD path RETURN path""", r -> {
            Path path = (Path) r.get("path");
            Iterator<Node> nodes = path.nodes().iterator();
            Node node = nodes.next();
            assertEquals(Map.of("id", 1L), node.getAllProperties());
            node = nodes.next();
            assertEquals(Map.of("id", 2L), node.getAllProperties());
            node = nodes.next();
            assertEquals(Map.of("id", 3L), node.getAllProperties());
            assertFalse(nodes.hasNext());

            Iterator<Relationship> rels = path.relationships().iterator();
            Relationship rel = rels.next();
            assertEquals(Map.of("id", 10L), rel.getAllProperties());
            rel = rels.next();
            assertEquals(Map.of("id", 20L), rel.getAllProperties());
            assertFalse(rels.hasNext());

        });
    }

    @Test
    public void testClonePathShouldNotDuplicateRelsWithMultipaths() {
        //create path with single rels
        db.executeTransactionally("""
                CREATE (n1:Node {id: 1}),
                              (n2:Node),
                              (n3:Node),
                              (n1)-[:R]->(n2)-[:R]->(n3)"""
        );
        
        // returns a list with all rels
        testCall(db, """
                MATCH p=(:Node {id: 1})-[:R*..2]->(:Node)
                WITH collect(p) AS paths
                CALL apoc.create.clonePathsToVirtual(paths)
                YIELD path
                WITH collect( relationships(path) ) as pathRels
                RETURN apoc.coll.flatten(pathRels) as rels""", r -> {
            final List<Relationship> rels = (List) r.get("rels");
            assertEquals(3, rels.size());

            // group the rels by id and check that there are not duplicated
            Map<String, List<Relationship>> relsById = rels
                    .stream()
                    .collect(Collectors.groupingBy(Entity::getElementId));

            assertEquals(2, relsById.size());
        });
    }
    
    @Test
    public void testVirtualPath() {
        db.executeTransactionally("CREATE p=(a:Test {foo: 7})-[:TEST]->(b:Baa:Baz {a:'b'})<-[:TEST_2 {aa:'bb'}]-(:Bar {one:'www'}), \n" +
                "q=(:Omega {alpha: 'beta'})<-[:TEST_3 {aa:'ccc'}]-(:Bar {one:'jjj'})");
        testCall(db, "MATCH p=(a:Test {foo: 7})-[:TEST]->(b:Baa:Baz {a:'b'})<-[:TEST_2 {aa:'bb'}]-(:Bar {one:'www'}) WITH p \n" +
                        "CALL apoc.create.clonePathToVirtual(p) YIELD path RETURN path",
                (row) -> assertionsFirstVirtualPath((Path) row.get("path")));

        testResult(db, """
                        MATCH p=(a:Test {foo: 7})-[:TEST]->(b:Baa:Baz {a:'b'})<-[:TEST_2 {aa:'bb'}]-(:Bar {one:'www'}),\s
                        q=(:Omega {alpha: 'beta'})<-[:TEST_3 {aa:'ccc'}]-(:Bar {one:'jjj'}) WITH [p, q] as paths\s
                        CALL apoc.create.clonePathsToVirtual(paths) YIELD path RETURN path""",
                (res) -> {
                    ResourceIterator<Path> paths = res.columnAs("path");
                    Path firstPath = paths.next();
                    assertionsFirstVirtualPath(firstPath);
                    Path secondPath = paths.next();
                    Iterator<Node> nodes = secondPath.nodes().iterator();
                    Node firstNode = nodes.next();
                    assertEquals(List.of(label("Omega")), firstNode.getLabels());
                    assertEquals(Map.of("alpha", "beta"), firstNode.getAllProperties());

                    Node secondNode = nodes.next();
                    assertEquals(List.of(label("Bar")), secondNode.getLabels());
                    assertEquals(Map.of("one", "jjj"), secondNode.getAllProperties());
                    assertFalse(nodes.hasNext());
                    
                    Iterator<Relationship> rels = secondPath.relationships().iterator();
                    Relationship relationship = rels.next();
                    assertEquals("TEST_3", relationship.getType().name());
                    assertEquals(Map.of("aa", "ccc"), relationship.getAllProperties());
                    assertFalse(rels.hasNext());
                    assertFalse(paths.hasNext());
                });
    }

    private void assertionsFirstVirtualPath(Path path) {
        Iterator<Node> nodes = path.nodes().iterator();
        Node firstNode = nodes.next();
        assertEquals(List.of(label("Test")), firstNode.getLabels());
        assertEquals(Map.of("foo", 7L), firstNode.getAllProperties());

        Node secondNode = nodes.next();
        assertEquals(Set.of(label("Baa"), label("Baz")), Iterables.asSet(secondNode.getLabels()));
        assertEquals(Map.of("a", "b"), secondNode.getAllProperties());
        
        Node thirdNode = nodes.next();
        assertEquals(List.of(label("Bar")), thirdNode.getLabels());
        assertEquals(Map.of("one", "www"), thirdNode.getAllProperties());
        assertFalse(nodes.hasNext());

        Iterator<Relationship> rels = path.relationships().iterator();
        Relationship firstRel = rels.next();
        assertEquals("TEST", firstRel.getType().name());
        assertTrue(firstRel.getAllProperties().isEmpty());
        
        Relationship secondRel = rels.next();
        assertEquals("TEST_2", secondRel.getType().name());
        assertEquals(Map.of("aa", "bb"), secondRel.getAllProperties());
        assertFalse(rels.hasNext());
    }

    @Test
    public void testValidationNodes() {
        assertionsError(ERROR_NODE_NULL,"RETURN apoc.create.virtual.fromNode(null, ['name']) as node");
        assertionsError(ERROR_START_NODE_NULL, "CREATE (n) WITH n CALL apoc.create.relationship(null,'KNOWS',{}, n) YIELD rel RETURN rel");
        assertionsError(ERROR_END_NODE_NULL, "CREATE (n) WITH n CALL apoc.create.relationship(n,'KNOWS',{}, null) YIELD rel RETURN rel");
        assertionsError(ERROR_START_NODE_NULL, "CREATE (m) RETURN apoc.create.vRelationship(null,'KNOWS',{}, m) AS rel");
        assertionsError(ERROR_END_NODE_NULL, "CREATE (n) WITH n CALL apoc.create.vRelationship(n,'KNOWS',{}, null) YIELD rel RETURN rel");
    }

    private void assertionsError(String expectedMessage, String query) {
        try {
            testCall(db, query, (row) -> fail("Should fail because of " + expectedMessage));
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertEquals(expectedMessage, rootCause.getMessage());
            assertTrue(rootCause instanceof RuntimeException);
        }
    }

}
