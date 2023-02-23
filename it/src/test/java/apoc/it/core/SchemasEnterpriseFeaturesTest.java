package apoc.it.core;

import apoc.result.AssertSchemaResult;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import junit.framework.TestCase;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.schema.SchemasTest.CALL_SCHEMA_NODES_ORDERED;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author as
 * @since 12.02.19
 */
public class SchemasEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    // coherently with SchemasTest we remove all indexes/constraints before (e.g. to get rid of lookup indexes)
    @Before
    public void removeAllConstraints() {
        session.writeTransaction(tx -> {
            final List<String> constraints = tx.run("SHOW CONSTRAINTS YIELD name").list(i -> i.get("name").asString());
            constraints.forEach(name -> tx.run(String.format("DROP CONSTRAINT %s", name)));

            final List<String> indexes = tx.run("SHOW INDEXES YIELD name").list(i -> i.get("name").asString());
            indexes.forEach(name -> tx.run(String.format("DROP INDEX %s", name)));
            tx.commit();
            return null;
        });
    }

    @Test
    public void testAddConstraintDoesntAllowCypherInjection() {
        String query = "CALL apoc.schema.assert(null,{Bar:[[\"foo`) IS UNIQUE MATCH (n) DETACH DELETE n; //\", \"bar\"]]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals(expectedKeys("foo`) IS UNIQUE MATCH (n) DETACH DELETE n; //", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testKeptNodeKeyAndUniqueConstraintIfExists() {
        String query = "CALL apoc.schema.assert(null,{Foo:[['foo','bar']]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });

        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(1, result.size());
            Map<String, Object> firstResult = result.get(0).asMap();
            assertThat( (String) firstResult.get( "createStatement" ) )
                    .contains( "CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS NODE KEY" );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testSchemaNodesWithNodeKey() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT node_key_movie FOR (m:Movie) REQUIRE (m.first, m.second) IS NODE KEY");
            tx.commit();
            return null;
        });

        testResult(session, CALL_SCHEMA_NODES_ORDERED, (result) -> {
            Map<String, Object> r = result.next();
            schemaNodeKeyAssertions(r);
            assertEquals("", r.get("status"));
            assertEquals("NODE_KEY", r.get("type"));
            final String expectedUserDescConstraint = "name='node_key_movie', type='NODE KEY', schema=(:Movie {first, second}), ownedIndex";
            Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescConstraint);

            r = result.next();
            schemaNodeKeyAssertions(r);
            assertEquals("ONLINE", r.get("status"));
            assertEquals("RANGE", r.get("type"));
            final String expectedUserDescIdx = "name='node_key_movie', type='RANGE', schema=(:Movie {first, second}), indexProvider='range-1.0', owningConstraint";
            Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescIdx);
            
            assertFalse(result.hasNext());
        });
    }

    private static void schemaNodeKeyAssertions(Map<String, Object> r) {
        assertEquals("Movie", r.get("label"));
        assertEquals(List.of("first", "second"), r.get("properties"));
        assertEquals(":Movie(first,second)", r.get("name"));
    }


    @Test
    public void testRelKeyConstraintIsKeptAndDropped() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_since FOR ()-[since:SINCE]-() REQUIRE (since.day, since.year) IS RELATIONSHIP KEY");
            tx.run("CREATE CONSTRAINT rel_con_knows FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY");
            tx.commit();
            return null;
        });

        HashSet<AssertSchemaResult> expectedResult = new HashSet<>();
        AssertSchemaResult sinceConstraint = new AssertSchemaResult("SINCE", List.of("day", "year"));
        sinceConstraint.unique = true;
        expectedResult.add(sinceConstraint);

        AssertSchemaResult knowsConstraint = new AssertSchemaResult("KNOWS", List.of("day", "year"));
        knowsConstraint.unique = true;
        knowsConstraint.action = "DROPPED";
        expectedResult.add(knowsConstraint);

        HashSet<AssertSchemaResult> actualResult = new HashSet<>();


        testResult(session, "CALL apoc.schema.assert({},{SINCE:[[\"day\", \"year\"]]})", (result) -> {
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                AssertSchemaResult con = new AssertSchemaResult(r.get("label"), (List<String>) r.get("keys"));
                con.unique = (boolean) r.get("unique");
                con.action = (String) r.get("action");
                actualResult.add(con);

                assertEquals(1, expectedResult.stream().filter(
                        c -> c.keys.containsAll(con.keys) &&
                                c.action.equals(con.action) &&
                                c.label.equals(con.label) &&
                                c.unique == con.unique
                ).toList().size());
            }

            assertEquals(expectedResult.size(), actualResult.size());
        });

        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT rel_con_since");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testSchemaRelationshipsExclude() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.commit();
            return null;
        });

        testResult(session, "CALL apoc.schema.relationships({excludeRelationships:['LIKED']})", (result) -> assertFalse(result.hasNext()));

        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT rel_con_liked");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testRelationshipConstraintsArentReturnedInNodesCheck() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.run("CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY");
            tx.commit();
            return null;
        });

        testResult(session, "CALL apoc.schema.nodes({})", (result) -> assertFalse(result.hasNext()));

        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT rel_con_liked");
            tx.run("DROP CONSTRAINT rel_con");
            tx.commit();
            return null;
        });
    }
    @Test
    public void testRelExistenceConstraintIsKeptAndDropped() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.run("CREATE CONSTRAINT rel_con_since FOR ()-[since:SINCE]-() REQUIRE (since.day) IS NOT NULL");
            tx.commit();
            return null;
        });

        HashSet<AssertSchemaResult> expectedResult = new HashSet<>();
        AssertSchemaResult sinceConstraint = new AssertSchemaResult("SINCE", "day");
        sinceConstraint.unique = true;
        expectedResult.add(sinceConstraint);

        AssertSchemaResult likedConstraint = new AssertSchemaResult("LIKED", "day");
        likedConstraint.unique = true;
        likedConstraint.action = "DROPPED";
        expectedResult.add(likedConstraint);

        HashSet<AssertSchemaResult> actualResult = new HashSet<>();

        testResult(session, "CALL apoc.schema.assert({},{SINCE:[\"day\"]})", (result) -> {
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                AssertSchemaResult con = new AssertSchemaResult((String) r.get("label"), (String) r.get("key"));
                con.unique = (boolean) r.get("unique");
                con.action = (String) r.get("action");
                actualResult.add(con);

                assertEquals(1, expectedResult.stream().filter(
                        c -> c.key.equals(con.key) &&
                                c.action.equals(con.action) &&
                                c.label.equals(con.label) &&
                                c.unique == con.unique
                ).toList().size());
            }

            assertEquals(expectedResult.size(), actualResult.size());
        });

        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT rel_con_since");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testKeptNodeKeyAndUniqueConstraintIfExistsAndDropExistingIsFalse() {
        String query = "CALL apoc.schema.assert(null,{Foo:[['foo','bar']]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(2, result.size());
            List<String> actualDescriptions = result.stream()
                    .map(record -> (String) record.asMap().get("createStatement"))
                    .collect(Collectors.toList());
            List<String> expectedDescriptions = List.of(
                    "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS NODE KEY",
                    "FOR (n:`Foo`) REQUIRE (n.`bar`, n.`foo`) IS NODE KEY" );
            assertMatchesAll( expectedDescriptions, actualDescriptions );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testCreateUniqueAndIsNodeKeyConstraintInSameLabel() {
        testResult(session, "CALL apoc.schema.assert(null,{Galileo: [['newton', 'tesla'], 'curie']}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Galileo", r.get("label"));
            assertEquals(expectedKeys("newton", "tesla"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            r = result.next();
            assertEquals("Galileo", r.get("label"));
            assertEquals(expectedKeys("curie"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(2, result.size());
            List<String> actualDescriptions = result.stream()
                    .map(record -> (String) record.asMap().get("createStatement"))
                    .collect(Collectors.toList());
            List<String> expectedDescriptions = List.of(
                    "FOR (n:`Galileo`) REQUIRE (n.`newton`, n.`tesla`) IS NODE KEY",
                    "FOR (n:`Galileo`) REQUIRE (n.`curie`) IS UNIQUE");
            assertMatchesAll( expectedDescriptions, actualDescriptions );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropNodeKeyConstraintAndCreateNodeKeyConstraintWhenUsingDropExistingOnlyIfNotExist() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.bar,f.foo) IS NODE KEY");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });

        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['baa','baz']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("baa","baz"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(1, result.size());
            Map<String, Object> firstResult = result.get(0).asMap();
            assertThat( (String) firstResult.get( "createStatement" ) )
                    .contains( "CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`baa`, n.`baz`) IS NODE KEY" );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropSchemaWithNodeKeyConstraintWhenUsingDropExisting() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.foo, f.bar) IS NODE KEY");
            tx.commit();
            return null;
        });
        testCall(session, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyNode() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (m:Movie) REQUIRE (m.title) IS NOT NULL");
            tx.commit();
            return null;
        });
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Movie", r.get("label"));
            assertEquals(expectedKeys("title"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyRelationship() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR ()-[acted:Acted]->() REQUIRE (acted.since) IS NOT NULL");
            tx.commit();
            return null;
        });

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Acted", r.get("label"));
            assertEquals(expectedKeys("since"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            tx.commit();
            return null;
        });
    }

    @Test
    public void testIndexOnMultipleProperties() {
        session.writeTransaction(tx -> {
            tx.run("CREATE INDEX FOR (n:Foo) ON (n.bar, n.foo)");
            tx.commit();
            return null;
        });

        String indexName = session.readTransaction(tx -> {
            String name = tx.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name").single().get("name").asString();
            tx.commit();
            return name;
        });

        session.writeTransaction(tx -> {
            tx.run("CALL db.awaitIndex($indexName)", Collections.singletonMap("indexName", indexName));
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.nodes() YIELD name, label, properties, status, type, " +
                "failure, populationProgress, size, valuesSelectivity, userDescription " +
                "WHERE label <> '<any-labels>' " +
                "RETURN *", (result) -> {
            // Get the index info
            Map<String, Object> r = result.next();

            assertEquals(":Foo(bar,foo)", r.get("name"));
            assertEquals("ONLINE", r.get("status"));
            assertEquals("Foo", r.get("label"));
            assertEquals("RANGE", r.get("type"));
            assertTrue(((List<String>) r.get("properties")).contains("bar"));
            assertTrue(((List<String>) r.get("properties")).contains("foo"));

            assertTrue(!result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP INDEX " + indexName);
            tx.commit();
            return null;
        });
    }

    @Test
    public void testPropertyExistenceConstraintOnNode() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT foobarConstraint FOR (bar:Bar) REQUIRE (bar.foobar) IS NOT NULL");
            return null;
        });

        testResult(session, "CALL apoc.schema.nodes() YIELD name, label, properties, status, type, " +
                "failure, populationProgress, size, valuesSelectivity, userDescription " +
                "WHERE label <> '<any-labels>' " +
                "RETURN *", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals("Bar", r.get("label"));
            assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("foobar"), r.get("properties"));

            assertTrue(!result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT foobarConstraint");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testConstraintExistsOnRelationship() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.commit();
            return null;
        });
        testResult(session, "RETURN apoc.schema.relationship.constraintExists('LIKED', ['day'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT likedConstraint");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testSchemaRelationships() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.relationships()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.day IS NOT NULL", r.get("name"));
            assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals("LIKED", r.get("relationshipType"));
            assertEquals(asList("day"), r.get("properties"));
            assertEquals(StringUtils.EMPTY, r.get("status"));
            assertFalse(result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT likedConstraint");
            tx.commit();
            return null;
        });
    }
    
    @Test
    public void testSchemaNodeWithRelationshipsConstraintsAndViceVersa() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT rel_cons IF NOT EXISTS FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.relationships() YIELD name, type, properties, status, relationshipType " +
                "WHERE type <> '<any-types>' " +
                "RETURN *", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.day IS NOT NULL", r.get("name"));
            assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals("LIKED", r.get("relationshipType"));
            assertEquals(asList("day"), r.get("properties"));
            assertEquals(StringUtils.EMPTY, r.get("status"));
            assertFalse(result.hasNext());
        });
        testResult(session, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("foobar"), r.get("properties"));
            assertFalse(result.hasNext());
        });
        
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT rel_cons");
            tx.run("DROP CONSTRAINT node_cons");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testConstraintsRelationshipsAndExcludeRelationshipsValuatedShouldFail() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT since_con FOR ()-[since:SINCE]-() REQUIRE since.year IS NOT NULL");
            tx.commit();
            return null;
        });

        ClientException e = Assert.assertThrows(ClientException.class,
                () ->  testResult(session, "CALL apoc.schema.relationships({relationships:['LIKED'], excludeRelationships:['SINCE']})", (result) -> {})
        );
        TestCase.assertTrue(e.getMessage().contains("Parameters relationships and excludeRelationships are both valuated. Please check parameters and valuate only one."));

        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT like_con");
            tx.run("DROP CONSTRAINT since_con");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testRelationshipKeyConstraint() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY");
            tx.commit();
            return null;
        });

        testResult(session, "CALL apoc.schema.relationships({})",
                result -> {
                    Map<String, Object> r = result.next();

                    assertEquals("CONSTRAINT FOR ()-[knows:KNOWS]-() REQUIRE (knows.day,knows.year) IS RELATIONSHIP KEY", r.get("name"));
                    assertEquals("RELATIONSHIP_KEY", r.get("type"));
                    assertEquals("KNOWS", r.get("relationshipType"));
                    assertEquals(List.of("day", "year"), r.get("properties"));

                    r = result.next();

                    assertEquals(":KNOWS(day,year)", r.get("name"));
                    assertEquals("RANGE", r.get("type"));
                    assertEquals("KNOWS", r.get("relationshipType"));
                    assertEquals(List.of("day", "year"), r.get("properties"));
                    assertFalse(result.hasNext());
                }
        );

        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT rel_con");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testNodeConstraintExists() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
            tx.commit();
            return null;
        });

        testCall(session, "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\"]) AS output;",
                (r) -> assertEquals(true, r.get("output"))
        );
    }
    @Test
    public void testNodeConstraintDoesntExist() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (foo:Foo) REQUIRE foo.barfoo IS NOT NULL");
            tx.commit();
            return null;
        });

        testCall(session, "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\", \"barfoo\"]) AS output;",
                (r) -> assertEquals(false, r.get("output"))
        );
    }

    @Test
    public void testRelationshipConstraintExists() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.commit();
            return null;
        });

        testCall(session, "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\"]) AS output;",
                (r) -> assertEquals(true, r.get("output"))
        );
    }
    @Test
    public void testRelationshipConstraintDoesntExist() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT since_con FOR ()-[since:SINCE]-() REQUIRE since.year IS NOT NULL");
            tx.commit();
            return null;
        });

        testCall(session, "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\", \"year\"]) AS output;",
                (r) -> assertEquals(false, r.get("output"))
        );
    }

    private static void assertMatchesAll( List<String> expectedCreateStatements, List<String> actualCreateStatements )
    {
        for ( String expectedCreateStatement : expectedCreateStatements )
        {
            boolean foundStatement = false;
            int foundIndex = -1;
            for ( int i = 0; i < actualCreateStatements.size(); i++ )
            {
                if ( actualCreateStatements.get( i ).contains( expectedCreateStatement ) )
                {
                    foundStatement = true;
                    foundIndex = i;
                }
            }
            assertTrue( foundStatement );
            actualCreateStatements.remove( foundIndex );
        }
    }

    private List<String> expectedKeys(String... keys){
        return asList(keys);
    }
}
