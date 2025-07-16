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
package apoc.it.core;

import static apoc.schema.SchemasTest.CALL_SCHEMA_NODES_ORDERED;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import apoc.result.AssertSchemaResult;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.Util;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
        session.executeWriteWithoutResult(tx -> {
            final List<String> constraints = tx.run("SHOW CONSTRAINTS YIELD name")
                    .list(i -> i.get("name").asString());
            constraints.forEach(name -> tx.run(String.format("DROP CONSTRAINT %s", name)));

            final List<String> indexes =
                    tx.run("SHOW INDEXES YIELD name").list(i -> i.get("name").asString());
            indexes.forEach(name -> tx.run(String.format("DROP INDEX %s", name)));
        });
    }

    @Test
    public void testAddConstraintDoesntAllowCypherInjection() {
        String query =
                "CALL apoc.schema.assert(null,{Bar:[[\"foo`) IS UNIQUE MATCH (n) DETACH DELETE n; //\", \"bar\"]]}, false)";
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

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                assertEquals(1, result.size());
                Map<String, Object> firstResult = result.get(0).asMap();

                if (cypherVersion.equals("5")) {
                    assertThat((String) firstResult.get("createStatement"))
                            .contains("CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS NODE KEY");
                } else {
                    assertThat((String) firstResult.get("createStatement"))
                            .contains("CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS KEY");
                }
                return null;
            });
        }
    }

    @Test
    public void testSchemaNodesWithNodeKey() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT node_key_movie FOR (m:Movie) REQUIRE (m.first, m.second) IS NODE KEY"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + CALL_SCHEMA_NODES_ORDERED, (result) -> {
                Map<String, Object> r = result.next();
                assertEquals("Movie", r.get("label"));
                assertEquals(List.of("first", "second"), r.get("properties"));
                assertEquals("", r.get("status"));
                assertEquals("NODE_KEY", r.get("type"));
                final String expectedUserDescConstraint =
                        "name='node_key_movie', type='NODE KEY', schema=(:Movie {first, second}), ownedIndex";
                Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescConstraint);
                if (cypherVersion.equals("5")) {
                    assertEquals(":Movie(first,second)", r.get("name"));
                } else {
                    assertEquals("node_key_movie", r.get("name"));
                }

                r = result.next();
                assertEquals("Movie", r.get("label"));
                assertEquals(List.of("first", "second"), r.get("properties"));
                assertEquals("ONLINE", r.get("status"));
                assertEquals("RANGE", r.get("type"));
                final String expectedUserDescIdx =
                        "name='node_key_movie', type='RANGE', schema=(:Movie {first, second}), indexProvider='range-1.0', owningConstraint";
                Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescIdx);

                if (cypherVersion.equals("5")) {
                    assertEquals(":Movie(first,second)", r.get("name"));
                } else {
                    assertEquals("node_key_movie", r.get("name"));
                }
                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testSchemaNodesWithNodeTypeConstraint() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT node_prop_type_movie FOR (m:Movie) REQUIRE (m.first) IS :: INTEGER"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";

            testResult(session, preparser + CALL_SCHEMA_NODES_ORDERED, (result) -> {
                Map<String, Object> r = result.next();
                assertEquals("Movie", r.get("label"));
                assertEquals(List.of("first"), r.get("properties"));
                assertEquals("", r.get("status"));
                assertEquals("NODE_PROPERTY_TYPE", r.get("type"));
                final String expectedUserDescConstraint =
                        "name='node_prop_type_movie', type='NODE PROPERTY TYPE', schema=(:Movie {first}), propertyType=INTEGER";
                Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescConstraint);

                if (cypherVersion.equals("5")) {
                    assertEquals(":Movie(first)", r.get("name"));
                } else {
                    assertEquals("node_prop_type_movie", r.get("name"));
                }

                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testNodeTypeConstraintIsKeptAndDropped() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT node_prop_type_movie_first FOR (m:Movie) REQUIRE (m.first) IS :: INTEGER");
            tx.run("CREATE CONSTRAINT node_prop_type_movie_second FOR (m:Movie) REQUIRE (m.second) IS :: INTEGER");
        });

        HashSet<AssertSchemaResult> expectedResult = new HashSet<>();
        AssertSchemaResult firstConstraint = new AssertSchemaResult("Movie", List.of("first"));
        firstConstraint.unique = false;
        expectedResult.add(firstConstraint);

        AssertSchemaResult secondConstraint = new AssertSchemaResult("Movie", List.of("second"));
        secondConstraint.unique = false;
        secondConstraint.action = "DROPPED";
        expectedResult.add(secondConstraint);

        HashSet<AssertSchemaResult> actualResult = new HashSet<>();

        testResult(session, "CALL apoc.schema.assert({},{Movie:[[\"first\"]]})", (result) -> {
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                AssertSchemaResult con = new AssertSchemaResult(r.get("label"), (List<String>) r.get("keys"));
                con.unique = (boolean) r.get("unique");
                con.action = (String) r.get("action");
                actualResult.add(con);

                assertEquals(
                        1,
                        expectedResult.stream()
                                .filter(c -> c.keys.containsAll(con.keys)
                                        && c.action.equals(con.action)
                                        && c.label.equals(con.label)
                                        && c.unique == con.unique)
                                .toList()
                                .size());
            }

            assertEquals(expectedResult.size(), actualResult.size());
        });
    }

    @Test
    public void testSchemaNodeTypeConstraintExclude() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT node_prop_type_movie_first FOR (m:Movie) REQUIRE (m.first) IS :: INTEGER"));

        testResult(
                session,
                "CALL apoc.schema.nodes({excludeLabels:['Movie']})",
                (result) -> assertFalse(result.hasNext()));
    }

    @Test
    public void testRelKeyConstraintIsKeptAndDropped() {
        session.executeWriteWithoutResult(tx -> {
            tx.run(
                    "CREATE CONSTRAINT rel_con_since FOR ()-[since:SINCE]-() REQUIRE (since.day, since.year) IS RELATIONSHIP KEY");
            tx.run(
                    "CREATE CONSTRAINT rel_con_knows FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY");
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

                assertEquals(
                        1,
                        expectedResult.stream()
                                .filter(c -> c.keys.containsAll(con.keys)
                                        && c.action.equals(con.action)
                                        && c.label.equals(con.label)
                                        && c.unique == con.unique)
                                .toList()
                                .size());
            }

            assertEquals(expectedResult.size(), actualResult.size());
        });
    }

    @Test
    public void testSchemaRelationshipsExclude() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL"));

        testResult(
                session,
                "CALL apoc.schema.relationships({excludeRelationships:['LIKED']})",
                (result) -> assertFalse(result.hasNext()));
    }

    @Test
    public void testRelationshipConstraintsArentReturnedInNodesCheck() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.run(
                    "CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY");
            tx.run("CREATE CONSTRAINT rel_con_type FOR ()-[knows:KNOWS]-() REQUIRE knows.day IS :: INTEGER");
        });

        testResult(session, "CALL apoc.schema.nodes({})", (result) -> assertFalse(result.hasNext()));
    }

    @Test
    public void testRelExistenceConstraintIsKeptAndDropped() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.run("CREATE CONSTRAINT rel_con_since FOR ()-[since:SINCE]-() REQUIRE (since.day) IS NOT NULL");
        });

        HashSet<AssertSchemaResult> expectedResult = new HashSet<>();
        AssertSchemaResult sinceConstraint = new AssertSchemaResult("SINCE", "day");
        sinceConstraint.unique = false;
        expectedResult.add(sinceConstraint);

        AssertSchemaResult likedConstraint = new AssertSchemaResult("LIKED", "day");
        likedConstraint.unique = false;
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

                assertEquals(
                        1,
                        expectedResult.stream()
                                .filter(c -> c.key.equals(con.key)
                                        && c.action.equals(con.action)
                                        && c.label.equals(con.label)
                                        && c.unique == con.unique)
                                .toList()
                                .size());
            }

            assertEquals(expectedResult.size(), actualResult.size());
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

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                assertEquals(2, result.size());
                List<String> actualDescriptions = result.stream()
                        .map(record -> (String) record.asMap().get("createStatement"))
                        .collect(Collectors.toList());

                List<String> expectedDescriptions;
                if (cypherVersion.equals("5")) {
                    expectedDescriptions = List.of(
                            "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS NODE KEY",
                            "FOR (n:`Foo`) REQUIRE (n.`bar`, n.`foo`) IS NODE KEY");
                } else {
                    expectedDescriptions = List.of(
                            "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS KEY",
                            "FOR (n:`Foo`) REQUIRE (n.`bar`, n.`foo`) IS KEY");
                }
                assertMatchesAll(expectedDescriptions, actualDescriptions);
                return null;
            });
        }
    }

    @Test
    public void testCreateUniqueAndIsNodeKeyConstraintInSameLabel() {
        testResult(
                session, "CALL apoc.schema.assert(null,{Galileo: [['newton', 'tesla'], 'curie']}, false)", (result) -> {
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

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                assertEquals(2, result.size());
                List<String> actualDescriptions = result.stream()
                        .map(record -> (String) record.asMap().get("createStatement"))
                        .collect(Collectors.toList());

                List<String> expectedDescriptions;
                if (cypherVersion.equals("5")) {
                    expectedDescriptions = List.of(
                            "FOR (n:`Galileo`) REQUIRE (n.`newton`, n.`tesla`) IS NODE KEY",
                            "FOR (n:`Galileo`) REQUIRE (n.`curie`) IS UNIQUE");
                } else {
                    expectedDescriptions = List.of(
                            "FOR (n:`Galileo`) REQUIRE (n.`newton`, n.`tesla`) IS",
                            "FOR (n:`Galileo`) REQUIRE (n.`curie`) IS UNIQUE");
                }
                assertMatchesAll(expectedDescriptions, actualDescriptions);
                return null;
            });
        }
    }

    @Test
    public void testDropNodeKeyConstraintAndCreateNodeKeyConstraintWhenUsingDropExistingOnlyIfNotExist() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.bar,f.foo) IS NODE KEY"));
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });

        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['baa','baz']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("baa", "baz"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            assertFalse(result.hasNext());
        });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                assertEquals(1, result.size());
                Map<String, Object> firstResult = result.get(0).asMap();
                if (cypherVersion.equals("5")) {
                    assertThat((String) firstResult.get("createStatement"))
                            .contains("CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`baa`, n.`baz`) IS NODE KEY");
                } else {
                    assertThat((String) firstResult.get("createStatement"))
                            .contains("CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`baa`, n.`baz`) IS KEY");
                }
                return null;
            });
        }
    }

    @Test
    public void testDropSchemaWithNodeKeyConstraintWhenUsingDropExisting() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.foo, f.bar) IS NODE KEY");
        });
        testCall(session, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyNode() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR (m:Movie) REQUIRE (m.title) IS NOT NULL"));
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Movie", r.get("label"));
            assertEquals(expectedKeys("title"), r.get("keys"));
            assertFalse((boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    public void testDropConstraintTypePropertyNode() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR (m:Movie) REQUIRE (m.title) IS :: STRING"));
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Movie", r.get("label"));
            assertEquals(expectedKeys("title"), r.get("keys"));
            assertFalse((boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyRelationship() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR ()-[acted:Acted]->() REQUIRE (acted.since) IS NOT NULL"));

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Acted", r.get("label"));
            assertEquals(expectedKeys("since"), r.get("keys"));
            assertFalse((boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    public void testDropConstraintTypePropertyRelationship() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR ()-[acted:Acted]->() REQUIRE (acted.since) IS :: DATE"));

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Acted", r.get("label"));
            assertEquals(expectedKeys("since"), r.get("keys"));
            assertFalse((boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    public void testIndexOnMultipleProperties() {
        session.executeWriteWithoutResult(tx -> tx.run("CREATE INDEX FOR (n:Foo) ON (n.bar, n.foo)"));

        String indexName = session.executeRead(tx -> {
            String name = tx.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name")
                    .single()
                    .get("name")
                    .asString();
            return name;
        });

        session.executeWriteWithoutResult(
                tx -> tx.run("CALL db.awaitIndex($indexName)", Collections.singletonMap("indexName", indexName)));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(
                    session,
                    preparser + "CALL apoc.schema.nodes() YIELD name, label, properties, status, type, "
                            + "failure, populationProgress, size, valuesSelectivity, userDescription "
                            + "WHERE label <> '<any-labels>' "
                            + "RETURN *",
                    (result) -> {
                        // Get the index info
                        Map<String, Object> r = result.next();

                        assertEquals("ONLINE", r.get("status"));
                        assertEquals("Foo", r.get("label"));
                        assertEquals("RANGE", r.get("type"));
                        assertTrue(((List<String>) r.get("properties")).contains("bar"));
                        assertTrue(((List<String>) r.get("properties")).contains("foo"));
                        if (cypherVersion.equals("5")) {
                            assertEquals(":Foo(bar,foo)", r.get("name"));
                        } else {
                            assertTrue(r.get("name").toString().startsWith("index_"));
                        }

                        assertTrue(!result.hasNext());
                    });
        }
    }

    @Test
    public void testPropertyExistenceConstraintOnNode() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT foobarConstraint FOR (bar:Bar) REQUIRE (bar.foobar) IS NOT NULL"));

        testResult(
                session,
                "CALL apoc.schema.nodes() YIELD name, label, properties, status, type, "
                        + "failure, populationProgress, size, valuesSelectivity, userDescription "
                        + "WHERE label <> '<any-labels>' "
                        + "RETURN *",
                (result) -> {
                    Map<String, Object> r = result.next();

                    assertEquals("Bar", r.get("label"));
                    assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
                    assertEquals(asList("foobar"), r.get("properties"));

                    assertTrue(!result.hasNext());
                });
    }

    @Test
    public void testConstraintExistsOnRelationship() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL"));
        testResult(session, "RETURN apoc.schema.relationship.constraintExists('LIKED', ['day'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
    }

    @Test
    public void testSchemaRelationships() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + "CALL apoc.schema.relationships()", (result) -> {
                Map<String, Object> r = result.next();
                assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
                assertEquals("LIKED", r.get("relationshipType"));
                assertEquals(asList("day"), r.get("properties"));
                assertEquals(StringUtils.EMPTY, r.get("status"));

                if (cypherVersion.equals("5")) {
                    assertEquals("CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.day IS NOT NULL", r.get("name"));
                } else {
                    assertEquals("likedConstraint", r.get("name"));
                }

                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testSchemaNodeWithRelationshipsConstraintsAndViceVersa() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT rel_cons IF NOT EXISTS FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
        });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(
                    session,
                    preparser
                            + "CALL apoc.schema.relationships() YIELD name, type, properties, status, relationshipType "
                            + "WHERE type <> '<any-types>' "
                            + "RETURN *",
                    (result) -> {
                        Map<String, Object> r = result.next();
                        assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
                        assertEquals("LIKED", r.get("relationshipType"));
                        assertEquals(asList("day"), r.get("properties"));
                        assertEquals(StringUtils.EMPTY, r.get("status"));

                        if (cypherVersion.equals("5")) {
                            assertEquals(
                                    "CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.day IS NOT NULL", r.get("name"));
                        } else {
                            assertEquals("rel_cons", r.get("name"));
                        }

                        assertFalse(result.hasNext());
                    });
            testResult(session, preparser + "CALL apoc.schema.nodes()", (result) -> {
                Map<String, Object> r = result.next();
                assertEquals("Bar", r.get("label"));
                assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
                assertEquals(asList("foobar"), r.get("properties"));
                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testConstraintsRelationshipsAndExcludeRelationshipsValuatedShouldFail() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT since_con FOR ()-[since:SINCE]-() REQUIRE since.year IS NOT NULL");
        });

        ClientException e = Assert.assertThrows(
                ClientException.class,
                () -> testResult(
                        session,
                        "CALL apoc.schema.relationships({relationships:['LIKED'], excludeRelationships:['SINCE']})",
                        (result) -> {}));
        TestCase.assertTrue(
                e.getMessage()
                        .contains(
                                "Parameters relationships and excludeRelationships are both valuated. Please check parameters and valuate only one."));
    }

    @Test
    public void testRelationshipKeyConstraint() {
        session.executeWriteWithoutResult(
                tx -> tx.run(
                        "CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + "CALL apoc.schema.relationships({})", result -> {
                Map<String, Object> r = result.next();

                assertEquals("RELATIONSHIP_KEY", r.get("type"));
                assertEquals("KNOWS", r.get("relationshipType"));
                assertEquals(List.of("day", "year"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    assertEquals(
                            "CONSTRAINT FOR ()-[knows:KNOWS]-() REQUIRE (knows.day,knows.year) IS RELATIONSHIP KEY",
                            r.get("name"));
                } else {
                    assertEquals("rel_con", r.get("name"));
                }

                r = result.next();

                assertEquals("RANGE", r.get("type"));
                assertEquals("KNOWS", r.get("relationshipType"));
                assertEquals(List.of("day", "year"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    assertEquals(":KNOWS(day,year)", r.get("name"));
                } else {
                    assertEquals("rel_con", r.get("name"));
                }
                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testRelationshipTypePropConstraint() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE knows.day IS :: INTEGER"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + "CALL apoc.schema.relationships({})", result -> {
                Map<String, Object> r = result.next();

                assertEquals("RELATIONSHIP_PROPERTY_TYPE", r.get("type"));
                assertEquals("KNOWS", r.get("relationshipType"));
                assertEquals(List.of("day"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    assertEquals("CONSTRAINT FOR ()-[knows:KNOWS]-() REQUIRE knows.day IS :: INTEGER", r.get("name"));
                } else {
                    assertEquals("rel_con", r.get("name"));
                }

                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testNodeConstraintExists() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL"));

        testCall(
                session,
                "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\"]) AS output;",
                (r) -> assertTrue((boolean) r.get("output")));
    }

    @Test
    public void testNodeTypePropConstraintExists() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS :: INTEGER"));

        testCall(
                session,
                "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\"]) AS output;",
                (r) -> assertTrue((boolean) r.get("output")));
    }

    @Test
    public void testNodeConstraintDoesntExist() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (foo:Foo) REQUIRE foo.barfoo IS NOT NULL");
        });

        testCall(
                session,
                "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\", \"barfoo\"]) AS output;",
                (r) -> assertEquals(false, r.get("output")));
    }

    @Test
    public void testRelationshipConstraintExists() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL"));

        testCall(
                session,
                "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\"]) AS output;",
                (r) -> assertEquals(true, r.get("output")));
    }

    @Test
    public void testRelationshipTypeConstraintExists() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS :: INTEGER"));

        testCall(
                session,
                "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\"]) AS output;",
                (r) -> assertEquals(true, r.get("output")));
    }

    @Test
    public void testRelationshipConstraintDoesntExist() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT since_con FOR ()-[since:SINCE]-() REQUIRE since.year IS NOT NULL");
        });

        testCall(
                session,
                "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\", \"year\"]) AS output;",
                (r) -> assertEquals(false, r.get("output")));
    }

    private static void assertMatchesAll(List<String> expectedCreateStatements, List<String> actualCreateStatements) {
        for (String expectedCreateStatement : expectedCreateStatements) {
            boolean foundStatement = false;
            int foundIndex = -1;
            for (int i = 0; i < actualCreateStatements.size(); i++) {
                if (actualCreateStatements.get(i).contains(expectedCreateStatement)) {
                    foundStatement = true;
                    foundIndex = i;
                }
            }
            assertTrue(foundStatement);
            actualCreateStatements.remove(foundIndex);
        }
    }

    private List<String> expectedKeys(String... keys) {
        return asList(keys);
    }
}
