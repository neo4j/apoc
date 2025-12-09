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

import apoc.result.AssertSchemaResult;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.Util;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;

class SchemasEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeAll
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    // coherently with SchemasTest we remove all indexes/constraints before (e.g. to get rid of lookup indexes)
    @BeforeEach
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
    void testAddConstraintDoesntAllowCypherInjection() {
        String query =
                "CALL apoc.schema.assert(null,{Bar:[[\"foo`) IS UNIQUE MATCH (n) DETACH DELETE n; //\", \"bar\"]]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Bar", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(
                    expectedKeys("foo`) IS UNIQUE MATCH (n) DETACH DELETE n; //", "bar"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));
            org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
        });
    }

    @Test
    void testKeptNodeKeyAndUniqueConstraintIfExists() {
        String query = "CALL apoc.schema.assert(null,{Foo:[['foo','bar']]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));
            org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
        });

        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("KEPT", r.get("action"));
            org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
        });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                org.junit.jupiter.api.Assertions.assertEquals(1, result.size());
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
    void testSchemaNodesWithNodeKey() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT node_key_movie FOR (m:Movie) REQUIRE (m.first, m.second) IS NODE KEY"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + CALL_SCHEMA_NODES_ORDERED, (result) -> {
                Map<String, Object> r = result.next();
                org.junit.jupiter.api.Assertions.assertEquals("Movie", r.get("label"));
                org.junit.jupiter.api.Assertions.assertEquals(List.of("first", "second"), r.get("properties"));
                org.junit.jupiter.api.Assertions.assertEquals("", r.get("status"));
                org.junit.jupiter.api.Assertions.assertEquals("NODE_KEY", r.get("type"));
                final String expectedUserDescConstraint =
                        "name='node_key_movie', type='NODE KEY', schema=(:Movie {first, second}), ownedIndex";
                Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescConstraint);
                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(":Movie(first,second)", r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("node_key_movie", r.get("name"));
                }

                r = result.next();
                org.junit.jupiter.api.Assertions.assertEquals("Movie", r.get("label"));
                org.junit.jupiter.api.Assertions.assertEquals(List.of("first", "second"), r.get("properties"));
                org.junit.jupiter.api.Assertions.assertEquals("ONLINE", r.get("status"));
                org.junit.jupiter.api.Assertions.assertEquals("RANGE", r.get("type"));
                final String expectedUserDescIdx =
                        "name='node_key_movie', type='RANGE', schema=(:Movie {first, second}), indexProvider='range-1.0', owningConstraint";
                Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescIdx);

                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(":Movie(first,second)", r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("node_key_movie", r.get("name"));
                }
                org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
            });
        }
    }

    @Test
    void testSchemaNodesWithNodeTypeConstraint() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT node_prop_type_movie FOR (m:Movie) REQUIRE (m.first) IS :: INTEGER"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";

            testResult(session, preparser + CALL_SCHEMA_NODES_ORDERED, (result) -> {
                Map<String, Object> r = result.next();
                org.junit.jupiter.api.Assertions.assertEquals("Movie", r.get("label"));
                org.junit.jupiter.api.Assertions.assertEquals(List.of("first"), r.get("properties"));
                org.junit.jupiter.api.Assertions.assertEquals("", r.get("status"));
                org.junit.jupiter.api.Assertions.assertEquals("NODE_PROPERTY_TYPE", r.get("type"));
                final String expectedUserDescConstraint =
                        "name='node_prop_type_movie', type='NODE PROPERTY TYPE', schema=(:Movie {first}), propertyType=INTEGER";
                Assertions.assertThat(r.get("userDescription").toString()).contains(expectedUserDescConstraint);

                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(":Movie(first)", r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("node_prop_type_movie", r.get("name"));
                }

                org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
            });
        }
    }

    @Test
    void testNodeTypeConstraintIsKeptAndDropped() {
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

                org.junit.jupiter.api.Assertions.assertEquals(
                        1,
                        expectedResult.stream()
                                .filter(c -> c.keys.containsAll(con.keys)
                                        && c.action.equals(con.action)
                                        && c.label.equals(con.label)
                                        && c.unique == con.unique)
                                .toList()
                                .size());
            }

            org.junit.jupiter.api.Assertions.assertEquals(expectedResult.size(), actualResult.size());
        });
    }

    @Test
    void testSchemaNodeTypeConstraintExclude() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT node_prop_type_movie_first FOR (m:Movie) REQUIRE (m.first) IS :: INTEGER"));

        testResult(
                session,
                "CALL apoc.schema.nodes({excludeLabels:['Movie']})",
                (result) -> org.junit.jupiter.api.Assertions.assertFalse(result.hasNext()));
    }

    @Test
    void testRelKeyConstraintIsKeptAndDropped() {
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

                org.junit.jupiter.api.Assertions.assertEquals(
                        1,
                        expectedResult.stream()
                                .filter(c -> c.keys.containsAll(con.keys)
                                        && c.action.equals(con.action)
                                        && c.label.equals(con.label)
                                        && c.unique == con.unique)
                                .toList()
                                .size());
            }

            org.junit.jupiter.api.Assertions.assertEquals(expectedResult.size(), actualResult.size());
        });
    }

    @Test
    void testSchemaRelationshipsExclude() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL"));

        testResult(
                session,
                "CALL apoc.schema.relationships({excludeRelationships:['LIKED']})",
                (result) -> org.junit.jupiter.api.Assertions.assertFalse(result.hasNext()));
    }

    @Test
    void testRelationshipConstraintsArentReturnedInNodesCheck() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT rel_con_liked FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.run(
                    "CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY");
            tx.run("CREATE CONSTRAINT rel_con_type FOR ()-[knows:KNOWS]-() REQUIRE knows.day IS :: INTEGER");
        });

        testResult(
                session,
                "CALL apoc.schema.nodes({})",
                (result) -> org.junit.jupiter.api.Assertions.assertFalse(result.hasNext()));
    }

    @Test
    void testRelExistenceConstraintIsKeptAndDropped() {
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

                org.junit.jupiter.api.Assertions.assertEquals(
                        1,
                        expectedResult.stream()
                                .filter(c -> c.key.equals(con.key)
                                        && c.action.equals(con.action)
                                        && c.label.equals(con.label)
                                        && c.unique == con.unique)
                                .toList()
                                .size());
            }

            org.junit.jupiter.api.Assertions.assertEquals(expectedResult.size(), actualResult.size());
        });
    }

    @Test
    void testKeptNodeKeyAndUniqueConstraintIfExistsAndDropExistingIsFalse() {
        String query = "CALL apoc.schema.assert(null,{Foo:[['foo','bar']]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));
            org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
        });
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]}, false)", (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("KEPT", r.get("action"));
            r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));
            org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
        });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                org.junit.jupiter.api.Assertions.assertEquals(2, result.size());
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
    void testCreateUniqueAndIsNodeKeyConstraintInSameLabel() {
        testResult(
                session, "CALL apoc.schema.assert(null,{Galileo: [['newton', 'tesla'], 'curie']}, false)", (result) -> {
                    Map<String, Object> r = result.next();
                    org.junit.jupiter.api.Assertions.assertEquals("Galileo", r.get("label"));
                    org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("newton", "tesla"), r.get("keys"));
                    org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
                    org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));
                    r = result.next();
                    org.junit.jupiter.api.Assertions.assertEquals("Galileo", r.get("label"));
                    org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("curie"), r.get("keys"));
                    org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
                    org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));
                    org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
                });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                org.junit.jupiter.api.Assertions.assertEquals(2, result.size());
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
    void testDropNodeKeyConstraintAndCreateNodeKeyConstraintWhenUsingDropExistingOnlyIfNotExist() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.bar,f.foo) IS NODE KEY"));
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]})", (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("KEPT", r.get("action"));
        });

        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['baa','baz']]})", (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("DROPPED", r.get("action"));

            r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("baa", "baz"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("CREATED", r.get("action"));

            org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
        });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            session.executeRead(tx -> {
                List<Record> result = tx.run(preparser + "SHOW CONSTRAINTS YIELD createStatement")
                        .list();
                org.junit.jupiter.api.Assertions.assertEquals(1, result.size());
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
    void testDropSchemaWithNodeKeyConstraintWhenUsingDropExisting() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.foo, f.bar) IS NODE KEY");
        });
        testCall(session, "CALL apoc.schema.assert(null,null)", (r) -> {
            org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertEquals(true, r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            org.junit.jupiter.api.Assertions.assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    void testDropConstraintExistsPropertyNode() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR (m:Movie) REQUIRE (m.title) IS NOT NULL"));
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            org.junit.jupiter.api.Assertions.assertEquals("Movie", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("title"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertFalse((boolean) r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            org.junit.jupiter.api.Assertions.assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    void testDropConstraintTypePropertyNode() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR (m:Movie) REQUIRE (m.title) IS :: STRING"));
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            org.junit.jupiter.api.Assertions.assertEquals("Movie", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("title"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertFalse((boolean) r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            org.junit.jupiter.api.Assertions.assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    void testDropConstraintExistsPropertyRelationship() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR ()-[acted:Acted]->() REQUIRE (acted.since) IS NOT NULL"));

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            org.junit.jupiter.api.Assertions.assertEquals("Acted", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("since"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertFalse((boolean) r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            org.junit.jupiter.api.Assertions.assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    void testDropConstraintTypePropertyRelationship() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT FOR ()-[acted:Acted]->() REQUIRE (acted.since) IS :: DATE"));

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            org.junit.jupiter.api.Assertions.assertEquals("Acted", r.get("label"));
            org.junit.jupiter.api.Assertions.assertEquals(expectedKeys("since"), r.get("keys"));
            org.junit.jupiter.api.Assertions.assertFalse((boolean) r.get("unique"));
            org.junit.jupiter.api.Assertions.assertEquals("DROPPED", r.get("action"));
        });

        session.executeRead(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            org.junit.jupiter.api.Assertions.assertEquals(0, result.size());
            return null;
        });
    }

    @Test
    void testIndexOnMultipleProperties() {
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

                        org.junit.jupiter.api.Assertions.assertEquals("ONLINE", r.get("status"));
                        org.junit.jupiter.api.Assertions.assertEquals("Foo", r.get("label"));
                        org.junit.jupiter.api.Assertions.assertEquals("RANGE", r.get("type"));
                        org.junit.jupiter.api.Assertions.assertTrue(
                                ((List<String>) r.get("properties")).contains("bar"));
                        org.junit.jupiter.api.Assertions.assertTrue(
                                ((List<String>) r.get("properties")).contains("foo"));
                        if (cypherVersion.equals("5")) {
                            org.junit.jupiter.api.Assertions.assertEquals(":Foo(bar,foo)", r.get("name"));
                        } else {
                            org.junit.jupiter.api.Assertions.assertTrue(
                                    r.get("name").toString().startsWith("index_"));
                        }

                        org.junit.jupiter.api.Assertions.assertTrue(!result.hasNext());
                    });
        }
    }

    @Test
    void testPropertyExistenceConstraintOnNode() {
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

                    org.junit.jupiter.api.Assertions.assertEquals("Bar", r.get("label"));
                    org.junit.jupiter.api.Assertions.assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
                    org.junit.jupiter.api.Assertions.assertEquals(asList("foobar"), r.get("properties"));

                    org.junit.jupiter.api.Assertions.assertTrue(!result.hasNext());
                });
    }

    @Test
    void testConstraintExistsOnRelationship() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL"));
        testResult(session, "RETURN apoc.schema.relationship.constraintExists('LIKED', ['day'])", (result) -> {
            Map<String, Object> r = result.next();
            org.junit.jupiter.api.Assertions.assertEquals(
                    true, r.entrySet().iterator().next().getValue());
        });
    }

    @Test
    void testSchemaRelationships() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + "CALL apoc.schema.relationships()", (result) -> {
                Map<String, Object> r = result.next();
                org.junit.jupiter.api.Assertions.assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
                org.junit.jupiter.api.Assertions.assertEquals("LIKED", r.get("relationshipType"));
                org.junit.jupiter.api.Assertions.assertEquals(asList("day"), r.get("properties"));
                org.junit.jupiter.api.Assertions.assertEquals(StringUtils.EMPTY, r.get("status"));

                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(
                            "CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.day IS NOT NULL", r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("likedConstraint", r.get("name"));
                }

                org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
            });
        }
    }

    @Test
    void testSchemaNodeWithRelationshipsConstraintsAndViceVersa() {
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
                        org.junit.jupiter.api.Assertions.assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
                        org.junit.jupiter.api.Assertions.assertEquals("LIKED", r.get("relationshipType"));
                        org.junit.jupiter.api.Assertions.assertEquals(asList("day"), r.get("properties"));
                        org.junit.jupiter.api.Assertions.assertEquals(StringUtils.EMPTY, r.get("status"));

                        if (cypherVersion.equals("5")) {
                            org.junit.jupiter.api.Assertions.assertEquals(
                                    "CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.day IS NOT NULL", r.get("name"));
                        } else {
                            org.junit.jupiter.api.Assertions.assertEquals("rel_cons", r.get("name"));
                        }

                        org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
                    });
            testResult(session, preparser + "CALL apoc.schema.nodes()", (result) -> {
                Map<String, Object> r = result.next();
                org.junit.jupiter.api.Assertions.assertEquals("Bar", r.get("label"));
                org.junit.jupiter.api.Assertions.assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
                org.junit.jupiter.api.Assertions.assertEquals(asList("foobar"), r.get("properties"));
                org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
            });
        }
    }

    @Test
    void testConstraintsRelationshipsAndExcludeRelationshipsValuatedShouldFail() {
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
        org.junit.jupiter.api.Assertions.assertTrue(
                e.getMessage()
                        .contains(
                                "Parameters relationships and excludeRelationships are both valuated. Please check parameters and valuate only one."));
    }

    @Test
    void testRelationshipKeyConstraint() {
        session.executeWriteWithoutResult(
                tx -> tx.run(
                        "CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE (knows.day, knows.year) IS RELATIONSHIP KEY"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + "CALL apoc.schema.relationships({})", result -> {
                Map<String, Object> r = result.next();

                org.junit.jupiter.api.Assertions.assertEquals("RELATIONSHIP_KEY", r.get("type"));
                org.junit.jupiter.api.Assertions.assertEquals("KNOWS", r.get("relationshipType"));
                org.junit.jupiter.api.Assertions.assertEquals(List.of("day", "year"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(
                            "CONSTRAINT FOR ()-[knows:KNOWS]-() REQUIRE (knows.day,knows.year) IS RELATIONSHIP KEY",
                            r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("rel_con", r.get("name"));
                }

                r = result.next();

                org.junit.jupiter.api.Assertions.assertEquals("RANGE", r.get("type"));
                org.junit.jupiter.api.Assertions.assertEquals("KNOWS", r.get("relationshipType"));
                org.junit.jupiter.api.Assertions.assertEquals(List.of("day", "year"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(":KNOWS(day,year)", r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("rel_con", r.get("name"));
                }
                org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
            });
        }
    }

    @Test
    void testRelationshipTypePropConstraint() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT rel_con FOR ()-[knows:KNOWS]-() REQUIRE knows.day IS :: INTEGER"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(session, preparser + "CALL apoc.schema.relationships({})", result -> {
                Map<String, Object> r = result.next();

                org.junit.jupiter.api.Assertions.assertEquals("RELATIONSHIP_PROPERTY_TYPE", r.get("type"));
                org.junit.jupiter.api.Assertions.assertEquals("KNOWS", r.get("relationshipType"));
                org.junit.jupiter.api.Assertions.assertEquals(List.of("day"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    org.junit.jupiter.api.Assertions.assertEquals(
                            "CONSTRAINT FOR ()-[knows:KNOWS]-() REQUIRE knows.day IS :: INTEGER", r.get("name"));
                } else {
                    org.junit.jupiter.api.Assertions.assertEquals("rel_con", r.get("name"));
                }

                org.junit.jupiter.api.Assertions.assertFalse(result.hasNext());
            });
        }
    }

    @Test
    void testNodeConstraintExists() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL"));

        testCall(
                session,
                "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\"]) AS output;",
                (r) -> org.junit.jupiter.api.Assertions.assertTrue((boolean) r.get("output")));
    }

    @Test
    void testNodeTypePropConstraintExists() {
        session.executeWriteWithoutResult(tx ->
                tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS :: INTEGER"));

        testCall(
                session,
                "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\"]) AS output;",
                (r) -> org.junit.jupiter.api.Assertions.assertTrue((boolean) r.get("output")));
    }

    @Test
    void testNodeConstraintDoesntExist() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
            tx.run("CREATE CONSTRAINT node_cons IF NOT EXISTS FOR (foo:Foo) REQUIRE foo.barfoo IS NOT NULL");
        });

        testCall(
                session,
                "RETURN apoc.schema.node.constraintExists(\"Bar\", [\"foobar\", \"barfoo\"]) AS output;",
                (r) -> org.junit.jupiter.api.Assertions.assertEquals(false, r.get("output")));
    }

    @Test
    void testRelationshipConstraintExists() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL"));

        testCall(
                session,
                "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\"]) AS output;",
                (r) -> org.junit.jupiter.api.Assertions.assertEquals(true, r.get("output")));
    }

    @Test
    void testRelationshipTypeConstraintExists() {
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS :: INTEGER"));

        testCall(
                session,
                "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\"]) AS output;",
                (r) -> org.junit.jupiter.api.Assertions.assertEquals(true, r.get("output")));
    }

    @Test
    void testRelationshipConstraintDoesntExist() {
        session.executeWriteWithoutResult(tx -> {
            tx.run("CREATE CONSTRAINT like_con FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.run("CREATE CONSTRAINT since_con FOR ()-[since:SINCE]-() REQUIRE since.year IS NOT NULL");
        });

        testCall(
                session,
                "RETURN apoc.schema.relationship.constraintExists(\"LIKED\", [\"day\", \"year\"]) AS output;",
                (r) -> org.junit.jupiter.api.Assertions.assertEquals(false, r.get("output")));
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
            org.junit.jupiter.api.Assertions.assertTrue(foundStatement);
            actualCreateStatements.remove(foundIndex);
        }
    }

    private List<String> expectedKeys(String... keys) {
        return asList(keys);
    }
}
