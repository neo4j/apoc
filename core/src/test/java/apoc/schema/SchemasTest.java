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
package apoc.schema;

import static apoc.util.TestUtil.singleResultFirstColumn;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

import apoc.result.AssertSchemaResult;
import apoc.result.IndexConstraintRelationshipInfo;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
public class SchemasTest {
    public static final String CALL_SCHEMA_NODES_ORDERED = "CALL apoc.schema.nodes() "
            + "YIELD label, type, properties, status, userDescription, name " + "RETURN * ORDER BY label, type";

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(
                GraphDatabaseSettings.procedure_unrestricted, List.of("apoc.schema.nodes", "apoc.schema.relationship"));
    }

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, Schemas.class, SchemaRestricted.class);
    }

    @BeforeEach
    void dropSchema() {
        try (Transaction tx = db.beginTx()) {
            Schema schema = tx.schema();
            schema.getConstraints().forEach(ConstraintDefinition::drop);
            schema.getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @Test
    public void testCreateIndex() {
        testCall(db, "CALL apoc.schema.assert({Foo:['bar']},null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes(Label.label("Foo")));
            assertEquals(1, indexes.size());
            assertEquals("Foo", Iterables.single(indexes.get(0).getLabels()).name());
            assertEquals(asList("bar"), indexes.get(0).getPropertyKeys());
        }
    }

    @Test
    public void testCreateSchema() {
        testCall(db, "CALL apoc.schema.assert(null,{Foo:['bar']})", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints(Label.label("Foo")));
            assertEquals(1, constraints.size());
            ConstraintDefinition constraint = constraints.get(0);
            assertEquals(ConstraintType.UNIQUENESS, constraint.getConstraintType());
            assertEquals("Foo", constraint.getLabel().name());
            assertEquals("bar", Iterables.single(constraint.getPropertyKeys()));
        }
    }

    @Test
    public void testDropIndexWhenUsingDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX titlesAndDescriptions FOR (n:Movie|Book) ON EACH [n.title, n.description]");
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            // the multi-token idx remains
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testDropIndexDoesntAffectVectorIndexes() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally(
                """
                CREATE VECTOR INDEX moviePlots IF NOT EXISTS
                FOR (m:Movie)
                ON m.embedding
                OPTIONS { indexConfig: {
                 `vector.dimensions`: 1536,
                 `vector.similarity_function`: 'cosine'
                }}
                """);
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            // the multi-token idx remains
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testDropIndexAndCreateIndexWhenUsingDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        testResult(db, "CALL apoc.schema.assert({Bar:['foo']},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testRetainIndexWhenNotUsingDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        testResult(db, "CALL apoc.schema.assert({Bar:['foo', 'bar']}, null, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(3, indexes.size());
        }
    }

    @Test
    public void testDropSchemaWhenUsingDropExisting() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }

    @Test
    public void testDropSchemaAndCreateSchemaWhenUsingDropExisting() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testResult(db, "CALL apoc.schema.assert(null, {Bar:['foo']})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
        }
    }

    @Test
    public void testRetainSchemaWhenNotUsingDropExisting() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testResult(db, "CALL apoc.schema.assert(null, {Bar:['foo', 'bar']}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints());
            assertEquals(3, constraints.size());
        }
    }

    @Test
    public void testKeepIndex() {
        keepIndexCommon(false);
    }

    @Test
    public void testKeepIndexWithDropExisting() {
        keepIndexCommon(true);
    }

    private void keepIndexCommon(boolean dropExisting) {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        testResult(
                db,
                "CALL apoc.schema.assert({Foo:['bar', 'foo']}, null, $drop)",
                Map.of("drop", dropExisting),
                (result) -> {
                    Map<String, Object> r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals("bar", r.get("key"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("KEPT", r.get("action"));

                    r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals("foo", r.get("key"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("CREATED", r.get("action"));
                });

        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(2, indexes.size());
        }
    }

    @Test
    public void testKeepSchema() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testResult(db, "CALL apoc.schema.assert(null,{Foo:['bar', 'foo']})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(expectedKeys("bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints());
            assertEquals(2, constraints.size());
        }
    }

    @Test
    public void testIndexes() {
        db.executeTransactionally("CREATE RANGE INDEX index1 FOR (n:Foo) ON (n.bar)");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(db, preparser + "CALL apoc.schema.nodes()", (result) -> {
                // Get the index info
                Map<String, Object> r = result.next();

                assertEquals("ONLINE", r.get("status"));
                assertEquals("Foo", r.get("label"));
                assertEquals("RANGE", r.get("type"));
                assertEquals("bar", ((List<String>) r.get("properties")).get(0));
                assertEquals("NO FAILURE", r.get("failure"));
                assertEquals(100d, r.get("populationProgress"));
                assertEquals(1d, r.get("valuesSelectivity"));
                assertThat(r.get("userDescription"))
                        .asString()
                        .contains("name='index1', type='RANGE', schema=(:Foo {bar}), indexProvider='range-1.0' )");

                if (cypherVersion.equals("5")) {
                    assertEquals(":Foo(bar)", r.get("name"));
                } else {
                    assertEquals("index1", r.get("name"));
                }

                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testRelIndex() {
        db.executeTransactionally("CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.id, r.since)");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testCall(db, preparser + "CALL apoc.schema.relationships()", row -> {
                assertEquals("ONLINE", row.get("status"));
                assertEquals("KNOWS", row.get("relationshipType"));
                assertEquals("RANGE", row.get("type"));
                assertEquals(List.of("id", "since"), row.get("properties"));

                if (cypherVersion.equals("5")) {
                    assertEquals(":KNOWS(id,since)", row.get("name"));
                } else {
                    assertTrue(row.get("name").toString().startsWith("index_"));
                }
            });
        }
    }

    @Test
    public void testIndexExists() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        testResult(db, "RETURN apoc.schema.node.indexExists('Foo', ['bar'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
    }

    @Test
    public void testIndexNotExists() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        testResult(db, "RETURN apoc.schema.node.indexExists('Bar', ['foo'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(false, r.entrySet().iterator().next().getValue());
        });
    }

    @Test
    public void testRelationshipExists() {
        db.executeTransactionally("CREATE INDEX rel_index_simple FOR ()-[r:KNOWS]-() ON (r.since)");
        db.executeTransactionally("CREATE INDEX rel_index_composite FOR ()-[r:PURCHASED]-() ON (r.date, r.amount)");
        awaitIndexesOnline();

        assertTrue((Boolean)
                singleResultFirstColumn(db, "RETURN apoc.schema.relationship.indexExists('KNOWS', ['since'])"));
        assertFalse((Boolean)
                singleResultFirstColumn(db, "RETURN apoc.schema.relationship.indexExists('KNOWS', ['dunno'])"));
        // - composite index
        assertTrue((Boolean) singleResultFirstColumn(
                db, "RETURN apoc.schema.relationship.indexExists('PURCHASED', ['date', 'amount'])"));
        assertFalse((Boolean) singleResultFirstColumn(
                db, "RETURN apoc.schema.relationship.indexExists('PURCHASED', ['date', 'another'])"));
    }

    @Test
    void testUniquenessConstraintOnNode() {
        db.executeTransactionally("CREATE CONSTRAINT constraintName FOR (bar:Bar) REQUIRE bar.foo IS UNIQUE");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(db, preparser + CALL_SCHEMA_NODES_ORDERED, (result) -> {
                Map<String, Object> r = result.next();

                assertEquals("Bar", r.get("label"));
                assertEquals(List.of("foo"), r.get("properties"));
                assertEquals("RANGE", r.get("type"));
                assertEquals("ONLINE", r.get("status"));
                final String expectedUserDescBarIdx =
                        "name='constraintName', type='RANGE', schema=(:Bar {foo}), indexProvider='range-1.0', owningConstraint";
                assertThat(r.get("userDescription")).asString().contains(expectedUserDescBarIdx);

                if (cypherVersion.equals("5")) {
                    assertEquals(":Bar(foo)", r.get("name"));
                } else {
                    assertEquals("constraintName", r.get("name"));
                }

                r = result.next();

                assertEquals("Bar", r.get("label"));
                assertEquals(List.of("foo"), r.get("properties"));
                assertEquals("UNIQUENESS", r.get("type"));
                assertEquals("", r.get("status"));
                if (cypherVersion.equals("5")) {
                    assertEquals(":Bar(foo)", r.get("name"));
                    final String expectedUserDescBarCons =
                            "name='constraintName', type='UNIQUENESS', schema=(:Bar {foo}), ownedIndex=2";
                    assertThat(r.get("userDescription").toString()).contains(expectedUserDescBarCons);

                } else {
                    assertEquals("constraintName", r.get("name"));
                    final String expectedUserDescBarCons =
                            "name='constraintName', type='NODE PROPERTY UNIQUENESS', schema=(:Bar {foo}), ownedIndex=2";
                    assertThat(r.get("userDescription").toString()).contains(expectedUserDescBarCons);
                }

                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testIndexAndUniquenessConstraintOnNode() {
        db.executeTransactionally("CREATE INDEX foo_idx FOR (n:Foo) ON (n.foo)");
        db.executeTransactionally("CREATE CONSTRAINT bar_unique FOR (bar:Bar) REQUIRE bar.bar IS UNIQUE");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(db, preparser + CALL_SCHEMA_NODES_ORDERED, (result) -> {
                Map<String, Object> r = result.next();

                assertEquals("Bar", r.get("label"));
                assertEquals(List.of("bar"), r.get("properties"));
                assertEquals("RANGE", r.get("type"));
                assertEquals("ONLINE", r.get("status"));

                if (cypherVersion.equals("5")) {
                    assertEquals(":Bar(bar)", r.get("name"));
                } else {
                    assertEquals("bar_unique", r.get("name"));
                }

                r = result.next();

                assertEquals("Bar", r.get("label"));
                assertEquals(List.of("bar"), r.get("properties"));
                assertEquals("UNIQUENESS", r.get("type"));
                assertEquals("", r.get("status"));

                if (cypherVersion.equals("5")) {
                    assertEquals(":Bar(bar)", r.get("name"));
                    final String expectedUserDescBarCons =
                            "name='bar_unique', type='UNIQUENESS', schema=(:Bar {bar}), ownedIndex";
                    assertThat(r.get("userDescription").toString()).contains(expectedUserDescBarCons);
                } else {
                    assertEquals("bar_unique", r.get("name"));
                    final String expectedUserDescBarCons =
                            "name='bar_unique', type='NODE PROPERTY UNIQUENESS', schema=(:Bar {bar}), ownedIndex";
                    assertThat(r.get("userDescription").toString()).contains(expectedUserDescBarCons);
                }

                r = result.next();
                assertEquals("Foo", r.get("label"));
                assertEquals("RANGE", r.get("type"));
                assertEquals("foo", ((List<String>) r.get("properties")).get(0));
                assertEquals("ONLINE", r.get("status"));
                final String expectedUserDescFoo =
                        "name='foo_idx', type='RANGE', schema=(:Foo {foo}), indexProvider='range-1.0' )";
                assertThat(r.get("userDescription").toString()).contains(expectedUserDescFoo);

                if (cypherVersion.equals("5")) {
                    assertEquals(":Foo(foo)", r.get("name"));
                } else {
                    assertEquals("foo_idx", r.get("name"));
                }

                assertFalse(result.hasNext());
            });
        }
    }

    @Test
    public void testSchemaNodesPointIndex() {
        db.executeTransactionally("CREATE POINT INDEX pointIdx FOR (n:Baz) ON (n.baz)");

        testResult(db, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals("Baz", r.get("label"));
            assertEquals("POINT", r.get("type"));
            assertEquals("baz", ((List<String>) r.get("properties")).get(0));
            assertEquals("ONLINE", r.get("status"));
            final String expectedUserDesc =
                    "name='pointIdx', type='POINT', schema=(:Baz {baz}), indexProvider='point-1.0'";
            assertThat(r.get("userDescription").toString()).contains(expectedUserDesc);

            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testRelUniquenessConstraintIsKeptAndDropped() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[knows:KNOWS]-() REQUIRE knows.since IS UNIQUE");
        awaitIndexesOnline();

        ArrayList<AssertSchemaResult> schemaResult = new ArrayList<>();

        AssertSchemaResult since = new AssertSchemaResult("SINCE", "year");
        since.unique = true;
        schemaResult.add(since);

        AssertSchemaResult knows = new AssertSchemaResult("KNOWS", "since");
        knows.unique = true;
        knows.action = "DROPPED";
        schemaResult.add(knows);

        testResult(db, "CALL apoc.schema.assert({},{SINCE:[\"year\"]})", (result) -> {
            while (result.hasNext()) {
                Map<String, Object> r = result.next();

                assertEquals(
                        1,
                        schemaResult.stream()
                                .filter(c -> c.label.equals(r.get("label"))
                                        && c.keys.containsAll((List<String>) r.get("keys"))
                                        && c.key.equals(r.get("key"))
                                        && c.action.equals(r.get("action"))
                                        && c.unique == (boolean) r.get("unique"))
                                .toList()
                                .size());
            }
        });
    }

    @Test
    public void testDropCompoundIndexWhenUsingDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar,n.baa)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert(null,null,true)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(0, indexes.size());
        }
    }

    @Test
    public void testDropCompoundIndexAndRecreateWithDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar,n.baa)");
        awaitIndexesOnline();
        testCall(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null,true)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testDoesntDropCompoundIndexWhenSupplyingSameCompoundIndex() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar,n.baa)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null,false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testCompoundIndexDoesntAllowCypherInjection() {
        awaitIndexesOnline();
        testResult(
                db,
                "CALL apoc.schema.assert({Foo:[['bar`) MATCH (n) DETACH DELETE n; //','baa']]},null,false)",
                (result) -> {
                    Map<String, Object> r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals(expectedKeys("bar`) MATCH (n) DETACH DELETE n; //", "baa"), r.get("keys"));
                    assertEquals(false, r.get("unique"));
                });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testKeepCompoundIndex() {
        testKeepCompoundCommon(false);
    }

    @Test
    public void testKeepCompoundIndexWithDropExisting() {
        testKeepCompoundCommon(true);
    }

    private void testKeepCompoundCommon(boolean dropExisting) {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar,n.baa)");
        awaitIndexesOnline();
        testResult(
                db,
                "CALL apoc.schema.assert({Foo:[['bar','baa'], ['foo','faa']]},null,$drop)",
                Map.of("drop", dropExisting),
                (result) -> {
                    Map<String, Object> r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("KEPT", r.get("action"));

                    r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals(expectedKeys("foo", "faa"), r.get("keys"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("CREATED", r.get("action"));
                });

        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(2, indexes.size());
        }
    }

    @Test
    public void testDropIndexAndCreateCompoundIndexWhenUsingDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Bar:[['foo','bar']]},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testAssertWithFullTextIndexes() {
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX fullIdxNode FOR (n:Moon|Blah) ON EACH [n.weightProp, n.anotherProp]");
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX fullIdxRel FOR ()-[r:TYPE_1|TYPE_2]->() ON EACH [r.alpha, r.beta]");
        // fulltext with single label, should return label field as string
        db.executeTransactionally("CREATE FULLTEXT INDEX fullIdxNodeSingle FOR (n:Asd) ON EACH [n.uno, n.due]");
        awaitIndexesOnline();
        testResult(
                db,
                "CALL apoc.schema.assert({Bar:[['foo','bar']]}, {One:['two']}) "
                        + "YIELD label, key, keys, unique, action RETURN * ORDER BY label",
                (result) -> {
                    Map<String, Object> r = result.next();
                    assertEquals("Asd", r.get("label"));
                    assertEquals(expectedKeys("uno", "due"), r.get("keys"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("DROPPED", r.get("action"));

                    r = result.next();
                    assertEquals("Bar", r.get("label"));
                    assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("CREATED", r.get("action"));

                    r = result.next();
                    assertEquals("One", r.get("label"));
                    assertEquals("two", r.get("key"));
                    assertEquals(true, r.get("unique"));
                    assertEquals("CREATED", r.get("action"));
                    assertFalse(result.hasNext());
                });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(4, indexes.size());
            List<ConstraintDefinition> constraints =
                    Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
        }
    }

    @Test
    public void testDropCompoundIndexAndCreateCompoundIndexWhenUsingDropExisting() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar,n.baa)");
        awaitIndexesOnline();
        testCall(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    private List<String> expectedKeys(String... keys) {
        return asList(keys);
    }

    @Test
    public void testIndexesOneLabel() {
        db.executeTransactionally("CREATE RANGE INDEX index1 FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally("CREATE RANGE INDEX index2 FOR (n:Bar) ON (n.foo)");
        db.executeTransactionally("CREATE TEXT INDEX index3 FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE TEXT INDEX index4 FOR (n:Movie) ON (n.title)");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(
                    db,
                    preparser + "CALL apoc.schema.nodes({labels:['Foo']})", // Get the index info
                    (result -> {
                        Map<String, Object> r = result.next();

                        assertEquals("ONLINE", r.get("status"));
                        assertEquals("Foo", r.get("label"));
                        assertEquals("RANGE", r.get("type"));
                        assertEquals("bar", ((List<String>) r.get("properties")).get(0));
                        assertEquals("NO FAILURE", r.get("failure"));
                        assertEquals(100d, r.get("populationProgress"));
                        assertEquals(1d, r.get("valuesSelectivity"));
                        assertThat(r.get("userDescription").toString())
                                .contains(
                                        "name='index1', type='RANGE', schema=(:Foo {bar}), indexProvider='range-1.0' )");

                        if (cypherVersion.equals("5")) {
                            assertEquals(":Foo(bar)", r.get("name"));
                        } else {
                            assertEquals("index1", r.get("name"));
                        }
                        assertTrue(!result.hasNext());
                    }));
        }
    }

    private void awaitIndexesOnline() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.commit();
        }
    }

    @Test
    public void testIndexesMoreLabels() {
        db.executeTransactionally("CREATE RANGE INDEX index1 FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally("CREATE RANGE INDEX index2 FOR (n:Bar) ON (n.foo)");
        db.executeTransactionally("CREATE TEXT INDEX index3 FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE TEXT INDEX index4 FOR (n:Movie) ON (n.title)");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(
                    db,
                    preparser + "CALL apoc.schema.nodes({labels:['Foo', 'Person']})", // Get the index info
                    (result) -> {
                        Map<String, Object> r = result.next();

                        assertEquals("ONLINE", r.get("status"));
                        assertEquals("Foo", r.get("label"));
                        assertEquals("RANGE", r.get("type"));
                        assertEquals("bar", ((List<String>) r.get("properties")).get(0));
                        assertEquals("NO FAILURE", r.get("failure"));
                        assertEquals(100d, r.get("populationProgress"));
                        assertEquals(1d, r.get("valuesSelectivity"));
                        assertThat(r.get("userDescription").toString())
                                .contains(
                                        "name='index1', type='RANGE', schema=(:Foo {bar}), indexProvider='range-1.0' )");

                        if (cypherVersion.equals("5")) {
                            assertEquals(":Foo(bar)", r.get("name"));
                        } else {
                            assertEquals("index1", r.get("name"));
                        }

                        r = result.next();

                        assertEquals("ONLINE", r.get("status"));
                        assertEquals("Person", r.get("label"));
                        assertEquals("TEXT", r.get("type"));
                        assertEquals("name", ((List<String>) r.get("properties")).get(0));
                        assertEquals("NO FAILURE", r.get("failure"));
                        assertEquals(100d, r.get("populationProgress"));
                        assertEquals(1d, r.get("valuesSelectivity"));
                        assertThat(r.get("userDescription").toString())
                                .contains(
                                        "name='index3', type='TEXT', schema=(:Person {name}), indexProvider='text-3.0' )");

                        if (cypherVersion.equals("5")) {
                            assertEquals(":Person(name)", r.get("name"));
                        } else {
                            assertEquals("index3", r.get("name"));
                        }

                        assertTrue(!result.hasNext());
                    });
        }
    }

    @Test
    public void testSchemaNodesExclude() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (book:Book) REQUIRE book.isbn IS UNIQUE");
        testResult(db, "CALL apoc.schema.nodes({excludeLabels:['Book']})", (result) -> assertFalse(result.hasNext()));
    }

    @Test
    public void testIndexesLabelsAndExcludeLabelsValuatedShouldFail() {
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally("CREATE INDEX FOR (n:Bar) ON (n.foo)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE INDEX FOR (n:Movie) ON (n.title)");
        awaitIndexesOnline();

        assertThatThrownBy(() -> db.executeTransactionally(
                        "CALL apoc.schema.nodes({labels:['Foo', 'Person', 'Bar'], excludeLabels:['Bar']})"))
                .isInstanceOf(QueryExecutionException.class)
                .hasMessageContaining(
                        "Parameters labels and excludeLabels are both valuated. Please check parameters and valuate only one.");
    }

    @Test
    public void testRelationshipConstraintsArentReturnedInNodesCheck() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.day IS UNIQUE");
        awaitIndexesOnline();

        testResult(db, "CALL apoc.schema.nodes({})", (result) -> assertFalse(result.hasNext()));
    }

    @Test
    public void testConstraintsRelationshipsAndExcludeRelationshipsValuatedShouldFail() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.day IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");

        assertThatThrownBy(() -> db.executeTransactionally(
                        "CALL apoc.schema.relationships({relationships:['LIKED'], excludeRelationships:['SINCE']})"))
                .isInstanceOf(QueryExecutionException.class)
                .hasMessageContaining(
                        "Parameters relationships and excludeRelationships are both valuated. Please check parameters and valuate only one.");
    }

    @Test
    public void testUniqueRelationshipConstraint() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(db, preparser + "CALL apoc.schema.relationships({})", result -> {
                Map<String, Object> r = result.next();
                assertEquals("RELATIONSHIP_UNIQUENESS", r.get("type"));
                assertEquals("SINCE", r.get("relationshipType"));
                assertEquals("year", ((List<String>) r.get("properties")).get(0));

                if (cypherVersion.equals("5")) {
                    assertEquals("CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE", r.get("name"));
                } else {
                    assertTrue(r.get("name").toString().startsWith("constraint_"));
                }

                r = result.next();

                assertEquals("RANGE", r.get("type"));
                assertEquals("SINCE", r.get("relationshipType"));
                assertEquals("year", ((List<String>) r.get("properties")).get(0));

                if (cypherVersion.equals("5")) {
                    assertEquals(":SINCE(year)", r.get("name"));
                } else {
                    assertTrue(r.get("name").toString().startsWith("constraint_"));
                }

                assertTrue(!result.hasNext());
            });
        }
    }

    @Test
    public void testCypher5Vs25RelNaming() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT gemTest FOR ()-[for:FOR]-() REQUIRE for.year IS UNIQUE");
        db.executeTransactionally("CREATE LOOKUP INDEX rel_type_lookup_index FOR ()-[r]-() ON EACH type(r)");
        awaitIndexesOnline();

        var cypher5Names = List.of(
                "CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE",
                "CONSTRAINT FOR ()-[for:FOR]-() REQUIRE for.year IS UNIQUE",
                ":FOR(year)",
                ":SINCE(year)",
                ":<any-types>()");

        var cypher25Names = List.of("gemTest", "gemTest", "rel_type_lookup_index");

        testResult(db, "CYPHER 5 CALL apoc.schema.relationships({})", result -> {
            List<String> producedNames = new ArrayList<>();
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                producedNames.add((String) r.get("name"));
            }

            assertTrue(producedNames.containsAll(cypher5Names));
            assertEquals(5, producedNames.size());
        });

        testResult(db, "CYPHER 25 CALL apoc.schema.relationships({})", result -> {
            List<String> producedNames = new ArrayList<>();
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                producedNames.add((String) r.get("name"));
            }

            assertTrue(producedNames.containsAll(cypher25Names));
            assertEquals(
                    2,
                    producedNames.stream()
                            .filter(c -> c.startsWith("constraint_"))
                            .toList()
                            .size());
            assertEquals(5, producedNames.size());
        });
    }

    @Test
    public void testCypher5Vs25NodeNaming() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (book:Book) REQUIRE book.isbn IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT gemTest FOR (library:Library) REQUIRE library.name IS UNIQUE");
        db.executeTransactionally("CREATE LOOKUP INDEX node_label_lookup_index FOR (n) ON EACH labels(n)");
        awaitIndexesOnline();

        var cypher5Names = List.of(":Book(isbn)", ":Library(name)", ":Book(isbn)", ":Library(name)", ":<any-labels>()");

        var cypher25Names = List.of("gemTest", "gemTest", "node_label_lookup_index");

        testResult(db, "CYPHER 5 CALL apoc.schema.nodes({})", result -> {
            List<String> producedNames = new ArrayList<>();
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                producedNames.add((String) r.get("name"));
            }

            assertTrue(producedNames.containsAll(cypher5Names));
            assertEquals(5, producedNames.size());
        });

        testResult(db, "CYPHER 25 CALL apoc.schema.nodes({})", result -> {
            List<String> producedNames = new ArrayList<>();
            while (result.hasNext()) {
                Map<String, Object> r = result.next();
                producedNames.add((String) r.get("name"));
            }

            assertTrue(producedNames.containsAll(cypher25Names));
            assertEquals(
                    2,
                    producedNames.stream()
                            .filter(c -> c.startsWith("constraint_"))
                            .toList()
                            .size());
            assertEquals(5, producedNames.size());
        });
    }

    @Test
    public void testMultipleUniqueRelationshipConstraints() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.when IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[knows:KNOW]-() REQUIRE knows.how IS UNIQUE");
        awaitIndexesOnline();

        ArrayList<IndexConstraintRelationshipInfo> relConstraints = new ArrayList<>();
        relConstraints.add(new IndexConstraintRelationshipInfo(
                "CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE",
                "RELATIONSHIP_UNIQUENESS",
                List.of("year"),
                "",
                "SINCE"));
        relConstraints.add(
                new IndexConstraintRelationshipInfo(":SINCE(year)", "RANGE", List.of("year"), "ONLINE", "SINCE"));
        relConstraints.add(new IndexConstraintRelationshipInfo(
                "CONSTRAINT FOR ()-[liked:LIKED]-() REQUIRE liked.when IS UNIQUE",
                "RELATIONSHIP_UNIQUENESS",
                List.of("when"),
                "",
                "LIKED"));
        relConstraints.add(
                new IndexConstraintRelationshipInfo(":LIKED(when)", "RANGE", List.of("when"), "ONLINE", "LIKED"));
        relConstraints.add(new IndexConstraintRelationshipInfo(
                "CONSTRAINT FOR ()-[know:KNOW]-() REQUIRE know.how IS UNIQUE",
                "RELATIONSHIP_UNIQUENESS",
                List.of("how"),
                "",
                "KNOW"));
        relConstraints.add(
                new IndexConstraintRelationshipInfo(":KNOW(how)", "RANGE", List.of("how"), "ONLINE", "KNOW"));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testResult(db, "CALL apoc.schema.relationships", result -> {
                while (result.hasNext()) {
                    Map<String, Object> r = result.next();

                    assertEquals(
                            1,
                            relConstraints.stream()
                                    .filter(c -> c.properties.containsAll((List<String>) r.get("properties"))
                                            && c.type.equals(r.get("type"))
                                            && c.relationshipType.equals(r.get("relationshipType"))
                                            && (c.name.equals(r.get("name"))
                                                    || r.get("name").toString().startsWith("index_")
                                                    || r.get("name").toString().startsWith("constraint_")))
                                    .toList()
                                    .size());
                }
            });
        }
    }

    @Test
    public void testLookupIndexes() {
        db.executeTransactionally("CREATE LOOKUP INDEX node_label_lookup_index FOR (n) ON EACH labels(n)");
        db.executeTransactionally("CREATE LOOKUP INDEX rel_type_lookup_index FOR ()-[r]-() ON EACH type(r)");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testCall(db, preparser + "CALL apoc.schema.nodes()", (row) -> {
                assertEquals("ONLINE", row.get("status"));
                assertEquals(TOKEN_LABEL, row.get("label"));
                assertEquals("LOOKUP", row.get("type"));
                assertTrue(((List) row.get("properties")).isEmpty());
                assertEquals("NO FAILURE", row.get("failure"));
                assertEquals(100d, row.get("populationProgress"));
                assertEquals(1d, row.get("valuesSelectivity"));
                final String expectedUserDesc =
                        "name='node_label_lookup_index', type='LOOKUP', schema=(:<any-labels>), indexProvider='token-lookup-1.0'";
                assertThat(row.get("userDescription").toString()).contains(expectedUserDesc);

                if (cypherVersion.equals("5")) {
                    assertEquals(":" + TOKEN_LABEL + "()", row.get("name"));
                } else {
                    assertEquals("node_label_lookup_index", row.get("name"));
                }
            });

            testCall(db, preparser + "CALL apoc.schema.relationships()", (row) -> {
                assertEquals("ONLINE", row.get("status"));
                assertEquals("LOOKUP", row.get("type"));
                assertEquals(TOKEN_REL_TYPE, row.get("relationshipType"));
                assertTrue(((List) row.get("properties")).isEmpty());

                if (cypherVersion.equals("5")) {
                    assertEquals(":" + TOKEN_REL_TYPE + "()", row.get("name"));
                } else {
                    assertEquals("rel_type_lookup_index", row.get("name"));
                }
            });
        }
    }

    @Test
    public void testIndexesWithMultipleLabelsAndRelTypes() {
        final String idxName = "fullIdxNode";
        db.executeTransactionally(String.format(
                "CREATE FULLTEXT INDEX %s FOR (n:Blah|Moon) ON EACH [n.weightProp, n.anotherProp]", idxName));
        db.executeTransactionally(
                "CREATE FULLTEXT INDEX fullIdxRel FOR ()-[r:TYPE_1|TYPE_2]->() ON EACH [r.alpha, r.beta]");
        awaitIndexesOnline();

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            try (final var tx = db.beginTx()) {
                final var indexId = tx
                        .execute(
                                "SHOW INDEXES YIELD id, name WHERE name = $indexName RETURN id",
                                Map.of("indexName", idxName))
                        .columnAs("id")
                        .stream()
                        .findAny()
                        .orElse("=== Did not find id ===");
                final var expectedName =
                        "5".equals(cypherVersion) ? ":[Blah, Moon],(weightProp,anotherProp)" : "fullIdxNode";
                assertThat(tx.execute("CYPHER %s CALL apoc.schema.nodes()".formatted(cypherVersion)).stream())
                        .singleElement(map(String.class, Object.class))
                        .containsExactlyInAnyOrderEntriesOf(Map.of(
                                "status",
                                "ONLINE",
                                "label",
                                List.of("Blah", "Moon"),
                                "type",
                                "FULLTEXT",
                                "properties",
                                List.of("weightProp", "anotherProp"),
                                "failure",
                                "NO FAILURE",
                                "populationProgress",
                                100d,
                                "valuesSelectivity",
                                1d,
                                "size",
                                0L,
                                "name",
                                expectedName,
                                "userDescription",
                                "Index( id=%s, name='%s', type='FULLTEXT', schema=(:Blah|Moon {weightProp, anotherProp}), indexProvider='fulltext-2.0' )"
                                        .formatted(indexId, idxName)));

                assertThat(tx.execute("CYPHER %s CALL apoc.schema.relationships()".formatted(cypherVersion)).stream())
                        .singleElement(map(String.class, Object.class))
                        .containsExactlyInAnyOrderEntriesOf(Map.of(
                                "status",
                                "ONLINE",
                                "relationshipType",
                                List.of("TYPE_1", "TYPE_2"),
                                "properties",
                                List.of("alpha", "beta"),
                                "type",
                                "FULLTEXT",
                                "name",
                                "5".equals(cypherVersion) ? ":[TYPE_1, TYPE_2],(alpha,beta)" : "fullIdxRel"));
            }
        }
    }

    @Test
    public void testNodeConstraintExists() {
        db.executeTransactionally("CREATE CONSTRAINT personName FOR (person:Person) REQUIRE person.name IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT userId FOR (user:User) REQUIRE user.id IS UNIQUE");
        awaitIndexesOnline();

        testCall(db, "RETURN apoc.schema.node.constraintExists(\"Person\", [\"name\"]) AS output;", (r) -> {
            assertEquals(true, r.get("output"));
        });
    }

    @Test
    public void testNodeConstraintDoesntExist() {
        db.executeTransactionally("CREATE CONSTRAINT personName FOR (person:Person) REQUIRE person.name IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT userId FOR (user:User) REQUIRE user.id IS UNIQUE");
        awaitIndexesOnline();

        testCall(db, "RETURN apoc.schema.node.constraintExists(\"Person\", [\"name\", \"id\"]) AS output;", (r) -> {
            assertEquals(false, r.get("output"));
        });
    }

    @Test
    public void testNodeDoesntCheckRelationshipConstraintExist() {
        db.executeTransactionally("CREATE CONSTRAINT personName FOR (person:Person) REQUIRE person.name IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.when IS UNIQUE");
        awaitIndexesOnline();

        testCall(db, "RETURN apoc.schema.node.constraintExists(\"LIKED\", [\"when\"]) AS output;", (r) -> {
            assertEquals(false, r.get("output"));
        });
    }

    @Test
    public void testRelationshipConstraintExists() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.when IS UNIQUE");
        awaitIndexesOnline();

        testCall(db, "RETURN apoc.schema.relationship.constraintExists(\"SINCE\", [\"year\"]) AS output;", (r) -> {
            assertEquals(true, r.get("output"));
        });
    }

    @Test
    public void testRelationshipConstraintDoesntExist() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[since:SINCE]-() REQUIRE since.year IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.when IS UNIQUE");
        awaitIndexesOnline();

        testCall(
                db,
                "RETURN apoc.schema.relationship.constraintExists(\"SINCE\", [\"year\", \"when\"]) AS output;",
                (r) -> {
                    assertEquals(false, r.get("output"));
                });
    }

    @Test
    public void testRelationshipDoesntCheckNodesConstraintExist() {
        db.executeTransactionally("CREATE CONSTRAINT personName FOR (person:Person) REQUIRE person.name IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE like.when IS UNIQUE");
        awaitIndexesOnline();

        testCall(db, "RETURN apoc.schema.relationship.constraintExists(\"Person\", [\"name\"]) AS output;", (r) -> {
            assertEquals(false, r.get("output"));
        });
    }

    @Test
    public void testSchemaNodesWithFailedIndex() {
        // create property which will cause an index failure
        String largeProp = Util.readResourceFile("movies.cypher");
        db.executeTransactionally("CREATE (n:LabelTest {prop: $largeProp})", Map.of("largeProp", largeProp));

        // create the failed index and check that has state "FAILED"
        db.executeTransactionally("CREATE INDEX failedIdx FOR (n:LabelTest) ON (n.prop)");
        assertThrows(IllegalStateException.class, this::awaitIndexesOnline);

        testCall(
                db,
                "SHOW INDEXES YIELD name, state WHERE name = 'failedIdx'",
                (r) -> assertEquals("FAILED", r.get("state")));

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testCall(db, preparser + "CALL apoc.schema.nodes", r -> {
                String actualFailure = (String) r.get("failure");
                String expectedFailure =
                        "Property value is too large to index, please see index documentation for limitations.";
                assertThat(actualFailure).contains(expectedFailure);
                assertEquals("FAILED", r.get("status"));
                assertEquals("LabelTest", r.get("label"));
                assertEquals("RANGE", r.get("type"));
                assertEquals(List.of("prop"), r.get("properties"));

                // Cypher 25 updated to using the real name of the index, and not creating one.
                if (cypherVersion.equals("5")) {
                    assertEquals(":LabelTest(prop)", r.get("name"));
                } else {
                    assertEquals("failedIdx", r.get("name"));
                }
            });
        }
    }

    @Test
    public void testSchemaRelationshipsWithFailedIndex() {
        // create property which will cause an index failure
        String largeProp = Util.readResourceFile("movies.cypher");
        db.executeTransactionally(
                "CREATE (:Start)-[:REL_TEST {prop: $largeProp}]->(:End)",
                Map.of("largeProp", largeProp),
                Result::resultAsString);

        // create the failed index and check that has state "FAILED"
        db.executeTransactionally("CREATE INDEX failedIdx FOR ()-[r:REL_TEST]-() ON (r.prop)");
        assertThrows(IllegalStateException.class, this::awaitIndexesOnline);

        testCall(db, "SHOW INDEXES YIELD name, state WHERE name = 'failedIdx'", (r) -> {
            assertEquals("FAILED", r.get("state"));
        });

        for (String cypherVersion : Util.getCypherVersions()) {
            var preparser = "CYPHER " + cypherVersion + " ";
            testCall(db, preparser + "CALL apoc.schema.relationships", r -> {
                assertEquals("FAILED", r.get("status"));
                assertEquals("REL_TEST", r.get("relationshipType"));
                assertEquals("RANGE", r.get("type"));
                assertEquals(List.of("prop"), r.get("properties"));

                if (cypherVersion.equals("5")) {
                    assertEquals(":REL_TEST(prop)", r.get("name"));
                } else {
                    assertEquals("failedIdx", r.get("name"));
                }
            });
        }
    }
}
