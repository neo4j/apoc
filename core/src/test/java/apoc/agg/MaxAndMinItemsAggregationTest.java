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
package apoc.agg;

import static apoc.util.TestUtil.testCall;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import apoc.util.Util;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension(createDatabasePerTest = false)
public class MaxAndMinItemsAggregationTest {
    @Inject
    GraphDatabaseService db;

    @BeforeAll
    public void setUp() {
        TestUtil.registerProcedure(db, MaxAndMinItems.class);

        db.executeTransactionally(Util.readResourceFile("movies.cypher"));
        db.executeTransactionally(
                "MATCH (p:Person) MERGE (b:BigBrother {name : 'Big Brother' })  MERGE (b)-[:FOLLOWS]->(p)");
    }

    @Test
    public void testBasicMax() {
        testCall(
                db,
                "UNWIND RANGE(0,10) as value " + "WITH apoc.agg.maxItems(value, value) as maxResult "
                        + "RETURN maxResult.value as value, maxResult.items as items",
                (row) -> {
                    assertEquals(10L, row.get("value"));
                    assertThat(row.get("items")).asInstanceOf(list(Long.class)).containsExactly(10L);
                });

        testCall(
                db,
                "UNWIND RANGE(0,10) as value " + "WITH apoc.agg.maxItems(value, value) as maxResult "
                        + "ORDER BY maxResult.value DESC "
                        + "RETURN maxResult.value as value, maxResult.items as items",
                (row) -> {
                    assertEquals(10L, row.get("value"));
                    assertThat(row.get("items")).asInstanceOf(list(Long.class)).containsExactly(10L);
                });
    }

    @Test
    public void testBasicMin() {
        testCall(
                db,
                "UNWIND RANGE(0,10) as value " + "WITH apoc.agg.minItems(value, value) as minResult "
                        + "RETURN minResult.value as value, minResult.items as items",
                (row) -> {
                    assertEquals(0L, row.get("value"));
                    assertThat(row.get("items")).asInstanceOf(list(Long.class)).containsExactly(0L);
                });

        testCall(
                db,
                "UNWIND RANGE(0,10) as value " + "WITH apoc.agg.minItems(value, value) as minResult "
                        + "ORDER BY minResult.value DESC "
                        + "RETURN minResult.value as value, minResult.items as items",
                (row) -> {
                    assertEquals(0L, row.get("value"));
                    assertThat(row.get("items")).asInstanceOf(list(Long.class)).containsExactly(0L);
                });
    }

    @Test
    public void testMaxWithGrouping() {
        /** comparing to:
         * MATCH (p:Person)
         * WHERE p.born <= 1974
         * WITH p.born as born, collect(p.name) as persons
         * ORDER BY born DESC
         * LIMIT 1
         * RETURN born, persons
         *
         * returns {born:1974, persons:["Jerry O'Connell", "Christian Bale"]}
         */
        testCall(
                db,
                "MATCH (p:Person) " + "WHERE p.born <= 1974 "
                        + "WITH apoc.agg.maxItems(p, p.born) as maxResult "
                        + "RETURN maxResult.value as born, [person in maxResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1974L, row.get("born"));
                    assertThat(row.get("persons"))
                            .asInstanceOf(list(String.class))
                            .containsExactlyInAnyOrder("Jerry O'Connell", "Christian Bale");
                });
    }

    @Test
    public void testMinWithGrouping() {
        /** comparing to:
         * MATCH (p:Person)
         * WHERE p.born >= 1930
         * WITH p.born as born, collect(p.name) as persons
         * ORDER BY born ASC
         * LIMIT 1
         * RETURN born, persons
         *
         * returns {born:1930, persons:["Gene Hackman", "Richard Harris", "Clint Eastwood"]}
         * with limited grouping will return a limited subset of the results
         */
        testCall(
                db,
                "MATCH (p:Person) " + "WHERE p.born >= 1930 "
                        + "WITH apoc.agg.minItems(p, p.born) as minResult "
                        + "RETURN minResult.value as born, [person in minResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1930L, row.get("born"));
                    assertThat(row.get("persons"))
                            .asInstanceOf(list(String.class))
                            .containsExactlyInAnyOrder("Gene Hackman", "Richard Harris", "Clint Eastwood");
                });
    }

    @Test
    public void testMaxWithLimitedGrouping() {
        /** comparing to:
         * MATCH (p:Person)
         * WHERE p.born <= 1974
         * WITH p.born as born, collect(p.name) as persons
         * ORDER BY born DESC
         * LIMIT 1
         * RETURN born, persons
         *
         * returns {born:1974, persons:["Jerry O'Connell", "Christian Bale"]}
         * with limited grouping will return a limited subset of the results
         */
        testCall(
                db,
                "MATCH (p:Person) " + "WHERE p.born <= 1974 "
                        + "WITH apoc.agg.maxItems(p, p.born, 1) as maxResult "
                        + "RETURN maxResult.value as born, [person in maxResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(1974L, row.get("born"));
                    assertThat(row.get("persons"))
                            .asInstanceOf(list(String.class))
                            .singleElement()
                            .isIn("Jerry O'Connell", "Christian Bale");
                });
    }

    @Test
    public void testMinWithLimitedGrouping() {
        /** comparing to:
         * MATCH (p:Person)
         * WHERE p.born >= 1930
         * WITH p.born as born, collect(p.name) as persons
         * ORDER BY born ASC
         * LIMIT 1
         * RETURN born, persons
         *
         * returns {born:1930, persons:["Gene Hackman", "Richard Harris", "Clint Eastwood"]}
         * with limited grouping will return a limited subset of the results
         */
        testCall(
                db,
                "MATCH (p:Person) " + "WHERE p.born >= 1930 "
                        + "WITH apoc.agg.minItems(p, p.born, 2) as minResult "
                        + "RETURN minResult.value as born, [person in minResult.items | person.name] as persons",
                (row) -> {
                    assertThat(row).containsEntry("born", 1930L).hasEntrySatisfying("persons", p -> assertThat(p)
                            .asInstanceOf(list(String.class))
                            .containsAnyOf("Gene Hackman", "Richard Harris", "Clint Eastwood"));
                });
    }

    @Test
    public void testMaxWithNullValuesProducesNoResults() {
        testCall(
                db,
                "MATCH (p:Person) " + "WITH apoc.agg.maxItems(p, p.doesNotExist) as maxResult "
                        + "RETURN maxResult.value as value, [person in maxResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(null, row.get("value"));
                    assertThat(row.get("persons"))
                            .asInstanceOf(list(Object.class))
                            .isEmpty();
                });
    }

    @Test
    public void testMinWithNullValuesProducesNoResults() {
        testCall(
                db,
                "MATCH (p:Person) " + "WITH apoc.agg.minItems(p, p.doesNotExist) as minResult "
                        + "RETURN minResult.value as value, [person in minResult.items | person.name] as persons",
                (row) -> {
                    assertEquals(null, row.get("value"));
                    assertThat(row.get("persons"))
                            .asInstanceOf(list(Object.class))
                            .isEmpty();
                });
    }
}
