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
package apoc.refactor.util;

import static apoc.util.TestUtil.testCall;
import static apoc.util.Util.map;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.refactor.GraphRefactoring;
import apoc.util.ArrayBackedList;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension()
public class PropertiesManagerTest {

    @Inject
    GraphDatabaseService db;

    private final String QUERY =
            """
            MATCH (d:Person {name:'Daniele'})
            MATCH (p:Country {name:'USA'})
            MATCH (d)-[r:TRAVELS_TO]->(p)
            MATCH (d)-[h:GOES_TO]->(p)
            MATCH (d)-[l:FLIGHTS_TO]->(p) return r as rel1,h as rel2""";

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, GraphRefactoring.class);
    }

    @Test
    public void testCombinePropertiesTargetArrayValuesSourceSingleValuesSameType() {
        TestUtil.singleResultFirstColumn(
                db,
                """
                        Create (d:Person {name:'Daniele'})
                        Create (p:Country {name:'USA'})
                        Create (d)-[:TRAVELS_TO {year:[2010,2015], reason:"work"}]->(p)
                        Create (d)-[:GOES_TO {year:1995, reason:"fun"}]->(p)
                        Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList(2010L, 2015L, 1995L),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testCombinePropertiesTargetSingleValueSourceArrayValuesSameType() {
        TestUtil.singleResultFirstColumn(
                db,
                """
                        Create (d:Person {name:'Daniele'})
                        Create (p:Country {name:'USA'})
                        Create (d)-[:TRAVELS_TO {year:1995, reason:"work"}]->(p)
                        Create (d)-[:GOES_TO {year:[2010,2015], reason:"fun"}]->(p)
                        Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList(1995L, 2010L, 2015L),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testCombinePropertiesTargetArrayValueSourceArrayValuesSameType() {
        db.executeTransactionally(
                """
                Create (d:Person {name:'Daniele'})
                Create (p:Country {name:'USA'})
                Create (d)-[:TRAVELS_TO {year:[1995,2014], reason:"work"}]->(p)
                Create (d)-[:GOES_TO {year:[2010,2015], reason:"fun"}]->(p)
                Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList(1995L, 2014L, 2010L, 2015L),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testCombinePropertiesTargetArrayValuesSourceSingleValuesDifferentType() {
        db.executeTransactionally(
                """
                Create (d:Person {name:'Daniele'})
                Create (p:Country {name:'USA'})
                Create (d)-[:TRAVELS_TO {year:[2010,2015], reason:"work"}]->(p)
                Create (d)-[:GOES_TO {year:"1995", reason:"fun"}]->(p)
                Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList("2010", "2015", "1995"),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testCombinePropertiesTargetSingleValueSourceArrayValuesDifferentType() {
        db.executeTransactionally(
                """
                Create (d:Person {name:'Daniele'})
                Create (p:Country {name:'USA'})
                Create (d)-[:TRAVELS_TO {year:1995, reason:"work"}]->(p)
                Create (d)-[:GOES_TO {year:["2010","2015"], reason:"fun"}]->(p)
                Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList("1995", "2010", "2015"),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testCombinePropertiesTargetArrayValueSourceArrayValuesDifferentTypeAndOneSameValue() {
        db.executeTransactionally(
                """
                Create (d:Person {name:'Daniele'})
                Create (p:Country {name:'USA'})
                Create (d)-[:TRAVELS_TO {year:["1995","2014"], reason:"work"}]->(p)
                Create (d)-[:GOES_TO {year:[2010,2015], reason:"fun"}]->(p)
                Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList("1995", "2014", "2010", "2015"),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testCombinePropertiesTargetSingleValueSourceSingleValuesSameTypeAndSameValue() {
        db.executeTransactionally(
                """
                Create (d:Person {name:'Daniele'})
                Create (p:Country {name:'USA'})
                Create (d)-[:TRAVELS_TO {year:1996, reason:"work"}]->(p)
                Create (d)-[:GOES_TO {year:1996, reason:"fun"}]->(p)
                Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(1996L, rel1.getProperty("year"));
        });
    }

    @Test
    public void testCombinePropertiesTargetArrayValueSourceArrayValuesSameTypeOneSameValue() {
        db.executeTransactionally(
                """
                Create (d:Person {name:'Daniele'})
                Create (p:Country {name:'USA'})
                Create (d)-[:TRAVELS_TO {year:[1995,2014], reason:"work"}]->(p)
                Create (d)-[:GOES_TO {year:[2010,2014], reason:"fun"}]->(p)
                Create (d)-[:FLIGHTS_TO {company:"Air America"}]->(p) RETURN id(p) as id""");
        testCall(db, QUERY, (r) -> {
            Relationship rel1 = (Relationship) r.get("rel1");
            Relationship rel2 = (Relationship) r.get("rel2");

            PropertiesManager.mergeProperties(
                    rel2.getProperties("year"), rel1, new RefactorConfig(map("properties", "combine")));

            assertEquals(
                    asList(1995L, 2014L, 2010L),
                    new ArrayBackedList(rel1.getProperty("year")).stream().toList());
        });
    }

    @Test
    public void testMergeProperties() {
        List<Node> nodes = TestUtil.firstColumn(
                db,
                "UNWIND [{name:'Joe',age:42,kids:'Jane'},{name:'Jane',age:32,kids:'June'}] AS data CREATE (p:Person) SET p = data RETURN p");
        try (Transaction tx = db.beginTx()) {
            Node target = Util.rebind(tx, nodes.get(0));
            Node source = Util.rebind(tx, nodes.get(1));
            PropertiesManager.mergeProperties(
                    source.getAllProperties(),
                    target,
                    new RefactorConfig(map(
                            "properties",
                            map(
                                    "nam.*",
                                    RefactorConfig.DISCARD,
                                    "age",
                                    RefactorConfig.OVERWRITE,
                                    "kids",
                                    RefactorConfig.COMBINE))));
            assertEquals("Joe", target.getProperty("name"));
            assertEquals(32L, target.getProperty("age"));
            assertEquals(asList("Jane", "June"), asList((String[]) target.getProperty("kids")));
            tx.commit();
        }
    }
}
