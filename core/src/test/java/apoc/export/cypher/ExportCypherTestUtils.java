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
package apoc.export.cypher;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;

import apoc.HelperProcedures;
import apoc.cypher.Cypher;
import apoc.graph.Graphs;
import apoc.schema.Schemas;
import apoc.util.TestUtil;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;

public class ExportCypherTestUtils {

    private static final String OPTIMIZED = "Optimized";
    private static final String ODD = "OddDataset";
    private static final String ROUND_TRIP = "RoundTrip";

    public static void setUp(GraphDatabaseService db, TestName testName) {
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(
                db, ExportCypher.class, Graphs.class, Schemas.class, Cypher.class, HelperProcedures.class);
        if (testName.getMethodName().contains(ROUND_TRIP)) return;
        db.executeTransactionally("CREATE RANGE INDEX barIndex FOR (n:Bar) ON (n.first_name, n.last_name)");
        db.executeTransactionally("CREATE RANGE INDEX fooIndex FOR (n:Foo) ON (n.name)");
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint FOR (b:Bar) REQUIRE b.name IS UNIQUE");
        db.executeTransactionally(
                "CREATE CONSTRAINT uniqueConstraintComposite FOR (b:Bar) REQUIRE (b.name, b.age) IS UNIQUE");
        if (testName.getMethodName().endsWith(OPTIMIZED)) {
            db.executeTransactionally(
                    "CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:17}),"
                            + " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})");
        } else if (testName.getMethodName().endsWith(ODD)) {
            db.executeTransactionally("CREATE (f:Foo {name:'foo', born:date('2018-10-31')}),"
                    + "(t:Foo {name:'foo2', born:date('2017-09-29')}),"
                    + "(g:Foo {name:'foo3', born:date('2016-03-12')}),"
                    + "(b:Bar {name:'bar',age:42}),"
                    + "(c:Bar {age:12}),"
                    + "(d:Bar {age:4}),"
                    + "(e:Bar {name:'bar2',age:44}),"
                    + "(f)-[:KNOWS {since:2016}]->(b)");
        } else {
            db.executeTransactionally(
                    "CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})");
        }
    }

    protected static final String NODES_MULTI_RELS =
            ":begin\n" + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.name=\"MyName\", n:Person;\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) SET n.a=1, n:Project;\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.name=\"one\", n:Team;\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) SET n.name=\"two\", n:Team;\n"
                    + ":commit\n";

    protected static final String NODES_MULTI_RELS_ADD_STRUCTURE = ":begin\n"
            + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) ON CREATE SET n.name=\"MyName\", n:Person;\n"
            + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) ON CREATE SET n.a=1, n:Project;\n"
            + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) ON CREATE SET n.name=\"one\", n:Team;\n"
            + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) ON CREATE SET n.name=\"two\", n:Team;\n"
            + ":commit\n";

    protected static final String NODES_UNWIND = ":begin\n" + "UNWIND [{_id:1, properties:{a:1}}] AS row\n"
            + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Project;\n"
            + "UNWIND [{_id:2, properties:{name:\"one\"}}, {_id:3, properties:{name:\"two\"}}] AS row\n"
            + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Team;\n"
            + "UNWIND [{_id:0, properties:{name:\"MyName\"}}] AS row\n"
            + "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Person;\n"
            + ":commit\n";

    protected static final String NODES_UNWIND_ADD_STRUCTURE =
            ":begin\n" + "UNWIND [{_id:1, properties:{a:1}}] AS row\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Project;\n"
                    + "UNWIND [{_id:2, properties:{name:\"one\"}}, {_id:3, properties:{name:\"two\"}}] AS row\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Team;\n"
                    + "UNWIND [{_id:0, properties:{name:\"MyName\"}}] AS row\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Person;\n"
                    + ":commit\n";

    protected static final String NODES_UNWIND_UPDATE_STRUCTURE =
            ":begin\n" + "UNWIND [{_id:1, properties:{a:1}}] AS row\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Project;\n"
                    + "UNWIND [{_id:2, properties:{name:\"one\"}}, {_id:3, properties:{name:\"two\"}}] AS row\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Team;\n"
                    + "UNWIND [{_id:0, properties:{name:\"MyName\"}}] AS row\n"
                    + "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Person;\n"
                    + ":commit\n";

    protected static final String NODES_MULTI_REL_CREATE =
            ":begin\n" + "CREATE (:Person:`UNIQUE IMPORT LABEL` {name:\"MyName\", `UNIQUE IMPORT ID`:0});\n"
                    + "CREATE (:Project:`UNIQUE IMPORT LABEL` {a:1, `UNIQUE IMPORT ID`:1});\n"
                    + "CREATE (:Team:`UNIQUE IMPORT LABEL` {name:\"one\", `UNIQUE IMPORT ID`:2});\n"
                    + "CREATE (:Team:`UNIQUE IMPORT LABEL` {name:\"two\", `UNIQUE IMPORT ID`:3});\n"
                    + ":commit\n";

    protected static final String SCHEMA_WITH_UNIQUE_IMPORT_ID = ":begin\n"
            + "CREATE CONSTRAINT UNIQUE_IMPORT_NAME FOR (node:`UNIQUE IMPORT LABEL`) REQUIRE (node.`UNIQUE IMPORT ID`) IS UNIQUE;\n"
            + ":commit\n"
            + "CALL db.awaitIndexes(300);\n";

    protected static final String SCHEMA_UPDATE_STRUCTURE_MULTI_REL =
            ":begin\n" + ":commit\n" + "CALL db.awaitIndexes(300);\n";

    protected static final String RELS_MULTI_RELS = ":begin\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:0}]->(n2) SET r.id=1;\n"
            + "\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:1}]->(n2) SET r.id=2;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:2}]->(n2) SET r.id=2;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:3}]->(n2) SET r.id=3;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:4}]->(n2) SET r.id=4;\n"
            + ":commit\n"
            + ":begin\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:5}]->(n2) SET r.id=5;\n"
            + "\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) SET r.name=\"aaa\";\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) SET r.name=\"eee\";\n"
            + ":commit\n";

    protected static final String RELS_UNWIND_MULTI_RELS =
            """
            :begin
            UNWIND [{start: {_id:0}, id: 0, end: {_id:1}, properties:{id:1}}, {start: {_id:0}, id: 1, end: {_id:1}, properties:{id:2}}, {start: {_id:0}, id: 2, end: {_id:1}, properties:{id:2}}, {start: {_id:0}, id: 3, end: {_id:1}, properties:{id:3}}, {start: {_id:0}, id: 4, end: {_id:1}, properties:{id:4}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            CREATE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;
            :commit

            :begin
            UNWIND [{start: {_id:0}, id: 6, end: {_id:2}, properties:{name:"aaa"}}, {start: {_id:0}, id: 7, end: {_id:3}, properties:{name:"eee"}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            CREATE (start)-[r:IS_TEAM_MEMBER_OF{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;
            UNWIND [{start: {_id:0}, id: 5, end: {_id:1}, properties:{id:5}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            CREATE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;
            :commit

            """;

    protected static final String RELS_UNWIND_UPDATE_ALL_MULTI_RELS =
            """
            :begin
            UNWIND [{start: {_id:0}, id: 0, end: {_id:1}, properties:{id:1}}, {start: {_id:0}, id: 1, end: {_id:1}, properties:{id:2}}, {start: {_id:0}, id: 2, end: {_id:1}, properties:{id:2}}, {start: {_id:0}, id: 3, end: {_id:1}, properties:{id:3}}, {start: {_id:0}, id: 4, end: {_id:1}, properties:{id:4}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            MERGE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;
            :commit

            :begin
            UNWIND [{start: {_id:0}, id: 6, end: {_id:2}, properties:{name:"aaa"}}, {start: {_id:0}, id: 7, end: {_id:3}, properties:{name:"eee"}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            MERGE (start)-[r:IS_TEAM_MEMBER_OF{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;
            UNWIND [{start: {_id:0}, id: 5, end: {_id:1}, properties:{id:5}}] AS row
            MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})
            MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
            MERGE (start)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:row.id}]->(end) SET r += row.properties;
            :commit
            """;

    protected static final String RELS_ADD_STRUCTURE_MULTI_RELS = ":begin\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:1}]->(n2);\n"
            + "\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:2}]->(n2);\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:2}]->(n2);\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:3}]->(n2);\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:4}]->(n2);\n"
            + ":commit\n"
            + ":begin\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:WORKS_FOR {id:5}]->(n2);\n"
            + "\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) CREATE (n1)-[r:IS_TEAM_MEMBER_OF {name:\"aaa\"}]->(n2);\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) CREATE (n1)-[r:IS_TEAM_MEMBER_OF {name:\"eee\"}]->(n2);\n"
            + ":commit\n";

    protected static final String RELSUPDATE_STRUCTURE_2 = ":begin\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:0}]->(n2) ON CREATE SET r.id=1;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:1}]->(n2) ON CREATE SET r.id=2;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:2}]->(n2) ON CREATE SET r.id=2;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:3}]->(n2) ON CREATE SET r.id=3;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:4}]->(n2) ON CREATE SET r.id=4;\n"
            + "\n"
            + ":commit\n"
            + ":begin\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) MERGE (n1)-[r:WORKS_FOR{`UNIQUE IMPORT ID REL`:5}]->(n2) ON CREATE SET r.id=5;\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) ON CREATE SET r.name=\"aaa\";\n"
            + "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}) MERGE (n1)-[r:IS_TEAM_MEMBER_OF]->(n2) ON CREATE SET r.name=\"eee\";\n"
            + ":commit\n";

    protected static final String CLEANUP_SMALL_BATCH_NODES = ":begin\n"
            + "MATCH (n:`UNIQUE IMPORT LABEL`) WITH n LIMIT 5 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;\n"
            + ":commit\n"
            + ":begin\n"
            + "DROP CONSTRAINT UNIQUE_IMPORT_NAME;\n"
            + ":commit\n";

    private static final String CLEANUP_SMALL_BATCH_MULTI_REL = ":begin\n"
            + "MATCH ()-[r]->() WHERE r.`UNIQUE IMPORT ID REL` IS NOT NULL WITH r LIMIT 5 REMOVE r.`UNIQUE IMPORT ID REL`;\n"
            + ":commit\n"
            + ":begin\n"
            + "MATCH ()-[r]->() WHERE r.`UNIQUE IMPORT ID REL` IS NOT NULL WITH r LIMIT 5 REMOVE r.`UNIQUE IMPORT ID REL`;\n"
            + ":commit\n";

    protected static final String CLEANUP_SMALL_BATCH = CLEANUP_SMALL_BATCH_NODES + CLEANUP_SMALL_BATCH_MULTI_REL;
    protected static final String CLEANUP_SMALL_BATCH_ONLY_RELS =
            ":begin\n:commit\n:begin\n:commit\n" + CLEANUP_SMALL_BATCH_MULTI_REL;

    protected static final String CLEANUP_EMPTY = ":begin\n:commit\n:begin\n:commit\n";
}
