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
package apoc.convert;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension(createDatabasePerTest = true, configurationCallback = "configure")
public class PathsToJsonTreeTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, Json.class);
    }

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.db_format, "aligned"); // Assertions depends on sequential ids
    }

    @Test
    public void testToTreeSimplePath() {
        /*            r:R
              a:A --------> b:B
        */
        db.executeTransactionally(
                "CREATE (a: A {nodeName: 'a'}) CREATE (b: B {nodeName: 'b'}) CREATE (a)-[r: R {relName: 'r'}]->(b)");

        var query =
                """
                MATCH path = (n)-[r]->(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "r":[
                             {
                                "nodeName":"b",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"B",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"A",
                          "_id": "${json-unit.any-number}",
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testSingleNode() {
        // a:A
        db.executeTransactionally("CREATE (a: A {nodeName: 'a'})");

        var query =
                """
                MATCH path = (n)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "_type":"A",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testSingleDisjointNodes() {
        // a:A
        db.executeTransactionally("CREATE (a: A {nodeName: 'a'}), (b: B {nodeName: 'b'}), (c: C {nodeName: 'c'})");

        var query =
                """
                MATCH path = (n)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRowA =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "_type":"A",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            var expectedRowB =
                    """
                    {   "tree":{
                          "nodeName":"b",
                          "_type":"B",
                          "_id":1,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            var expectedRowC =
                    """
                    {   "tree":{
                          "nodeName":"c",
                          "_type":"C",
                          "_id":2,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(
                            row -> assertThatJson(row).isEqualTo(expectedRowA),
                            row -> assertThatJson(row).isEqualTo(expectedRowB),
                            row -> assertThatJson(row).isEqualTo(expectedRowC));
        }
    }

    @Test
    public void testToTreeSimpleReversePath() {
        /*            r:R
              a:A <-------- b:B
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})<-[r: R {relName: 'r'}]-(b: B {nodeName: 'b'})");

        var query =
                """
                MATCH path = (n)<-[r]-(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {   "tree":{
                          "nodeName":"b",
                          "r":[
                             {
                                "nodeName":"a",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"A",
                                "_id":0,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"B",
                          "_id":1,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeSimpleBidirectionalPath() {
        /*         r1:R
                 -------->
             a:A          b:B
                 <--------
                   r2:R
        */
        db.executeTransactionally("CREATE "
                + "(a: A {nodeName: 'a'})<-[r1: R {relName: 'r'}]-(b: B {nodeName: 'b'}),"
                + "(a)-[r2: R {relName: 'r'}]->(b)");

        var query =
                """
                MATCH path = (n)-[r]-(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());
            var expectedRow =
                    """
                    {   "tree":{
                                "nodeName":"a",
                          "r":[
                             {
                                "nodeName":"b",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "r":[
                                   {
                                      "nodeName":"a",
                                      "r._id": "${json-unit.any-number}",
                                      "r._elementId": "${json-unit.any-string}",
                                      "_type":"A",
                                      "_id": "${json-unit.any-number}",
                                      "_elementId": "${json-unit.any-string}",
                                      "r.relName":"r"
                                   }
                                ],
                                "_type":"B",
                                "_id": "${json-unit.any-number}",
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"A",
                          "_id": "${json-unit.any-number}",
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeSimpleBidirectionalQuery() {
        /*         r1:R
             a:A --------> b:B
        */
        db.executeTransactionally("CREATE (a: A {nodeName: 'a'})-[r1: R {relName: 'r'}]->(b: B {nodeName: 'b'})");

        // Note this would be returning both the path (a)-[r]->(b) and (b)<-[r]-(a)
        // but we only expect a tree starting in 'a'
        var query =
                """
                MATCH path = (n)-[r]-(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "r":[
                             {
                                "nodeName":"b",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"B",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"A",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeSimpleQueryDisjointPaths() {
        /*         r1:R
             a:A --------> b:B
        */
        db.executeTransactionally("CREATE (a: A {nodeName: 'a'})-[r1: R {relName: 'r'}]->(b: B {nodeName: 'b'})");
        db.executeTransactionally("CREATE (c: C {nodeName: 'c'})-[r2: R {relName: 'r'}]->(d: D {nodeName: 'd'})");

        var query =
                """
                MATCH path = (n)-[r]->(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRowA =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "r":[
                             {
                                "nodeName":"b",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"B",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"A",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            var expectedRowC =
                    """
                    {   "tree":{
                          "nodeName":"c",
                          "r":[
                             {
                                "nodeName":"d",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"D",
                                "_id":3,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"C",
                          "_id":2,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRowA), row -> assertThatJson(row)
                            .isEqualTo(expectedRowC));
        }
    }

    @Test
    public void testToTreeBidirectionalPathAndQuery() {
        /*          r1:R1         r2:R2
              a:A ---------> b:B --------> a
        */
        db.executeTransactionally(
                "CREATE (a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'})-[r2: R2 {relName: 'r2'}]->(a)");

        // The query is bidirectional in this case, so
        // we would have duplicated paths, but we do not
        // expect duplicated trees
        var query =
                """
                MATCH path = (n)-[r]-(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "_type":"A",
                          "_id":"${json-unit.any-number}",
                          "_elementId": "${json-unit.any-string}",
                          "r1":[
                             {
                                "nodeName":"b",
                                "r2":[
                                   {
                                      "nodeName":"a",
                                      "r2._id":"${json-unit.any-number}",
                                      "r2._elementId": "${json-unit.any-string}",
                                      "_type":"A",
                                      "r2.relName":"r2",
                                      "_id":"${json-unit.any-number}",
                                      "_elementId": "${json-unit.any-string}"
                                   }
                                ],
                                "_type":"B",
                                "r1._id":"${json-unit.any-number}",
                                "r1._elementId": "${json-unit.any-string}",
                                "_id":"${json-unit.any-number}",
                                "_elementId": "${json-unit.any-string}",
                                "r1.relName":"r1"
                             }
                          ]
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeComplexGraph() {
        /*          r1:R1         r2:R2
              a:A --------> b:B ------> c:C
                             |
                      r3:R3  |
                            \|/
                            d:D
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'}),"
                + "(b)-[r2: R2 {relName: 'r2'}]->(c: C {nodeName: 'c'}),"
                + "(b)-[r3: R3 {relName: 'r3'}]->(d: D {nodeName: 'd'})");

        var query =
                """
                MATCH path = (n)-[r]->(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {  "tree": {
                        "nodeName": "a",
                        "_type": "A",
                        "_id": 0,
                        "_elementId": "${json-unit.any-string}",
                        "r1": [
                          {
                            "nodeName": "b",
                            "r2": [
                              {
                                "nodeName": "c",
                                "r2._id": "${json-unit.any-number}",
                                "r2._elementId": "${json-unit.any-string}",
                                "_type": "C",
                                "r2.relName": "r2",
                                "_id": 2,
                                "_elementId": "${json-unit.any-string}"
                              }
                            ],
                            "r3": [
                              {
                                "nodeName": "d",
                                "r3._id": "${json-unit.any-number}",
                                "r3._elementId": "${json-unit.any-string}",
                                "r3.relName": "r3",
                                "_type": "D",
                                "_id": 3,
                                "_elementId": "${json-unit.any-string}"
                              }
                            ],
                            "_type": "B",
                            "r1._id": "${json-unit.any-number}",
                            "r1._elementId": "${json-unit.any-string}",
                            "_id": 1,
                            "_elementId": "${json-unit.any-string}",
                            "r1.relName": "r1"
                          }
                        ]
                      }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeComplexGraphBidirectionalQuery() {
        /*          r1:R1         r2:R2
              a:A --------> b:B -------> c:C
                             |
                      r3:R3  |
                            \|/
                            d:D
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'}),"
                + "(b)-[r2: R2 {relName: 'r2'}]->(c: C {nodeName: 'c'}),"
                + "(b)-[r3: R3 {relName: 'r3'}]->(d: D {nodeName: 'd'})");

        // The query is bidirectional in this case, we don't expect duplicated paths
        var query =
                """
                MATCH path = (n)-[r]-(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {  "tree": {
                        "nodeName": "a",
                        "_type": "A",
                        "_id": 0,
                        "_elementId": "${json-unit.any-string}",
                        "r1": [
                          {
                            "nodeName": "b",
                            "r2": [
                              {
                                "nodeName": "c",
                                "r2._id":  "${json-unit.any-number}",
                                "r2._elementId": "${json-unit.any-string}",
                                "_type": "C",
                                "r2.relName": "r2",
                                "_id": 2,
                                "_elementId": "${json-unit.any-string}"
                              }
                            ],
                            "r3": [
                              {
                                "nodeName": "d",
                                "r3._id": "${json-unit.any-number}",
                                "r3._elementId": "${json-unit.any-string}",
                                "r3.relName": "r3",
                                "_type": "D",
                                "_id": 3,
                                "_elementId": "${json-unit.any-string}"
                              }
                            ],
                            "_type": "B",
                            "r1._id": "${json-unit.any-number}",
                            "r1._elementId": "${json-unit.any-string}",
                            "_id": 1,
                            "_elementId": "${json-unit.any-string}",
                            "r1.relName": "r1"
                          }
                        ]
                      }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeGraphWithLoops() {
        /*          r1:R1          r2:R2
              a:A ---------> b:B --------> c:C
                            /  /|
                            |___|
                            r3:R3
        */
        db.executeTransactionally("CREATE " + "(a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'}),"
                + "(b)-[r2: R2 {relName: 'r2'}]->(c:C {nodeName: 'c'}),"
                + "(b)-[r3: R3 {relName: 'r3'}]->(b)");

        var query =
                """
                MATCH path = (n)-[r]->(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedRow =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "_type":"A",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}",
                          "r1":[
                             {
                                "nodeName":"b",
                                "r2":[
                                   {
                                      "nodeName":"c",
                                      "r2._id": "${json-unit.any-number}",
                                      "r2._elementId": "${json-unit.any-string}",
                                      "_type":"C",
                                      "r2.relName":"r2",
                                      "_id":2,
                                      "_elementId": "${json-unit.any-string}"
                                   }
                                ],
                                "r3":[
                                   {
                                      "nodeName":"b",
                                      "r3._id": "${json-unit.any-number}",
                                      "r3._elementId": "${json-unit.any-string}",
                                      "r3.relName":"r3",
                                      "_type":"B",
                                      "_id":1,
                                      "_elementId": "${json-unit.any-string}"
                                   }
                                ],
                                "_type":"B",
                                "r1._id": "${json-unit.any-number}",
                                "r1._elementId": "${json-unit.any-string}",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r1.relName":"r1"
                             }
                          ]
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testIncomingRelationships() {
        /*          r1:R1         r2:R2
              a:A --------> b:B <------ c:C
        */
        db.executeTransactionally(
                "CREATE (a: A {nodeName: 'a'})-[r1: R1 {relName: 'r1'}]->(b: B {nodeName: 'b'})<-[r2: R2 {relName: 'r2'}]-(c:C {nodeName: 'c'})");

        var query =
                """
                MATCH path = (n)-[:R1]->(m)<-[:R2]-(o)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {sortPaths: false}) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            var expectedFirstRow =
                    """
                    {   "tree":{
                          "nodeName":"a",
                          "_type":"A",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}",
                          "r1":[
                             {
                                "nodeName":"b",
                                "_type":"B",
                                "r1._id": "${json-unit.any-number}",
                                "r1._elementId": "${json-unit.any-string}",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r1.relName":"r1"
                             }
                          ]
                       }
                    }""";
            var expectedSecondRow =
                    """
                    {   "tree":{
                          "nodeName":"c",
                          "r2":[
                             {
                                "nodeName":"b",
                                "r2._id": "${json-unit.any-number}",
                                "r2._elementId": "${json-unit.any-string}",
                                "_type":"B",
                                "r2.relName":"r2",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}"
                             }
                          ],
                          "_type":"C",
                          "_id":2,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedFirstRow), row -> assertThatJson(row)
                            .isEqualTo(expectedSecondRow));
        }
    }

    @Test
    public void testToTreeMultiLabelFilters() {
        /*            r:R
              a:A:B -------> c:C
        */
        db.executeTransactionally(
                "CREATE " + "(a: A: B {nodeName: 'a & b'})-[r: R {relName: 'r'}]->(c: C {nodeName: 'c'})");

        var query =
                """
                MATCH path = (n)-[r]->(m)
                WITH COLLECT(path) AS paths
                CALL apoc.paths.toJsonTree(paths, true, {nodes: { A: ['-nodeName'] } }) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            // No nodename under A:B
            var expectedRow =
                    """
                    {   "tree":{
                          "r":[
                             {
                                "nodeName":"c",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"C",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"A:B",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }

    @Test
    public void testToTreeMultiLabelFiltersForOldProcedure() {
        /*            r:R
              a:A:B -------> c:C
        */
        db.executeTransactionally(
                "CREATE " + "(a: A: B {nodeName: 'a & b'})-[r: R {relName: 'r'}]->(c: C {nodeName: 'c'})");

        var query =
                """
                CYPHER 5
                MATCH path = (n)-[r]->(m)
                WITH COLLECT(path) AS paths
                CALL apoc.convert.toTree(paths, true, {nodes: { A: ['-nodeName'] } }) YIELD value AS tree
                RETURN tree""";

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(query);
            var rows = result.stream().collect(Collectors.toList());

            assertEquals(rows.size(), 1);
            // No nodename under A:B
            var expectedRow =
                    """
                    {   "tree":{
                          "r":[
                             {
                                "nodeName":"c",
                                "r._id": "${json-unit.any-number}",
                                "r._elementId": "${json-unit.any-string}",
                                "_type":"C",
                                "_id":1,
                                "_elementId": "${json-unit.any-string}",
                                "r.relName":"r"
                             }
                          ],
                          "_type":"A:B",
                          "_id":0,
                          "_elementId": "${json-unit.any-string}"
                       }
                    }""";
            assertThat(tx.execute(query).stream())
                    .satisfiesExactly(row -> assertThatJson(row).isEqualTo(expectedRow));
        }
    }
}
