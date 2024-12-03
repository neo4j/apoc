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
package apoc.util.kernel;

import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.BatchJobResult;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.forAllNodes;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class MultiThreadedGlobalGraphOperationsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        createData();
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    private static void createData() {
        db.executeTransactionally(
                "UNWIND range(1,1000) as x MERGE (s{id:x}) MERGE (e{id:x+1}) merge (s)-[:REL{id:x}]->(e)");
    }

    @Test
    public void shouldforAllNodesWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result =
                forAllNodes(db, Executors.newFixedThreadPool(4), 10, (nodeCursor) -> counter.incrementAndGet());
        assertEquals(1001, counter.get());
        assertEquals(1001, result.getSucceeded());
        assertEquals(0, result.getFailures());
    }
}
