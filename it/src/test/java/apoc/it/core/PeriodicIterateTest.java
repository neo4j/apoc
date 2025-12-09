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

import static apoc.util.TestContainerUtil.createDB;
import static apoc.util.TestContainerUtil.dockerImageForNeo4j;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;

class PeriodicIterateTest {

    // The Query Log is only accessible on Enterprise
    @Test
    void check_metadata_in_batches() {
        try {
            Neo4jContainerExtension neo4jContainer = createDB(
                            TestContainerUtil.Neo4jVersion.ENTERPRISE,
                            List.of(TestContainerUtil.ApocPackage.CORE),
                            !TestUtil.isRunningInCI())
                    .withNeo4jConfig("dbms.transaction.timeout", "60s");

            neo4jContainer.start();

            Session session = neo4jContainer.getSession();
            session
                    .run(
                            "CALL apoc.periodic.iterate(\"MATCH (p:Person) RETURN p\"," + "\"SET p.name='test'\","
                                    + "{batchSize:1, parallel:false})",
                            TransactionConfig.builder()
                                    .withMetadata(Map.of("shouldAppear", "inBatches"))
                                    .build())
                    .stream()
                    .count();
            var queryLogs = neo4jContainer.queryLogs();
            Assertions.assertTrue(queryLogs.contains(
                    "SET p.name='test' - {_batch: [], _count: 0} - runtime=pipelined - {shouldAppear: 'inBatches'}"));
            session.close();
            neo4jContainer.close();
        } catch (Exception ex) {
            // if Testcontainers wasn't able to retrieve the docker image we ignore the test
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                Assertions.fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            } else if (!TestUtil.isRunningInCI()) {
                Assertions.fail("The docker image " + dockerImageForNeo4j(TestContainerUtil.Neo4jVersion.ENTERPRISE)
                        + " could not be loaded. Check whether it's available locally / in the CI. Exception:"
                        + ex);
            }
        }
    }
}
