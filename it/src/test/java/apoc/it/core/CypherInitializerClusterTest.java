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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CypherInitializerClusterTest {

    private static TestcontainersCausalCluster cluster;
    private static List<Neo4jContainerExtension> clusterMembers;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil.createEnterpriseCluster(
                List.of(TestContainerUtil.ApocPackage.CORE),
                3,
                0,
                Collections.emptyMap(),
                Map.of("NEO4J_dbms_routing_enabled", "true"));

        clusterMembers = cluster.getClusterMembers();
        assertEquals(3, clusterMembers.size());
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void triggerConstraintDoesntErrorOnFollowers() {
        for (Neo4jContainerExtension member : clusterMembers) {
            assertFalse(
                    "Cluster member logged a follower write error during CypherInitializer startup",
                    member.getLogs().contains("No write operations are allowed directly on this database"));
        }
    }
}
