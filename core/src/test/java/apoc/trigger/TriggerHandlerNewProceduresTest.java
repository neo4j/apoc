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
package apoc.trigger;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import apoc.util.TestUtil;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class TriggerHandlerNewProceduresTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        apocConfig().setProperty(APOC_TRIGGER_ENABLED, false);
        TestUtil.registerProcedure(db, Trigger.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void setLastUpdateRecoversWhenMultipleMetaNodesExistForSameDatabase() {
        GraphDatabaseAPI dbApi = db;
        String databaseName = "neo4j";

        // simulate the edge case where duplicate ApocTriggerMeta nodes exist for the same database name,
        // e.g. because the triggerConstraint uniqueness constraint wasn't yet in place
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 3; i++) {
                Node node = tx.createNode(SystemLabels.ApocTriggerMeta);
                node.setProperty(SystemPropertyKeys.database.name(), databaseName);
            }
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            TriggerHandlerNewProcedures.setLastUpdate(dbApi, databaseName, tx);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            List<Node> nodes =
                    tx
                            .findNodes(SystemLabels.ApocTriggerMeta, SystemPropertyKeys.database.name(), databaseName)
                            .stream()
                            .toList();
            assertEquals(1, nodes.size());
            assertTrue(nodes.get(0).hasProperty(SystemPropertyKeys.lastUpdated.name()));
        }
    }
}
