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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.SystemLabels;
import apoc.SystemPropertyKeys;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension
class TriggerHandlerNewProceduresTest {

    @Inject
    GraphDatabaseService db;

    @AfterEach
    void tearDown() {
        db.executeTransactionally("MATCH (n:" + SystemLabels.ApocTriggerMeta + ") DELETE n");
    }

    @Test
    void setLastUpdateRecoversWhenMultipleMetaNodesExistForSameDatabase() {
        GraphDatabaseAPI dbApi = (GraphDatabaseAPI) db;
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
