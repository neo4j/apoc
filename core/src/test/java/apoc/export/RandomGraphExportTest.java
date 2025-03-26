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
package apoc.export;

import static apoc.test.util.RandomGraph.assertEqualGraph;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import apoc.HelperProcedures;
import apoc.cypher.Cypher;
import apoc.export.cypher.ExportCypher;
import apoc.graph.Graphs;
import apoc.schema.Schemas;
import apoc.test.util.RandomGraph;
import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
@ImpermanentEnterpriseDbmsExtension
public class RandomGraphExportTest {
    @Inject
    DatabaseManagementService dbms;

    @Inject
    GraphDatabaseService db;

    @Inject
    RandomSupport rand;

    GraphDatabaseService dbA;
    GraphDatabaseService dbB;

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(
                db, ExportCypher.class, Graphs.class, Schemas.class, Cypher.class, HelperProcedures.class);
    }

    @BeforeEach
    void beforeEach() {
        dbms.createDatabase("dba");
        dbA = dbms.database("dba");
        dbms.createDatabase("dbb");
        dbB = dbms.database("dbb");
        assertThat(dbA.isAvailable(10_000)).isTrue();
        assertThat(dbB.isAvailable(10_000)).isTrue();
    }

    @AfterEach
    void afterEach() {
        if (dbA != null) dbms.dropDatabase(dbA.databaseName());
        if (dbB != null) dbms.dropDatabase(dbB.databaseName());
        dbA = null;
        dbB = null;
    }

    @Test
    void exportCypherRandomGraph() {
        new RandomGraph(rand, RandomGraph.DEFAULT_CONF).commitRandomGraph(dbA);
        final var batchSize = rand.intBetween(1, 20_000);
        final var unwindBatchSize = rand.intBetween(1, Math.min(batchSize, 50));
        final var exportParams = Map.<String, Object>of(
                "conf", Map.of("stream", true, "batchSize", batchSize, "unwindBatchSize", unwindBatchSize));
        final var exportQuery = "CALL apoc.export.cypher.all(null, $conf) yield cypherStatements";
        final var importQuery = "CALL apoc.cypher.runMany($cypherStatements, {}, {})";
        performStreamingExport(exportQuery, importQuery, exportParams);
        assertEqualGraph(dbB, dbA);
    }

    private void performStreamingExport(String exportQuery, String importQuery, Map<String, Object> exportParams) {
        try (final var txA = dbA.beginTx();
                final var txb = dbB.beginTx()) {
            try (final var export = txA.execute(exportQuery, exportParams)) {
                final var columns = export.columns();
                export.accept(row -> {
                    final var params = columns.stream().collect(Collectors.toUnmodifiableMap(c -> c, row::get));
                    txb.execute(importQuery, params).accept(importRow -> true);
                    return true;
                });
            }
        }
    }
}
