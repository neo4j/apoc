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
package apoc.export.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import apoc.export.cypher.ExportFileManager;
import apoc.export.cypher.FileManagerFactory;
import apoc.result.ExportProgressInfo;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension(createDatabasePerTest = false)
class ExportUtilsTest {

    private static final String FORMAT = "json";
    private static final TerminationGuard NO_TERMINATION = () -> {};

    @Inject
    private GraphDatabaseService db;

    private ExecutorService executorService;

    @BeforeAll
    void beforeAll() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    void afterAll() {
        executorService.shutdownNow();
    }

    @Test
    void shouldPropagateFailureFromTheDumpThread() {
        var stream = progressInfoStream((tx, reporter) -> {
            throw new IllegalStateException("dump blew up");
        });

        assertThatThrownBy(stream::toList)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("dump blew up");
    }

    @Test
    void shouldWrapCheckedFailureFromTheDumpThread() {
        var stream = progressInfoStream((tx, reporter) -> {
            throw new RuntimeException(new java.io.IOException("disk full"));
        });

        assertThatThrownBy(stream::toList).rootCause().hasMessage("disk full");
    }

    @Test
    void shouldReturnProgressWhenTheDumpSucceeds() {
        var stream = progressInfoStream((tx, reporter) -> {
            reporter.update(2, 1, 5);
            reporter.done();
        });

        List<ExportProgressInfo> results = stream.toList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).nodes).isEqualTo(2);
        assertThat(results.get(0).relationships).isEqualTo(1);
        assertThat(results.get(0).properties).isEqualTo(5);
    }

    @Test
    void shouldEmitBatchesAlreadyReportedBeforeFailing() {
        var stream = progressInfoStream(
                (tx, reporter) -> {
                    reporter.update(1, 0, 1);
                    throw new IllegalStateException("dump blew up after the first batch");
                },
                1);

        assertThatThrownBy(stream::toList)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("dump blew up after the first batch");
    }

    private Stream<ExportProgressInfo> progressInfoStream(BiConsumer<Transaction, ProgressReporter> dump) {
        return progressInfoStream(dump, -1);
    }

    private Stream<ExportProgressInfo> progressInfoStream(
            BiConsumer<Transaction, ProgressReporter> dump, long batchSize) {
        ExportConfig config = new ExportConfig(Map.of("stream", true, "timeoutSeconds", 10L));
        ExportProgressInfo progressInfo = new ExportProgressInfo(null, "test", FORMAT);
        progressInfo.setBatchSize(batchSize);
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        ExportFileManager fileManager = FileManagerFactory.createFileManager(null, false, config);

        return ExportUtils.getProgressInfoStream(
                db, executorService, NO_TERMINATION, FORMAT, config, reporter, fileManager, dump);
    }
}
