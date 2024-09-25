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
package apoc.export.json;

import apoc.Pools;
import apoc.export.util.CountingReader;
import apoc.export.util.ProgressReporter;
import apoc.result.ImportProgressInfo;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

public class ImportJson {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure(value = "apoc.import.json", mode = Mode.WRITE)
    @Description("Imports a graph from the provided JSON file.")
    public Stream<ImportProgressInfo> all(
            @Name(
                            value = "urlOrBinaryFile",
                            description = "The name of the file or binary data to import the data from.")
                    Object urlOrBinaryFile,
            @Name(
                            value = "config",
                            defaultValue = "{}",
                            description =
                                    """
                    {
                        unwindBatchSize = 5000 :: INTEGER,
                        txBatchSize = 5000 :: INTEGER,
                        importIdName = "neo4jImportId" :: STRING,
                        nodePropertyMappings = {} :: MAP,
                        relPropertyMappings = {} :: MAP,
                        compression = "NONE" :: ["NONE", "BYTES", "GZIP", "BZIP2", "DEFLATE", "BLOCK_LZ4", "FRAMED_SNAPPY"],
                        cleanup = false :: BOOLEAN,
                        nodePropFilter = {} :: MAP,
                        relPropFilter = {} :: MAP
                    }
                    """)
                    Map<String, Object> config) {
        ImportProgressInfo result = Util.inThread(pools, () -> {
            ImportJsonConfig importJsonConfig = new ImportJsonConfig(config);
            String file = null;
            String source = "binary";
            if (urlOrBinaryFile instanceof String) {
                file = (String) urlOrBinaryFile;
                source = "file";
            }
            ProgressReporter reporter = new ProgressReporter(null, null, new ImportProgressInfo(file, source, "json"));

            try (final CountingReader reader = FileUtils.readerFor(
                            urlOrBinaryFile, importJsonConfig.getCompressionAlgo(), urlAccessChecker);
                    final Scanner scanner = new Scanner(reader).useDelimiter("\n|\r");
                    JsonImporter jsonImporter = new JsonImporter(importJsonConfig, db, reporter)) {
                while (scanner.hasNext() && !Util.transactionIsTerminated(terminationGuard)) {
                    Map<String, Object> row = JsonUtil.OBJECT_MAPPER.readValue(scanner.nextLine(), Map.class);
                    jsonImporter.importRow(row);
                }
            }

            return (ImportProgressInfo) reporter.getTotal();
        });
        return Stream.of(result);
    }
}
