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
package apoc.export.csv;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ImportProgressInfo;
import apoc.util.Util;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class ImportCsv {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    @Context
    public URLAccessChecker urlAccessChecker;

    @Procedure(name = "apoc.import.csv", mode = Mode.SCHEMA)
    @Description("Imports `NODE` and `RELATIONSHIP` values with the given labels and types from the provided CSV file.")
    public Stream<ImportProgressInfo> importCsv(
            @Name(
                            value = "nodes",
                            description =
                                    "List of map values of where to import the node values from; { fileName :: STRING, data :: BYTEARRAY, labels :: LIST<STRING> }.")
                    List<Map<String, Object>> nodes,
            @Name(
                            value = "rels",
                            description =
                                    "List of map values specifying where to import relationship values from: { fileName :: STRING, data :: BYTEARRAY, type :: STRING }.")
                    List<Map<String, Object>> relationships,
            @Name(
                            value = "config",
                            description =
                                    """
                    {
                        delimiter = "," :: STRING,
                        arrayDelimiter = ";" :: STRING,
                        ignoreDuplicateNodes = false :: BOOLEAN,
                        quotationCharacter = "\"" :: STRING,
                        stringIds = true :: BOOLEAN,
                        skipLines = 1 :: INTEGER,
                        ignoreBlankString = false :: BOOLEAN,
                        ignoreEmptyCellArray = false :: BOOLEAN,
                        compression = "NONE" :: ["NONE", "BYTES", "GZIP", "BZIP2", "DEFLATE", "BLOCK_LZ4", "FRAMED_SNAPPY"],
                        charset = "UTF-8" :: STRING,
                        batchSize = 2000 :: INTEGER
                    }
                    """)
                    Map<String, Object> config) {
        ImportProgressInfo result = Util.inThread(pools, () -> {
            String file = "progress.csv";
            String source = "file";
            if (nodes.stream().anyMatch(node -> node.containsKey("data"))) {
                file = null;
                source = "file/binary";
            }
            final CsvLoaderConfig clc = CsvLoaderConfig.from(config);
            final ProgressReporter reporter =
                    new ProgressReporter(null, null, new ImportProgressInfo(file, source, "csv"));
            final CsvEntityLoader loader = new CsvEntityLoader(clc, reporter, log, urlAccessChecker);

            final Map<String, Map<String, String>> idMapping = new HashMap<>();
            for (Map<String, Object> node : nodes) {
                final Object fileName = node.getOrDefault("fileName", node.get("data"));
                final List<String> labels = (List<String>) node.get("labels");
                loader.loadNodes(fileName, labels, db, idMapping);
            }

            for (Map<String, Object> relationship : relationships) {
                final Object fileName = relationship.getOrDefault("fileName", relationship.get("data"));
                final String type = (String) relationship.get("type");
                loader.loadRelationships(fileName, type, db, idMapping);
            }

            return (ImportProgressInfo) reporter.getTotal();
        });
        return Stream.of(result);
    }
}
