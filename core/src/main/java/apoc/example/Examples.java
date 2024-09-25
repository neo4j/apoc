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
package apoc.example;

import apoc.util.Util;
import java.util.stream.Stream;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;

public class Examples {

    @Context
    public Transaction tx;

    public static class ExamplesProgressInfo {
        @Description("The name of the file containing the movies example.")
        public final String file;

        @Description("Where the examples were sourced from.")
        public String source;

        @Description("The format the movies file was in.")
        public final String format;

        @Description("The number of nodes imported.")
        public long nodes;

        @Description("The number of relationships imported.")
        public long relationships;

        @Description("The number of properties imported.")
        public long properties;

        @Description("The duration of the import.")
        public long time;

        @Description("The number of rows returned.")
        public long rows;

        @Description("The size of the batches the import was run in.")
        public long batchSize = -1;

        @Description("The number of batches the import was run in.")
        public long batches;

        @Description("Whether the import ran successfully.")
        public boolean done;

        @Description("The data returned by the import.")
        public Object data;

        public ExamplesProgressInfo(long nodes, long relationships, long properties, long time) {
            this.file = "movies.cypher";
            this.source = "example movie database from themoviedb.org";
            this.format = "cypher";
            this.nodes = nodes;
            this.relationships = relationships;
            this.properties = properties;
            this.time = time;
            this.done = true;
        }
    }

    @NotThreadSafe
    @Procedure(name = "apoc.example.movies", mode = Mode.WRITE)
    @Description("Seeds the database with the Neo4j movie dataset.")
    public Stream<ExamplesProgressInfo> movies() {
        long start = System.currentTimeMillis();
        String file = "movies.cypher";
        Result result = tx.execute(Util.readResourceFile(file));
        QueryStatistics stats = result.getQueryStatistics();
        ExamplesProgressInfo progress = new ExamplesProgressInfo(
                stats.getNodesCreated(),
                stats.getRelationshipsCreated(),
                stats.getPropertiesSet(),
                System.currentTimeMillis() - start);
        result.close();
        return Stream.of(progress);
    }
}
