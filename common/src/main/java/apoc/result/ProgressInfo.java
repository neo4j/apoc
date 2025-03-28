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
package apoc.result;

import apoc.export.util.ExportConfig;
import java.io.StringWriter;

public interface ProgressInfo {

    ProgressInfo EMPTY = null;

    ProgressInfo update(long nodes, long relationships, long properties);

    ProgressInfo updateTime(long start);

    ProgressInfo done(long start);

    void nextRow();

    ProgressInfo drain(StringWriter writer, ExportConfig config);

    void setBatches(long batches);

    void setRows(long rows);

    long getBatchSize();
}
