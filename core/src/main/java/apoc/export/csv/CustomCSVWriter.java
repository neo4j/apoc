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

import com.opencsv.CSVWriter;
import java.io.Writer;

public class CustomCSVWriter extends CSVWriter {

    private boolean shouldDifferentiateNulls;

    public CustomCSVWriter(
            Writer writer,
            char separator,
            char quoteChar,
            char escapeChar,
            String lineEnd,
            boolean shouldDifferentiateNulls) {
        super(writer, separator, quoteChar, escapeChar, lineEnd);
        this.shouldDifferentiateNulls = shouldDifferentiateNulls;
    }

    /**
     * We have overridden the openCSV writer so that empty strings are also counted as special.
     * This is so quotes will be added to them and not to null values if shouldDifferentiateNulls is true.
     */
    @Override
    protected boolean stringContainsSpecialCharacters(String line) {
        return line.indexOf(this.quotechar) != -1
                || line.indexOf(this.escapechar) != -1
                || line.indexOf(this.separator) != -1
                || line.contains("\n")
                || line.contains("\r")
                || (line.isEmpty() && shouldDifferentiateNulls);
    }
}
