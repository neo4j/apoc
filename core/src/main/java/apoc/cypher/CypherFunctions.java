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
package apoc.cypher;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.cypher.CypherUtils.withParamMapping;

/**
 * Created by lyonwj on 9/29/17.
 */
public class CypherFunctions {
    @Context
    public Transaction tx;

    public Object runFirstColumn(String statement, Map<String, Object> params, boolean expectMultipleValues) {
        if (params == null) params = Collections.emptyMap();
        String resolvedStatement = withParamMapping(statement, params.keySet());
        if (!resolvedStatement.contains(" runtime")) resolvedStatement = "cypher runtime=slotted " + resolvedStatement;
        try (Result result = tx.execute(resolvedStatement, params)) {

        String firstColumn = result.columns().get(0);
        try (ResourceIterator<Object> iter = result.columnAs(firstColumn)) {
            if (expectMultipleValues) return iter.stream().collect(Collectors.toList());
            return iter.hasNext() ? iter.next() : null;
        }
      }
    }

    @UserFunction("apoc.cypher.runFirstColumnMany")
    @Description("Runs the given statement with the given parameters and returns the first column collected into a list.")
    public List<Object> runFirstColumnMany(@Name("statement") String statement, @Name("params") Map<String, Object> params) {
        return (List)runFirstColumn(statement, params, true);
    }
    @UserFunction("apoc.cypher.runFirstColumnSingle")
    @Description("Runs the given statement with the given parameters and returns the first element of the first column.")
    public Object runFirstColumnSingle(@Name("statement") String statement, @Name("params") Map<String, Object> params) {
        return runFirstColumn(statement, params, false);
    }
}
