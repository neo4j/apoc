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

import static apoc.cypher.CypherUtils.withParamMapping;

import apoc.util.Util;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.UserFunction;

/**
 * Created by lyonwj on 9/29/17.
 */
public class CypherFunctions {
    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    public Object runFirstColumn(String statement, Map<String, Object> params, boolean expectMultipleValues) {
        if (params == null) params = Collections.emptyMap();
        String resolvedStatement = withParamMapping(statement, params.keySet());
        resolvedStatement = Util.slottedRuntime(resolvedStatement, Util.getCypherVersionString(procedureCallContext));
        try (Result result = tx.execute(resolvedStatement, params)) {

            String firstColumn = result.columns().get(0);
            try (ResourceIterator<Object> iter = result.columnAs(firstColumn)) {
                if (expectMultipleValues) return iter.stream().collect(Collectors.toList());
                return iter.hasNext() ? iter.next() : null;
            }
        }
    }

    @NotThreadSafe
    @UserFunction("apoc.cypher.runFirstColumnMany")
    @Description(
            "Runs the given statement with the given parameters and returns the first column collected into a `LIST<ANY>`.")
    public List<Object> runFirstColumnMany(
            @Name(value = "statement", description = "The Cypher query to execute.") String statement,
            @Name(value = "params", description = "The parameters needed for input to the given Cypher query.")
                    Map<String, Object> params) {
        return (List) runFirstColumn(statement, params, true);
    }

    @NotThreadSafe
    @UserFunction("apoc.cypher.runFirstColumnSingle")
    @Description(
            "Runs the given statement with the given parameters and returns the first element of the first column.")
    public Object runFirstColumnSingle(
            @Name(value = "statement", description = "The Cypher query to execute.") String statement,
            @Name(value = "params", description = "The parameters needed for input to the given Cypher query.")
                    Map<String, Object> params) {
        return runFirstColumn(statement, params, false);
    }
}
