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
package apoc.help;

import static apoc.util.Util.map;

import apoc.util.Util;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;

public class Help {

    @Context
    public Transaction tx;

    @Context
    public ProcedureCallContext procedureCallContext;

    @NotThreadSafe
    @Procedure("apoc.help")
    @Description(
            "Returns descriptions of the available APOC procedures and functions. If a keyword is provided, it will return only those procedures and functions that have the keyword in their name.")
    public Stream<HelpResult> infoCypher25(
            @Name(value = "proc", description = "A keyword to filter the results by.") String name) {
        boolean searchText = false;
        if (name != null) {
            name = name.trim();
            if (name.endsWith("+")) {
                name = name.substring(0, name.lastIndexOf('+')).trim();
                searchText = true;
            }
        }
        String filter =
                " WHERE name starts with 'apoc.' " + " AND ($name IS NULL  OR toLower(name) CONTAINS toLower($name) "
                        + " OR ($desc IS NOT NULL AND toLower(description) CONTAINS toLower($desc))) ";

        String proceduresQuery = Util.prefixQuery(
                procedureCallContext,
                "SHOW PROCEDURES yield name, description, signature, isDeprecated " + filter
                        + "RETURN 'procedure' as type, name, description, signature, isDeprecated ");

        String functionsQuery = Util.prefixQuery(
                procedureCallContext,
                "SHOW FUNCTIONS yield name, description, signature, isDeprecated " + filter
                        + "RETURN 'function' as type, name, description, signature, isDeprecated ");
        Map<String, Object> params = map("name", name, "desc", searchText ? name : null);
        Stream<Map<String, Object>> proceduresResults = tx.execute(proceduresQuery, params).stream();
        Stream<Map<String, Object>> functionsResults = tx.execute(functionsQuery, params).stream();

        return Stream.of(proceduresResults, functionsResults)
                .flatMap(results -> results.map(row -> new HelpResult(
                        row,
                        existsInCore(
                                (String) row.get("name"),
                                procedureCallContext.calledwithQueryLanguage().equals(QueryLanguage.CYPHER_5),
                                row.get("type").equals("function")))));
    }

    private boolean existsInCore(String name, boolean version5, boolean function) {
        if (version5) {
            if (function) return HelpUtil.coreFunctionsV5.contains(name);
            else return HelpUtil.coreProceduresV5.contains(name);
        } else {
            if (function) return HelpUtil.coreFunctionsV25.contains(name);
            else return HelpUtil.coreProceduresV25.contains(name);
        }
    }
}
