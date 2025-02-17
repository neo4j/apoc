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
package apoc;

import java.util.List;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.UserFunction;

public class HelperProcedures {

    @Context
    public ProcedureCallContext procedureCallContext;

    public static class CypherVersionCombinations {
        public String outerVersion;
        public String innerVersion;
        public String result;

        public CypherVersionCombinations(String outerVersion, String innerVersion, String result) {
            this.outerVersion = outerVersion;
            this.innerVersion = innerVersion;
            this.result = result;
        }
    }

    public static final List<CypherVersionCombinations> cypherVersions = List.of(
            new CypherVersionCombinations("CYPHER 5", "", "CYPHER_5"),
            new CypherVersionCombinations("CYPHER 5", "CYPHER 5", "CYPHER_5"),
            new CypherVersionCombinations("CYPHER 5", "CYPHER 25", "CYPHER_25"),
            new CypherVersionCombinations("CYPHER 25", "", "CYPHER_25"),
            new CypherVersionCombinations("CYPHER 25", "CYPHER 25", "CYPHER_25"),
            new CypherVersionCombinations("CYPHER 25", "CYPHER 5", "CYPHER_5"));

    @UserFunction(name = "apoc.cypherVersion")
    @Description("This test function returns a string of the Cypher Version that is was called with.")
    public String cypherVersion() {
        return procedureCallContext.calledwithQueryLanguage().name();
    }
}
