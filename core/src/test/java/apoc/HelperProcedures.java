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
