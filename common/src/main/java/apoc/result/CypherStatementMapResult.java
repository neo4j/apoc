package apoc.result;

import java.util.Collections;
import java.util.Map;
import org.neo4j.procedure.Description;

public class CypherStatementMapResult {
    public static final CypherStatementMapResult EMPTY = new CypherStatementMapResult(Collections.emptyMap());

    @Description("The result returned from the Cypher statement.")
    public final Map<String, Object> value;

    public static CypherStatementMapResult empty() {
        return EMPTY;
    }

    public CypherStatementMapResult(Map<String, Object> value) {
        this.value = value;
    }
}
