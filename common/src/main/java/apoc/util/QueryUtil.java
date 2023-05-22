package apoc.util;

import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser;
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory;

public class QueryUtil {
    private static OpenCypherExceptionFactory exceptionFactory = new OpenCypherExceptionFactory(scala.Option.empty());

    public static boolean isValidQuery(String query) {
        try {
            JavaCCParser.parse(query, exceptionFactory);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
