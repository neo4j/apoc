package apoc.util;

import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser;
import org.neo4j.cypher.internal.ast.prettifier.DefaultExpressionStringifier;
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier;
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier$;
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory;
import org.neo4j.cypher.internal.ast.prettifier.Prettifier;
import org.neo4j.cypher.internal.rewriting.rewriters.sensitiveLiteralReplacement;

public class LogsUtil {
    private static OpenCypherExceptionFactory exceptionFactory = new OpenCypherExceptionFactory(scala.Option.empty());
    private static ExpressionStringifier.Extension extension = ExpressionStringifier.Extension$.MODULE$.simple((ExpressionStringifier$.MODULE$.failingExtender()));
    private static ExpressionStringifier stringifier = new DefaultExpressionStringifier(extension, false, false, false, false);
    private static Prettifier prettifier = new Prettifier(stringifier, Prettifier.EmptyExtension$.MODULE$, true);


    public static String sanitizeQuery(String query) {
        try {
            var statement = JavaCCParser.parse(query, exceptionFactory);
            var rewriter = sensitiveLiteralReplacement.apply(statement)._1;
            var res = (Statement) rewriter.apply(statement);

            return prettifier.asString(res);
        } catch (Exception e) {
            return query;
        }
    }
}
