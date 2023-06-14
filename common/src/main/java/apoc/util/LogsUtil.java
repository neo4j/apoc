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
