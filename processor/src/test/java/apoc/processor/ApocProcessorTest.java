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
package apoc.processor;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.JavaFileObjects;
import javax.annotation.processing.Processor;
import org.junit.Rule;
import org.junit.Test;

public class ApocProcessorTest {

    @Rule
    public CompilationRule compilationRule = new CompilationRule();

    Processor apocProcessor = new ApocProcessor();

    @Test
    public void generates_signatures() {
        assert_()
                .about(javaSource())
                .that(
                        JavaFileObjects.forSourceLines(
                                "my.ApocProcedure",
                                """
                                package my;

                                import org.neo4j.kernel.api.QueryLanguage;
                                import org.neo4j.kernel.api.procedure.QueryLanguageScope;
                                import org.neo4j.procedure.Description;
                                import org.neo4j.procedure.Name;
                                import org.neo4j.procedure.Procedure;
                                import org.neo4j.procedure.UserFunction;

                                import java.util.List;
                                import java.util.stream.Stream;

                                class ApocProcedure {

                                    @Procedure(name = "apoc.nodes")
                                    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
                                    @Description("Quickly returns all nodes with these id's")
                                    public Stream<NodeResult> nodesCypher5(@Name("nodes") Object ids) {
                                        return Stream.empty();
                                    }

                                    @Procedure(name = "apoc.nodes")
                                    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
                                    @Description("apoc.nodes(node|id|[ids]) - quickly returns all nodes with these id's")
                                    public Stream<NodeResult> nodesCypher25(@Name("nodes") Object ids) {
                                        return Stream.empty();
                                    }

                                    @Procedure(name = "apoc.oldNodes")
                                    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
                                    @Description("apoc.nodes(node|id|[ids]) - quickly returns all nodes with these id's")
                                    public Stream<NodeResult> oldNodes(@Name("nodes") Object ids) {
                                        return Stream.empty();
                                    }

                                    @Procedure(name = "apoc.newNodes")
                                    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
                                    @Description("apoc.nodes(node|id|[ids]) - quickly returns all nodes with these id's")
                                    public Stream<NodeResult> newNodes(@Name("nodes") Object ids) {
                                        return Stream.empty();
                                    }

                                    @UserFunction(name = "my.join")
                                    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
                                    public String joinCypher5(@Name("words") List<String> words, @Name("separator") String separator) {
                                        return String.join(separator, words);
                                    }

                                    @UserFunction(name = "my.join")
                                    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
                                    public String joinCypher25(@Name("words") List<String> words, @Name("separator") String separator) {
                                        return String.join(separator, words);
                                    }

                                    @UserFunction(name = "apoc.sum")
                                    public int sum(@Name("a") int a, @Name("b") int b) {
                                        return a + b;
                                    }
                                }

                                class NodeResult {}
                                """))
                .processedWith(apocProcessor)
                .compilesWithoutError()
                .and()
                .generatesSources(
                        JavaFileObjects.forSourceLines(
                                "apoc.ApocSignatures",
                                """
                                package apoc;

                                import java.lang.String;
                                import java.util.List;

                                public class ApocSignatures {
                                  public static final List<String> PROCEDURES_CYPHER_5 = List.of("apoc.nodes", "apoc.oldNodes");

                                  public static final List<String> FUNCTIONS_CYPHER_5 = List.of("my.join", "apoc.sum");

                                  public static final List<String> PROCEDURES_CYPHER_25 = List.of("apoc.nodes", "apoc.newNodes");

                                  public static final List<String> FUNCTIONS_CYPHER_25 = List.of("my.join", "apoc.sum");
                                }
                                """));
    }
}
