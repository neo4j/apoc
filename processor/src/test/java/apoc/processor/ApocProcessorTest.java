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
                .that(JavaFileObjects.forSourceLines(
                        "my.ApocProcedure",
                        "package my;\n" + "\n"
                                + "import org.neo4j.procedure.Description;\n"
                                + "import org.neo4j.procedure.Name;\n"
                                + "import org.neo4j.procedure.Procedure;\n"
                                + "import org.neo4j.procedure.UserFunction;\n"
                                + "\n"
                                + "import java.util.List;\n"
                                + "import java.util.stream.Stream;\n"
                                + "\n"
                                + ""
                                + "class ApocProcedure {\n"
                                + "\n"
                                + "    @Procedure(name = \"apoc.nodes\")\n"
                                + "    @Description(\"apoc.nodes(node|id|[ids]) - quickly returns all nodes with these id's\")\n"
                                + "    public Stream<NodeResult> nodes(@Name(\"nodes\") Object ids) {\n"
                                + "        return Stream.empty();\n"
                                + "    }\n"
                                + "\n"
                                + "    "
                                + "@UserFunction\n"
                                + "    public String join(@Name(\"words\") List<String> words, @Name(\"separator\") String separator) {\n"
                                + "        return String.join(separator, words);\n"
                                + "    }\n"
                                + "    \n"
                                + "    @UserFunction(name = \"apoc.sum\")\n"
                                + "    public int sum(@Name(\"a\") int a, @Name(\"b\") int b) {\n"
                                + "        return a + b;\n"
                                + "    }\n"
                                + "}\n"
                                + "\n"
                                + "class NodeResult {\n"
                                + "    \n"
                                + "}"))
                .processedWith(apocProcessor)
                .compilesWithoutError()
                .and()
                .generatesSources(JavaFileObjects.forSourceLines(
                        "apoc.ApocSignatures",
                        "package apoc;\n" + "\n"
                                + "import java.lang.String;\n"
                                + "import java.util.List;\n"
                                + "\n"
                                + "public class ApocSignatures {\n"
                                + "  public static final List<String> PROCEDURES = List.of(\"apoc.nodes\");\n"
                                + "  ;\n"
                                + "\n"
                                + "  public static final List<String> FUNCTIONS = List.of(\"my.join\",\n"
                                + "          \"apoc.sum\");\n"
                                + "  ;\n"
                                + "}"));
    }
}
