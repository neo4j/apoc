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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.google.testing.compile.CompilationRule;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.TypeElement;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.procedure.UserAggregationFunction;

public class UserAggregationFunctionSignatureVisitorTest {

    @Rule
    public CompilationRule compilationRule = new CompilationRule();

    ElementVisitor<String, Void> visitor;

    TypeElement typeElement;

    @Before
    public void prepare() {
        visitor = new SignatureVisitor(compilationRule.getElements(), mock(Messager.class));
        typeElement = compilationRule
                .getElements()
                .getTypeElement(UserAggregationFunctionSignatureVisitorTest.class.getName());
    }

    @Test
    public void gets_the_annotated_name_of_the_procedure() {
        Element method = findMemberByName(typeElement, "myFunction");

        String result = visitor.visit(method);

        assertThat(result).isEqualTo("my.func");
    }

    @Test
    public void gets_the_annotated_value_of_the_procedure() {
        Element method = findMemberByName(typeElement, "myFunction2");

        String result = visitor.visit(method);

        assertThat(result).isEqualTo("my.func2");
    }

    @Test
    public void gets_the_annotated_name_over_value() {
        Element method = findMemberByName(typeElement, "myFunction3");

        String result = visitor.visit(method);

        assertThat(result).isEqualTo("my.func3");
    }

    @Test
    public void gets_the_default_name_of_the_procedure() {
        Element method = findMemberByName(typeElement, "myDefaultNamedFunction");

        String result = visitor.visit(method);

        assertThat(result).isEqualTo("apoc.processor.myDefaultNamedFunction");
    }

    @UserAggregationFunction(name = "my.func")
    public static void myFunction() {}

    @UserAggregationFunction(value = "my.func2")
    public static void myFunction2() {}

    @UserAggregationFunction(name = "my.func3", value = "ignored")
    public static void myFunction3() {}

    @UserAggregationFunction
    public static void myDefaultNamedFunction() {}

    private Element findMemberByName(TypeElement typeElement, String name) {
        return compilationRule.getElements().getAllMembers(typeElement).stream()
                .filter(e -> e.getSimpleName().contentEquals(name))
                .findFirst()
                .get();
    }
}
