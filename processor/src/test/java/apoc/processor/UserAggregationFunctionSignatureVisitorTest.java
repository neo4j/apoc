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
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.QueryLanguage.CYPHER_25;
import static org.neo4j.kernel.api.QueryLanguage.CYPHER_5;

import java.util.List;
import java.util.Map;
import javax.annotation.processing.Messager;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.PackageElement;
import javax.lang.model.util.Elements;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.procedure.UserAggregationFunction;

public class UserAggregationFunctionSignatureVisitorTest {

    private Elements elements;
    ElementVisitor<Map<String, List<QueryLanguage>>, Void> visitor;

    @BeforeEach
    void prepare() {
        elements = mock(Elements.class);
        visitor = new SignatureVisitor(elements, mock(Messager.class));
    }

    @Test
    void gets_the_annotated_name_of_the_procedure() {
        ExecutableElement method = methodWithUserAggregationFunction("my.func", "", "ignoredName");

        Map<String, List<QueryLanguage>> result = visitor.visitExecutable(method, null);

        assertThat(result).isEqualTo(Map.of("my.func", List.of(CYPHER_5, CYPHER_25)));
    }

    @Test
    void gets_the_annotated_value_of_the_procedure() {
        ExecutableElement method = methodWithUserAggregationFunction("", "my.func2", "ignoredName");

        Map<String, List<QueryLanguage>> result = visitor.visitExecutable(method, null);

        assertThat(result).isEqualTo(Map.of("my.func2", List.of(CYPHER_5, CYPHER_25)));
    }

    @Test
    void gets_the_annotated_name_over_value() {
        ExecutableElement method = methodWithUserAggregationFunction("my.func3", "ignored", "ignoredName");

        Map<String, List<QueryLanguage>> result = visitor.visitExecutable(method, null);

        assertThat(result).isEqualTo(Map.of("my.func3", List.of(CYPHER_5, CYPHER_25)));
    }

    @Test
    void gets_the_default_name_of_the_procedure() {
        String pkg = "apoc.processor";
        String methodName = "myDefaultNamedFunction";
        ExecutableElement method = methodWithoutUserAggregationFunction(pkg, methodName);

        Map<String, List<QueryLanguage>> result = visitor.visitExecutable(method, null);

        assertThat(result).isEqualTo(Map.of(pkg + "." + methodName, List.of(CYPHER_5, CYPHER_25)));
    }

    private ExecutableElement methodWithUserAggregationFunction(String name, String value, String methodName) {
        ExecutableElement method = mock(ExecutableElement.class);
        // mock annotation
        UserAggregationFunction annotation = mock(UserAggregationFunction.class);
        when(annotation.name()).thenReturn(name);
        when(annotation.value()).thenReturn(value);
        when(method.getAnnotation(UserAggregationFunction.class)).thenReturn(annotation);
        // simple name not used for annotated cases, but set anyway
        Name simpleName = mock(Name.class);
        when(simpleName.toString()).thenReturn(methodName);
        when(method.getSimpleName()).thenReturn(simpleName);
        return method;
    }

    private ExecutableElement methodWithoutUserAggregationFunction(String packageName, String methodName) {
        ExecutableElement method = mock(ExecutableElement.class);
        when(method.getAnnotation(UserAggregationFunction.class)).thenReturn(null);
        Name simpleName = mock(Name.class);
        when(simpleName.toString()).thenReturn(methodName);
        when(method.getSimpleName()).thenReturn(simpleName);

        PackageElement pkg = mock(PackageElement.class);
        when(pkg.toString()).thenReturn(packageName);
        when(elements.getPackageOf(method)).thenReturn(pkg);
        return method;
    }
}
