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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.SimpleElementVisitor9;
import javax.tools.Diagnostic;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

public class SignatureVisitor extends SimpleElementVisitor9<SignatureVisitor.Signature, Void> {

    private final Elements elementUtils;

    private final Messager messager;

    public SignatureVisitor(Elements elementUtils, Messager messager) {
        this.elementUtils = elementUtils;
        this.messager = messager;
    }

    @Override
    public Signature visitExecutable(ExecutableElement method, Void unused) {
        final var isProcedure = method.getAnnotation(Procedure.class) != null;
        final var className =
                ((TypeElement) method.getEnclosingElement()).getQualifiedName().toString();
        final var name = getProcedureName(method)
                .or(() -> getUserFunctionName(method))
                .or(() -> getUserAggregationFunctionName(method))
                .orElse("%s.%s".formatted(elementUtils.getPackageOf(method), method.getSimpleName()));
        return new Signature(name, isProcedure, cypherScopes(method), className);
    }

    @Override
    public Signature visitUnknown(Element e, Void unused) {
        messager.printMessage(Diagnostic.Kind.ERROR, "unexpected .....");
        return super.visitUnknown(e, unused);
    }

    private Set<QueryLanguage> cypherScopes(ExecutableElement method) {
        final var annotation = method.getAnnotation(QueryLanguageScope.class);
        if (annotation != null && annotation.scope().length > 0) {
            return EnumSet.copyOf(Arrays.asList(annotation.scope()));
        } else {
            return QueryLanguage.ALL;
        }
    }

    private Optional<String> getProcedureName(ExecutableElement method) {
        return Optional.ofNullable(method.getAnnotation(Procedure.class))
                .flatMap((annotation) -> pickFirstNonBlank(annotation.name(), annotation.value()));
    }

    private Optional<String> getUserFunctionName(ExecutableElement method) {
        return Optional.ofNullable(method.getAnnotation(UserFunction.class))
                .flatMap((annotation) -> pickFirstNonBlank(annotation.name(), annotation.value()));
    }

    private Optional<String> getUserAggregationFunctionName(ExecutableElement method) {
        return Optional.ofNullable(method.getAnnotation(UserAggregationFunction.class))
                .flatMap((annotation) -> pickFirstNonBlank(annotation.name(), annotation.value()));
    }

    private Optional<String> pickFirstNonBlank(String name, String value) {
        if (!name.isBlank()) return Optional.of(name);
        else if (!value.isBlank()) return Optional.of(value);
        else return Optional.empty();
    }

    public record Signature(String name, boolean isProcedure, Set<QueryLanguage> scope, String className) {}
}
