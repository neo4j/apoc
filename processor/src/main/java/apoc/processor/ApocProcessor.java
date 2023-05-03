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

import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@SupportedSourceVersion(SourceVersion.RELEASE_17)
public class ApocProcessor extends AbstractProcessor {

    private List<String> procedureSignatures;

    private List<String> userFunctionSignatures;

    private SignatureVisitor signatureVisitor;

    private ExtensionClassWriter extensionClassWriter;

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(
                Procedure.class.getName(),
                UserFunction.class.getName(),
                UserAggregationFunction.class.getName()
        );
    }


    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        procedureSignatures = new ArrayList<>();
        userFunctionSignatures = new ArrayList<>();
        extensionClassWriter = new ExtensionClassWriter(processingEnv.getFiler());
        signatureVisitor = new SignatureVisitor(
                processingEnv.getElementUtils(),
                processingEnv.getMessager()
        );
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations,
                           RoundEnvironment roundEnv) {

        annotations.forEach(annotation -> extractSignature(annotation, roundEnv));

        if (roundEnv.processingOver()) {
            extensionClassWriter.write(procedureSignatures, userFunctionSignatures);
        }
        return false;
    }

    private void extractSignature(TypeElement annotation, RoundEnvironment roundEnv) {
        List<String> signatures = accumulator(annotation);
        roundEnv.getElementsAnnotatedWith(annotation)
                .forEach(annotatedElement -> signatures.add(signatureVisitor.visit(annotatedElement)));
    }

    private List<String> accumulator(TypeElement annotation) {
        if (annotation.getQualifiedName().contentEquals(Procedure.class.getName())) {
            return procedureSignatures;
        }
        return userFunctionSignatures;
    }

}
