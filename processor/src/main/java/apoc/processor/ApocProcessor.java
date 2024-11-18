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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class ApocProcessor extends AbstractProcessor {

    private List<Map<String, List<QueryLanguage>>> procedureSignatures;

    private List<Map<String, List<QueryLanguage>>> userFunctionSignatures;

    private SignatureVisitor signatureVisitor;

    private ExtensionClassWriter extensionClassWriter;

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(Procedure.class.getName(), UserFunction.class.getName(), UserAggregationFunction.class.getName());
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        procedureSignatures = new ArrayList<>();
        userFunctionSignatures = new ArrayList<>();
        extensionClassWriter = new ExtensionClassWriter(processingEnv.getFiler());
        signatureVisitor = new SignatureVisitor(processingEnv.getElementUtils(), processingEnv.getMessager());
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {

        annotations.forEach(annotation -> extractSignature(annotation, roundEnv));

        List<String> procedureSignaturesCypher5 = new ArrayList<>();
        List<String> userFunctionSignaturesCypher5 = new ArrayList<>();
        List<String> procedureSignaturesCypher25 = new ArrayList<>();
        List<String> userFunctionSignaturesCypher25 = new ArrayList<>();

        separateKeysByQueryLanguage(procedureSignatures, procedureSignaturesCypher5, procedureSignaturesCypher25);
        separateKeysByQueryLanguage(
                userFunctionSignatures, userFunctionSignaturesCypher5, userFunctionSignaturesCypher25);

        if (roundEnv.processingOver()) {
            extensionClassWriter.write(
                    procedureSignaturesCypher5,
                    userFunctionSignaturesCypher5,
                    procedureSignaturesCypher25,
                    userFunctionSignaturesCypher25);
        }
        return false;
    }

    private void extractSignature(TypeElement annotation, RoundEnvironment roundEnv) {
        List<Map<String, List<QueryLanguage>>> signatures = accumulator(annotation);
        roundEnv.getElementsAnnotatedWith(annotation)
                .forEach(annotatedElement -> signatures.add(signatureVisitor.visit(annotatedElement)));
    }

    private List<Map<String, List<QueryLanguage>>> accumulator(TypeElement annotation) {
        if (annotation.getQualifiedName().contentEquals(Procedure.class.getName())) {
            return procedureSignatures;
        }
        return userFunctionSignatures;
    }

    public static void separateKeysByQueryLanguage(
            List<Map<String, List<QueryLanguage>>> list, List<String> c5Keys, List<String> c6Keys) {
        for (Map<String, List<QueryLanguage>> map : list) {
            for (Map.Entry<String, List<QueryLanguage>> entry : map.entrySet()) {
                String key = entry.getKey();
                List<QueryLanguage> values = entry.getValue();

                if (values.contains(QueryLanguage.CYPHER_5)) {
                    c5Keys.add(key);
                }
                if (values.contains(QueryLanguage.CYPHER_25)) {
                    c6Keys.add(key);
                }
            }
        }
    }
}
