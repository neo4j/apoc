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

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import org.neo4j.kernel.api.QueryLanguage;

public class ExtensionClassWriter {

    private final Filer filer;

    public ExtensionClassWriter(Filer filer) {
        this.filer = filer;
    }

    public void write(List<SignatureVisitor.Signature> signatures) {
        try {
            final var suffix = isExtendedProject() ? "Extended" : "";
            JavaFile.builder("apoc", defineClass(signatures, suffix)).build().writeTo(filer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isExtendedProject() throws IOException {
        // create and delete a file to retrieve the current project path (e.g `ROOT/../core/build/generated/.../tmp`)
        FileObject resource = filer.createResource(StandardLocation.SOURCE_OUTPUT, "", "tmp", (Element[]) null);
        String projectPath = resource.getName();
        resource.delete();

        return projectPath.contains("extended/build/generated");
    }

    private TypeSpec defineClass(List<SignatureVisitor.Signature> signatures, String suffix) {
        return TypeSpec.classBuilder("ApocSignatures" + suffix)
                .addModifiers(Modifier.PUBLIC)
                .addField(signatureListField("PROCEDURES_CYPHER_5", names(signatures, true, QueryLanguage.CYPHER_5)))
                .addField(signatureListField("FUNCTIONS_CYPHER_5", names(signatures, false, QueryLanguage.CYPHER_5)))
                .addField(signatureListField("PROCEDURES_CYPHER_25", names(signatures, true, QueryLanguage.CYPHER_25)))
                .addField(signatureListField("FUNCTIONS_CYPHER_25", names(signatures, false, QueryLanguage.CYPHER_25)))
                .build();
    }

    private String[] names(List<SignatureVisitor.Signature> signatures, boolean procedure, QueryLanguage lang) {
        return signatures.stream()
                .filter(s -> s.isProcedure() == procedure)
                .filter(s -> s.scope().contains(lang))
                .map(SignatureVisitor.Signature::name)
                .toArray(String[]::new);
    }

    private FieldSpec signatureListField(String fieldName, String[] signatures) {
        ParameterizedTypeName fieldType =
                ParameterizedTypeName.get(ClassName.get(List.class), ClassName.get(String.class));
        return FieldSpec.builder(fieldType, fieldName, Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer(CodeBlock.builder()
                        .addStatement(String.format("List.of(%s)", placeholders(signatures)), (Object[]) signatures)
                        .build())
                .build();
    }

    private String placeholders(String[] signatures) {
        // FIXME: find a way to manage the indentation automatically
        return Arrays.stream(signatures).map((ignored) -> "$S").collect(Collectors.joining(",\n\t\t"));
    }
}
