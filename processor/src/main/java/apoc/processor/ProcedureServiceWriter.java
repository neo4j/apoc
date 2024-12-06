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

import static javax.tools.StandardLocation.CLASS_OUTPUT;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.processing.Filer;
import org.neo4j.procedure.Procedure;

public class ProcedureServiceWriter {
    private final Filer filer;

    public ProcedureServiceWriter(Filer filer) {
        this.filer = filer;
    }

    public void write(List<SignatureVisitor.Signature> signatures) {
        final var classNames = signatures.stream()
                .map(SignatureVisitor.Signature::className)
                .distinct()
                .sorted()
                .toList();
        try {
            writeProcedureClasses(classNames);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeProcedureClasses(Iterable<String> classNames) throws IOException {
        final var path = "META-INF/services/" + Procedure.class.getCanonicalName();
        var file = filer.createResource(CLASS_OUTPUT, "", path);

        try (var writer =
                new PrintWriter(new BufferedOutputStream(file.openOutputStream()), true, StandardCharsets.UTF_8)) {
            for (final var name : classNames) writer.println(name);
        }
    }
}
