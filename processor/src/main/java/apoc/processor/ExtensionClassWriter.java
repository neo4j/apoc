package apoc.processor;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.processing.Filer;
import javax.lang.model.element.Element;
import javax.lang.model.element.Modifier;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ExtensionClassWriter {

    private final Filer filer;

    public ExtensionClassWriter(Filer filer) {
        this.filer = filer;
    }

    public void write(List<String> procedureSignatures,
                      List<String> userFunctionSignatures) {

        try {
            String path = getProjectPath();
            final TypeSpec typeSpec = defineClass(procedureSignatures, userFunctionSignatures, path);

            JavaFile.builder("apoc", typeSpec)
                    .build()
                    .writeTo(filer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getProjectPath() throws IOException {
        // create and delete a file to retrieve the current project (`core` or `extended`)
        FileObject resource = filer.createResource(StandardLocation.SOURCE_OUTPUT, "", "tmp", (Element[]) null);
        String projectPath = resource.getName();
        resource.delete();

        // in order to ignore test case, i.e. `ApocProcessorTest`
        String buildPath = "/build";
        if (!projectPath.contains(buildPath)) {
            return "";
        }

        // transform e.g. "myPath/core/build/generated/..."   to "Core"
        projectPath = StringUtils.substringBefore(projectPath, buildPath);
        projectPath = StringUtils.substringAfterLast(projectPath, "/");
        return StringUtils.capitalize(projectPath);
    }

    private TypeSpec defineClass(List<String> procedureSignatures, List<String> userFunctionSignatures, String projectPath) {
        return TypeSpec.classBuilder("ApocSignatures" + projectPath)
                .addModifiers(Modifier.PUBLIC)
                .addField(signatureListField("PROCEDURES", procedureSignatures))
                .addField(signatureListField("FUNCTIONS", userFunctionSignatures))
                .build();
    }

    private FieldSpec signatureListField(String fieldName, List<String> signatures) {
        ParameterizedTypeName fieldType = ParameterizedTypeName.get(
                ClassName.get(List.class),
                ClassName.get(String.class)
        );
        return FieldSpec.builder(fieldType, fieldName, Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer(CodeBlock.builder()
                        .addStatement(String.format("List.of(%s)", placeholders(signatures)), signatures.toArray())
                        .build())
                .build();
    }

    private String placeholders(List<String> signatures) {
        // FIXME: find a way to manage the indentation automatically
        return signatures.stream().map((ignored) -> "$S").collect(Collectors.joining(",\n\t\t"));
    }
}