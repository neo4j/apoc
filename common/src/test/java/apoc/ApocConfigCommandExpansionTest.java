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
package apoc;

import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_RATIO;
import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

public class ApocConfigCommandExpansionTest {

    private ApocConfig apocConfig;
    private File apocConfigCommandExpansionFile;
    private static final Set<PosixFilePermission> permittedFilePermissionsForCommandExpansion =
            Set.of(OWNER_READ, OWNER_WRITE, GROUP_READ);
    private static final Set<PosixFilePermission> forbiddenFilePermissionsForCommandExpansion =
            Set.of(OWNER_EXECUTE, GROUP_WRITE, GROUP_EXECUTE, OTHERS_READ, OTHERS_WRITE, OTHERS_EXECUTE);

    @Before
    public void setup() throws Exception {
        InternalLogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.get(GraphDatabaseSettings.allow_file_urls)).thenReturn(false);
        when(neo4jConfig.expandCommands()).thenReturn(true);

        apocConfigCommandExpansionFile = new File(getClass()
                .getClassLoader()
                .getResource("apoc-config-command-expansion/apoc.conf")
                .toURI());
        Files.setPosixFilePermissions(
                apocConfigCommandExpansionFile.toPath(), permittedFilePermissionsForCommandExpansion);

        when(neo4jConfig.get(GraphDatabaseSettings.configuration_directory))
                .thenReturn(Path.of(apocConfigCommandExpansionFile.getParent()));

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        apocConfig =
                new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);
    }

    private void setApocConfigFilePermissions(Set<PosixFilePermission> forbidden) throws Exception {
        Files.setPosixFilePermissions(
                apocConfigCommandExpansionFile.toPath(),
                Sets.union(permittedFilePermissionsForCommandExpansion, forbidden));
    }

    @Test
    public void testApocConfWithExpandCommands() {
        apocConfig.init();

        assertEquals("expanded value", apocConfig.getConfig().getString("command.expansion"));
    }

    @Test
    public void testApocConfWithInvalidExpandCommands() throws Exception {
        String invalidExpandLine = "command.expansion.3=$(fakeCommand 3 + 3)";
        addLineToApocConfig(invalidExpandLine);

        RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
        String expectedMessage =
                "java.io.IOException: Cannot run program \"fakeCommand\": error=2, No such file or directory";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);

        removeLineFromApocConfig(invalidExpandLine);
    }

    @Test
    public void testApocConfWithWrongFilePermissions() throws Exception {
        for (PosixFilePermission filePermission : forbiddenFilePermissionsForCommandExpansion) {
            setApocConfigFilePermissions(Set.of(filePermission));

            RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
            String expectedMessage = "does not have the correct file permissions to evaluate commands.";
            Assertions.assertThat(e.getMessage()).contains(expectedMessage);
        }
        // Set back to permitted after test
        setApocConfigFilePermissions(permittedFilePermissionsForCommandExpansion);
    }

    @Test
    public void testApocConfWithoutExpandCommands() {
        InternalLogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.expandCommands()).thenReturn(false);
        when(neo4jConfig.get(GraphDatabaseSettings.configuration_directory))
                .thenReturn(Path.of(apocConfigCommandExpansionFile.getParent()));

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        ApocConfig apocConfig =
                new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);

        RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
        String expectedMessage =
                "$(echo \"expanded value\") is a command, but config is not explicitly told to expand it. (Missing --expand-commands argument?)";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);
    }

    @Test
    public void testMaxDecompressionRatioValidation() {

        InternalLogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.expandCommands()).thenReturn(false);
        when(neo4jConfig.get(GraphDatabaseSettings.configuration_directory))
                .thenReturn(Path.of("C:/neo4j/neo4j-enterprise-5.x.0/conf"));

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        System.setProperty(APOC_MAX_DECOMPRESSION_RATIO, "0");
        ApocConfig apocConfig =
                new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);

        RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
        String expectedMessage =
                String.format("value 0 is not allowed for the config option %s", APOC_MAX_DECOMPRESSION_RATIO);
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);
        System.clearProperty(APOC_MAX_DECOMPRESSION_RATIO);
    }

    private void removeLineFromApocConfig(String lineContent) throws IOException {
        File temp = new File("_temp_");
        PrintWriter out = new PrintWriter(new FileWriter(temp));
        Files.lines(apocConfigCommandExpansionFile.toPath())
                .filter(line -> !line.contains(lineContent))
                .forEach(out::println);
        out.flush();
        out.close();
        temp.renameTo(apocConfigCommandExpansionFile);
    }

    private void addLineToApocConfig(String line) throws IOException {
        FileWriter fw = new FileWriter(apocConfigCommandExpansionFile, true);
        fw.write(line);
        fw.close();
    }
}
