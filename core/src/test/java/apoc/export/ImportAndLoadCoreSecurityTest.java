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
package apoc.export;

import static apoc.export.SecurityTestUtil.ALLOWED_EXCEPTIONS;
import static apoc.export.SecurityTestUtil.cypher5OnlyProcedures;
import static apoc.export.SecurityTestUtil.setImportFileApocConfigs;
import static apoc.util.FileTestUtil.createTempFolder;
import static apoc.util.FileUtils.ACCESS_OUTSIDE_DIR_ERROR;
import static apoc.util.FileUtils.ERROR_READ_FROM_FS_NOT_ALLOWED;
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import apoc.ApocConfig;
import apoc.export.csv.ImportCsv;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ImportJson;
import apoc.load.LoadArrow;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.SensitivePathGenerator;
import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import com.nimbusds.jose.util.Pair;
import inet.ipaddr.IPAddressString;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(configurationCallback = "configure")
class ImportAndLoadCoreSecurityTest {
    private static final Path TEMP_FOLDER = createTempFolder();

    // base path: "../../../../etc/passwd"
    private static final String ABSOLUTE_URL =
            SensitivePathGenerator.etcPasswd().getLeft();
    private static final String ABSOLUTE_URL_WITH_FILE_PREFIX = "file:" + ABSOLUTE_URL;
    private static final String ABSOLUTE_URL_WITH_FILE_SLASH = "file:/" + ABSOLUTE_URL;
    private static final String ABSOLUTE_URL_WITH_FILE_DOUBLE_SLASH = "file://" + ABSOLUTE_URL;
    private static final String ABSOLUTE_URL_WITH_FILE_TRIPLE_SLASH = "file:///" + ABSOLUTE_URL;

    // base path: "/etc/passwd"
    private static final String RELATIVE_URL =
            SensitivePathGenerator.etcPasswd().getRight();
    private static final String RELATIVE_URL_WITH_FILE_SLASH = "file:" + RELATIVE_URL;
    private static final String RELATIVE_URL_WITH_FILE_DOUBLE_SLASH = "file:/" + RELATIVE_URL;
    private static final String RELATIVE_URL_WITH_FILE_TRIPLE_SLASH = "file://" + RELATIVE_URL;

    static Collection<String[]> data() {
        // Create local copies instead of reusing streams from SecurityTestUtil to avoid reuse issues
        List<Pair<String, String>> importAndLoadProcedures = new java.util.ArrayList<>();
        // IMPORT procedures
        importAndLoadProcedures.add(Pair.of("apoc.import.json", "($fileName)"));
        importAndLoadProcedures.add(
                Pair.of("apoc.import.csv", "([{fileName: $fileName, labels: ['Person']}], [], {})"));
        importAndLoadProcedures.add(Pair.of("apoc.import.csv", "([], [{fileName: $fileName, type: 'KNOWS'}], {})"));
        importAndLoadProcedures.add(Pair.of("apoc.import.graphml", "($fileName, {})"));
        importAndLoadProcedures.add(Pair.of("apoc.import.xml", "($fileName)"));
        // LOAD procedures
        importAndLoadProcedures.add(Pair.of("apoc.load.json", "($fileName, '', {})"));
        importAndLoadProcedures.add(Pair.of("apoc.load.jsonArray", "($fileName, '', {})"));
        importAndLoadProcedures.add(Pair.of("apoc.load.jsonParams", "($fileName, {}, '')"));
        importAndLoadProcedures.add(Pair.of("apoc.load.xml", "($fileName, '', {}, false)"));
        importAndLoadProcedures.add(Pair.of("apoc.load.arrow", "($fileName)"));

        return getParameterData(importAndLoadProcedures);
    }

    private static List<String[]> getParameterData(List<Pair<String, String>> importAndLoadProcedures) {
        // from a stream of fileNames and a List of Pair<procName, procArgs>
        // returns a List of String[]{ procName, procArgs, fileName }

        Stream<String> fileNames = Stream.of(
                ABSOLUTE_URL,
                ABSOLUTE_URL_WITH_FILE_PREFIX,
                ABSOLUTE_URL_WITH_FILE_SLASH,
                ABSOLUTE_URL_WITH_FILE_DOUBLE_SLASH,
                ABSOLUTE_URL_WITH_FILE_TRIPLE_SLASH,
                RELATIVE_URL,
                RELATIVE_URL_WITH_FILE_SLASH,
                RELATIVE_URL_WITH_FILE_DOUBLE_SLASH,
                RELATIVE_URL_WITH_FILE_TRIPLE_SLASH);

        return fileNames
                .flatMap(fileName -> importAndLoadProcedures.stream()
                        .map(procPair -> new String[] {procPair.getLeft(), procPair.getRight(), fileName}))
                .toList();
    }

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseSettings.load_csv_file_url_root, TEMP_FOLDER);
        builder.setConfig(
                GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString("127.168.0.0/8")));
    }

    @BeforeAll
    void setUpAll() {
        Logger logger = Logger.getLogger(ImportAndLoadCoreSecurityTest.class.getName());
        logger.setLevel(Level.SEVERE);

        TestUtil.registerProcedure(
                db,
                // import procedures (ExportGraphML contains the `apoc.import.graphml` too)
                ImportJson.class,
                Xml.class,
                ImportCsv.class,
                ExportGraphML.class,
                // load procedures (Xml contains both `apoc.load.xml` and `apoc.import.xml` procedures)
                LoadJson.class,
                LoadArrow.class);
    }

    static Stream<Arguments> args() {
        return data().stream().map(arr -> {
            String method = arr[0];
            String methodArgs = arr[1];
            String fileName = arr[2];
            String cypherVersion = cypher5OnlyProcedures.contains(method) ? "CYPHER 5 " : "";
            String apocProcedure = cypherVersion + "CALL " + method + methodArgs;
            return Arguments.of(apocProcedure, method, fileName);
        });
    }

    @ParameterizedTest(name = "Procedure: {0} ({1}), fileName: {2}")
    @MethodSource("args")
    void testIllegalFSAccessWithDifferentApocConfs(String apocProcedure, String importMethod, String fileName) {
        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(true, true, true);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, false, false);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, true, false);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, false, true);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, true, false);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(false, true, true);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, false, true);
        assertIpAddressBlocked(apocProcedure);

        // apoc.import.file.enabled=false
        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, false, false);
        assertIpAddressBlocked(apocProcedure);
    }

    @ParameterizedTest(name = "Procedure: {0} ({1}), fileName: {2}")
    @MethodSource("args")
    void testImportFileDisabled(String apocProcedure, String importMethod, String fileName) {
        // all assertions with `apoc.import.file.enabled=false`

        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, false, false);
        assertImportDisabled(apocProcedure, fileName);

        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(false, true, false);
        assertImportDisabled(apocProcedure, fileName);

        // apoc.import.file.use_neo4j_config=false
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(false, false, true);
        assertImportDisabled(apocProcedure, fileName);

        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(false, true, true);
        assertImportDisabled(apocProcedure, fileName);
    }

    @ParameterizedTest(name = "Procedure: {0} ({1}), fileName: {2}")
    @MethodSource("args")
    void testIllegalFSAccessWithImportAndUseNeo4jConfsEnabled(
            String apocProcedure, String importMethod, String fileName) {
        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, true, false);
        assertReadFromFsNotAllowed(apocProcedure, fileName);
    }

    @ParameterizedTest(name = "Procedure: {0} ({1}), fileName: {2}")
    @MethodSource("args")
    void testImportOutsideDirNotAllowedWithAllApocFileConfigsEnabled(
            String apocProcedure, String importMethod, String fileName) {
        // apoc.import.file.enabled=true
        // apoc.import.file.use_neo4j_config=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(true, true, true);

        // only `../../../etc/passwd` throw the error, other urls just don't find the file,
        // i.e.: `file:/../../../etc/passwd`, `file://../../../etc/passwd` and `file:///../../../etc/passwd`
        // and relative ones (like `/etc/passwd`)
        if (fileName.equals(ABSOLUTE_URL) || fileName.equals(ABSOLUTE_URL_WITH_FILE_PREFIX)) {
            assertImportOutsideDirNotAllowed(apocProcedure, fileName);
        } else {
            assertFileNotExists(apocProcedure, fileName);
        }
    }

    @ParameterizedTest(name = "Procedure: {0} ({1}), fileName: {2}")
    @MethodSource("args")
    void testReadSensitiveFileWorksWithApocUseNeo4jConfigDisabled(
            String apocProcedure, String importMethod, String fileName) {
        // all checks with `apoc.import.file.use_neo4j_config=false`

        // apoc.import.file.enabled=true
        // apoc.import.file.allow_read_from_filesystem=true
        setImportFileApocConfigs(true, false, true);
        shouldRead(apocProcedure, importMethod, fileName);

        // apoc.import.file.enabled=false
        // apoc.import.file.allow_read_from_filesystem=false
        setImportFileApocConfigs(true, false, false);
        shouldRead(apocProcedure, importMethod, fileName);
    }

    private void assertIpAddressBlocked(String apocProcedure) {
        Stream.of("https", "http", "ftp").forEach(protocol -> {
            String url = String.format("%s://127.168.0.0/test.file", protocol);
            QueryExecutionException e = assertThrows(
                    QueryExecutionException.class,
                    () -> testCall(db, apocProcedure, Map.of("fileName", url), (r) -> {}));
            assertTrue(
                    e.getMessage()
                            .contains(
                                    "access to /127.168.0.0 is blocked via the configuration property internal.dbms.cypher_ip_blocklist"));
        });
    }

    private void assertImportDisabled(String apocProcedure, String fileName) {
        assertFailingProcedure(apocProcedure, fileName, ApocConfig.LOAD_FROM_FILE_ERROR, RuntimeException.class);
    }

    private void assertReadFromFsNotAllowed(String apocProcedure, String fileName) {
        assertFailingProcedure(
                apocProcedure,
                fileName,
                String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, fileName),
                RuntimeException.class);
    }

    private void assertFailingProcedure(
            String apocProcedure, String fileName, String expectedError, Class exceptionClass) {
        final String message = apocProcedure + " should throw an exception";

        try {
            db.executeTransactionally(apocProcedure, Map.of("fileName", fileName), Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            TestUtil.assertError(e, expectedError, exceptionClass, apocProcedure);
        }
    }

    private void shouldRead(String apocProcedure, String importMethod, String fileName) {
        // the `file://../../../etc/passwd` is not found unlike the other similar urls,
        // i.e.: `file:/../../../etc/passwd`, `file:///../../../etc/passwd` and `../../../etc/passwd`
        if (fileName.equals(RELATIVE_URL_WITH_FILE_DOUBLE_SLASH)) {
            assertFileNotExists(apocProcedure, fileName);
            return;
        }
        try {
            db.executeTransactionally(apocProcedure, Map.of("fileName", fileName), Result::resultAsString);
        } catch (Exception e) {
            if (ALLOWED_EXCEPTIONS.containsKey(importMethod)) {
                Class<?> rootCause = ExceptionUtils.getRootCause(e).getClass();
                Class<?> classException = ALLOWED_EXCEPTIONS.get(importMethod);
                assertTrue(classException.isAssignableFrom(rootCause));
            }
        }
    }

    private void assertImportOutsideDirNotAllowed(String apocProcedure, String fileName) {
        assertFailingProcedure(apocProcedure, fileName, ACCESS_OUTSIDE_DIR_ERROR, IOException.class);
    }

    private void assertFileNotExists(String apocProcedure, String fileName) {
        try {
            db.executeTransactionally(apocProcedure, Map.of("fileName", fileName), Result::resultAsString);
        } catch (Exception e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof IOException);
            String message = e.getMessage();
            Assertions.assertThat(message).contains("Cannot open file ");
            Assertions.assertThat(message).contains(" for reading.");
        }
    }
}
