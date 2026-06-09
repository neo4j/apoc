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

import static apoc.export.SecurityTestUtil.ERROR_KEY;
import static apoc.export.SecurityTestUtil.EXPORT_PROCEDURES;
import static apoc.export.SecurityTestUtil.PROCEDURE_KEY;
import static apoc.export.SecurityTestUtil.assertPathTraversalError;
import static apoc.export.SecurityTestUtil.getApocProcedure;
import static apoc.export.SecurityTestUtil.setExportFileApocConfigs;
import static apoc.util.TestUtil.assertError;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.ApocConfig;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import com.nimbusds.jose.util.Pair;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(configurationCallback = "configure")
public class ExportCoreSecurityTest {
    private static final File directory = new File("target/import");
    private static final File directoryWithSamePrefix = new File("target/imported");
    private static final File subDirectory = new File("target/import/tests");
    private static final List<String> APOC_EXPORT_PROCEDURE_NAME = Arrays.asList("csv", "json", "graphml", "cypher");

    public static final String FILENAME = "my-test.txt";
    public static final String PARAM_NAMES = "Procedure: {0}.{1}, fileName: {3}";

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        subDirectory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        directoryWithSamePrefix.mkdirs();
    }

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(
                GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
    }

    @AfterEach
    void tearDown() {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, false);
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG, true);
    }

    @BeforeAll
    void setUpAll() {
        Logger logger = Logger.getLogger(ExportCoreSecurityTest.class.getName());
        logger.setLevel(Level.SEVERE);
        TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class);
    }

    public static void setFileExport(boolean allowed) {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, allowed);
    }

    private static List<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors) {
        return getParameterData(fileAndErrors, EXPORT_PROCEDURES, APOC_EXPORT_PROCEDURE_NAME);
    }

    public static List<Object[]> getParameterData(
            List<Pair<String, Consumer<Map>>> fileAndErrors,
            List<Pair<String, String>> importAndLoadProcedures,
            List<String> procedureNames) {
        // from a stream of fileNames and a List of Pair<procName, procArgs>
        // returns a List of String[]{ procName, procArgs, fileName }
        return fileAndErrors.stream()
                .flatMap(fileName -> importAndLoadProcedures.stream()
                        .flatMap(procPair -> procedureNames.stream().map(procName -> new Object[] {
                            procName, procPair.getLeft(), procPair.getRight(), fileName.getLeft(), fileName.getRight()
                        })))
                .toList();
    }

    public static final Consumer<Map> EXCEPTION_OUTDIR_CONSUMER = (Map e) ->
            assertError((Exception) e.get(ERROR_KEY), FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, (String)
                    e.get(PROCEDURE_KEY));
    public static final Consumer<Map> EXCEPTION_NOT_FOUND_CONSUMER = (Map e) ->
            assertTrue(((Exception) e.get(ERROR_KEY)).getMessage().contains("test.txt (No such file or directory)"));

    /*
     * These test cases attempt to access a directory with the same prefix as the import directory. This is design to
     * test "directoryName.startsWith" logic which is a common path traversal bug.
     * All these tests should fail because they access a directory which isn't the configured directory
     */
    private static final String case01 = "../imported/" + FILENAME;
    private static final String case02 = "tests/../../imported/" + FILENAME;
    private static final String case03 = "../" + FILENAME;
    private static final String case04 = "file:../" + FILENAME;
    private static final String case05 = "file:..//" + FILENAME;
    public static final List<String> casesAllowed = Arrays.asList(case03, case04, case05);
    private static final String case07 = "tests/../../" + FILENAME;
    private static final String case08 = "tests/..//..//" + FILENAME;
    public static final List<String> casesOutsideDir =
            Arrays.asList(case01, case02, case03, case04, case05, case07, case08);
    /*
    All of these will resolve to a local path after normalization which will point to
    a non-existing directory in our import folder: /apoc. Causing them to error that is
    not found. They all attempt to exit the import folder back to the apoc folder:
    Directory Layout: .../apoc/core/target/import
    */
    private static final String nonExistingDirectory = "__non-existing-dir__";
    private static final String case10 =
            "file://%2e%2e%2f%2e%2e%2f%2e%2e%2f%2e%2e%2f/" + nonExistingDirectory + "/" + FILENAME;
    private static final String case11 = String.format("file://../../../../%s/%s", nonExistingDirectory, FILENAME);
    private static final String case12 =
            String.format("file:///..//..//..//..//%s//core//..//%s", nonExistingDirectory, FILENAME);
    private static final String case13 = String.format("file:///..//..//..//..//%s/%s", nonExistingDirectory, FILENAME);
    private static final String case14 = String.format(
            "file://" + directory.getAbsolutePath() + "//..//..//..//..//%s/%s", nonExistingDirectory, FILENAME);
    private static final String case15 = "file:///%252e%252e%252f%252e%252e%252f%252e%252e%252f%252e%252e%252f/"
            + nonExistingDirectory + "/" + FILENAME;
    public static final List<String> casesNotExistingDir =
            Arrays.asList(case10, case11, case12, case13, case14, case15);
    public static List<Pair<String, Consumer<Map>>> dataPairs;

    static {
        dataPairs = casesOutsideDir.stream()
                .map(i -> Pair.of(i, EXCEPTION_OUTDIR_CONSUMER))
                .collect(Collectors.toList());

        List<Pair<String, Consumer<Map>>> notExistingDirList = casesNotExistingDir.stream()
                .map(i -> Pair.of(i, EXCEPTION_NOT_FOUND_CONSUMER))
                .toList();

        dataPairs.addAll(notExistingDirList);
    }

    static Stream<Arguments> illegalExternalData() {
        return ExportCoreSecurityTest.getParameterData(dataPairs).stream()
                .map(arr -> Arguments.of(
                        getApocProcedure((String) arr[0], (String) arr[1], (String) arr[2]),
                        (String) arr[3],
                        (Consumer<Map>) arr[4]));
    }

    /*
    All of these will resolve to a local path after normalization which will point to
    a non-existing directory in our import folder: /apoc. Causing them to error that is
    not found. They all attempt to exit the import folder back to the apoc folder:
    Directory Layout: .../apoc/core/target/import
    */
    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("illegalExternalData")
    void testsWithExportDisabled(String apocProcedure, String fileName, Consumer<Map> consumer) {
        SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("illegalExternalData")
    void testIllegalExternalFSAccessExportWithExportAndUseNeo4jConfEnabled(
            String apocProcedure, String fileName, Consumer<Map> consumer) {
        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, true, false);
        assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, true, true);
        assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("illegalExternalData")
    void testWithUseNeo4jConfDisabledExternal(String apocProcedure, String fileName, Consumer<Map> consumer) {
        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, false, true);
        testWithUseNeo4jConfFalse(apocProcedure, fileName);

        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, false, false);
        testWithUseNeo4jConfFalse(apocProcedure, fileName);
    }

    private void testWithUseNeo4jConfFalse(String apocProcedure, String fileName) {
        try {
            SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, new File("../", FILENAME));
        } catch (QueryExecutionException e) {
            EXCEPTION_NOT_FOUND_CONSUMER.accept(Util.map(ERROR_KEY, e, PROCEDURE_KEY, apocProcedure));
        }
    }

    /**
     * These tests normalize the path to be within the import directory (or subdirectory) and make the file there.
     * Some attempt to exit the directory.
     */
    public static final Consumer<Map> MAIN_DIR_CONSUMER =
            (r) -> assertTrue(((String) r.get("file")).contains("" + FILENAME));

    public static final Consumer<Map> SUB_DIR_CONSUMER =
            (r) -> assertTrue(((String) r.get("file")).contains("tests/" + FILENAME));

    /**
     * These tests normalize the path to be within the import directory and make the file there.
     * They result in a file being created (and deleted after).
     */
    private static final String caseBase = "./" + FILENAME;

    private static final String ncase01 = "file:///..//..//..//..//apoc//..//..//..//..//" + FILENAME;
    private static final String ncase02 = "file:///..//..//..//..//apoc//..//" + FILENAME;
    private static final String ncase03 = "file:///../import/../import//..//" + FILENAME;
    private static final String ncase04 = "file://" + FILENAME;
    private static final String ncase05 = "file://tests/../" + FILENAME;
    private static final String ncase06 = "file:///tests//..//" + FILENAME;
    private static final String ncase07 = "" + FILENAME;
    private static final String ncase08 = "file:///..//..//..//..//" + FILENAME;
    private static final String ncase09 = "file:///%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f/" + FILENAME;
    public static final String ncase10 = "file:///%2e%2e%2f%2f" + FILENAME;

    public static final List<String> mainDirCases = Arrays.asList(
            caseBase, ncase01, ncase02, ncase03, ncase04, ncase05, ncase06, ncase07, ncase08, ncase09, ncase10);

    /**
     * These tests normalize the path to be within the import directory and step into a subdirectory
     * to make the file there.
     * They result in a file in the directory /tests being created (and deleted after).
     */
    private static final String ncase11 = "file:///../import/../import//..//tests/" + FILENAME;

    private static final String ncase12 = "file:///..//..//..//..//apoc//..//tests/" + FILENAME;
    private static final String ncase13 = "file:///../import/../import//..//tests/../tests/" + FILENAME;
    private static final String ncase14 = "file:///tests/" + FILENAME;
    private static final String ncase15 = "tests/" + FILENAME;

    public static final List<String> subDirCases = Arrays.asList(ncase11, ncase12, ncase13, ncase14, ncase15);

    static Stream<Arguments> traversalData() {
        List<Pair<String, Consumer<Map>>> collect =
                mainDirCases.stream().map(i -> Pair.of(i, MAIN_DIR_CONSUMER)).collect(Collectors.toList());
        List<Pair<String, Consumer<Map>>> collect2 =
                subDirCases.stream().map(i -> Pair.of(i, SUB_DIR_CONSUMER)).toList();
        collect.addAll(collect2);

        return getParameterData(collect).stream()
                .map(arr -> Arguments.of(
                        getApocProcedure((String) arr[0], (String) arr[1], (String) arr[2]),
                        (String) arr[3],
                        (Consumer<Map>) arr[4]));
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("traversalData")
    void testPathTraversal(String apocProcedure, String fileName, Consumer<Map> consumer) {
        File dir = subDirCases.contains(fileName) ? subDirectory : directory;
        setExportFileApocConfigs(true, true, false);

        File file = new File(dir.getAbsolutePath(), FILENAME);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("traversalData")
    void testIllegalFSAccessExport(String apocProcedure, String fileName, Consumer<Map> consumer) {
        SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("traversalData")
    void testWithUseNeo4jConfDisabledNormalised(String apocProcedure, String fileName, Consumer<Map> consumer) {
        File dir = subDirCases.contains(fileName) ? subDirectory : directory;
        File file = new File(dir.getAbsolutePath(), FILENAME);

        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, true, false);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, true, true);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
    }

    // tests with `apoc.import.file.use_neo4j_config=false` not implemented because results can vary by project path
    // e.g. based on project folder name, so the exported file can be basically everywhere

    private final String apocSchemaProc = "CALL apoc.export.cypher.schema(%s)";

    @Test
    void testIllegalFSAccessExportCypherSchema() {
        setFileExport(false);
        QueryExecutionException e = assertThrows(
                QueryExecutionException.class,
                () -> TestUtil.testCall(db, String.format(apocSchemaProc, "'./hello', {}"), (r) -> {}));
        assertError(e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocSchemaProc);
    }

    @Test
    void testIllegalExternalFSAccessExportCypherSchema() {
        setExportFileApocConfigs(true, true, false);
        assertPathTraversalError(
                db,
                String.format(apocSchemaProc, "'../hello', {}"),
                Map.of(),
                e -> assertError(
                        (Exception) e.get(ERROR_KEY), FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, (String)
                                e.get(PROCEDURE_KEY)));
    }
}
