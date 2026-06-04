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
package apoc.export.arrow;

import static apoc.export.SecurityTestUtil.ERROR_KEY;
import static apoc.export.SecurityTestUtil.PROCEDURE_KEY;
import static apoc.export.SecurityTestUtil.getApocProcedure;
import static apoc.export.SecurityTestUtil.setExportFileApocConfigs;
import static apoc.export.arrow.ExportArrowService.EXPORT_TO_FILE_ARROW_ERROR;
import static org.junit.jupiter.api.Assertions.assertTrue;

import apoc.export.ExportCoreSecurityTest;
import apoc.export.SecurityTestUtil;
import apoc.meta.Meta;
import apoc.meta.MetaRestricted;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import com.nimbusds.jose.util.Pair;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

/**
 * CYPHER 5 only; moved to extended for Cypher 25
 */
@EnterpriseDbmsExtension(configurationCallback = "configure")
class ExportArrowSecurityTest {
    public static final File directory = new File("target/import");
    public static final File directoryWithSamePrefix = new File("target/imported");
    public static final File subDirectory = new File("target/import/tests");
    public static final List<String> APOC_EXPORT_PROCEDURE_NAME = List.of("arrow");

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        subDirectory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        directoryWithSamePrefix.mkdirs();
    }

    public static List<Pair<String, String>> EXPORT_PROCEDURES = List.of(
            Pair.of("query", "$fileName, 'RETURN 1', {}"),
            Pair.of("all", "$fileName, {}"),
            Pair.of("graph", "$fileName, {nodes: [], relationships: []}, {}"));

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(
                GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
        builder.setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher5);
    }

    @BeforeAll
    void setUpAll() {
        Logger logger = Logger.getLogger(ExportArrowSecurityTest.class.getName());
        logger.setLevel(Level.SEVERE);
        TestUtil.registerProcedure(db, ExportArrow.class, Meta.class, MetaRestricted.class);
    }

    private static Collection<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors) {
        return ExportCoreSecurityTest.getParameterData(fileAndErrors, EXPORT_PROCEDURES, APOC_EXPORT_PROCEDURE_NAME);
    }

    // Local copies of consumers and datasets previously imported from ExportCoreSecurityTest's inner classes
    static final Consumer<Map> EXCEPTION_NOT_FOUND_CONSUMER = (Map e) ->
            assertTrue(((Exception) e.get(ERROR_KEY)).getMessage().contains("test.txt (No such file or directory)"));
    static final Consumer<Map> EXCEPTION_OUTDIR_CONSUMER =
            (Map e) -> assertTrue(((Exception) e.get(ERROR_KEY)).getMessage().contains("outside the import directory"));

    // Illegal external FS access cases
    private static final String FILENAME = "arrow-my-test.txt";
    private static final String PARAM_NAMES = ExportCoreSecurityTest.PARAM_NAMES;

    private static final String case01 = "../imported/" + FILENAME;
    private static final String case02 = "tests/../../imported/" + FILENAME;
    private static final String case03 = "../" + FILENAME;
    private static final String case04 = "file:../" + FILENAME;
    private static final String case05 = "file:..//" + FILENAME;
    private static final String case07 = "tests/../../" + FILENAME;
    private static final String case08 = "tests/..//..//" + FILENAME;

    private static final List<String> casesOutsideDir = List.of(case01, case02, case03, case04, case05, case07, case08);

    private static final String nonExistingDirectory = "__non-existing-dir__";
    public static final String case10 = "file:///%2e%2e%2f%2f" + FILENAME;
    private static final String case10Full =
            "file://%2e%2e%2f%2e%2e%2f%2e%2e%2f%2e%2e%2f/" + nonExistingDirectory + "/" + FILENAME;
    private static final String case11 = String.format("file://../../../../%s/%s", nonExistingDirectory, FILENAME);
    private static final String case12 =
            String.format("file:///..//..//..//..//%s//core//..//%s", nonExistingDirectory, FILENAME);
    private static final String case13 = String.format("file:///..//..//..//..//%s/%s", nonExistingDirectory, FILENAME);
    private static final String case14 = String.format(
            "file://" + directory.getAbsolutePath() + "//..//..//..//..//%s/%s", nonExistingDirectory, FILENAME);
    private static final String case15 = "file:///%252e%252e%252f%252e%252e%252f%252e%252e%252f%252e%252e%252f/"
            + nonExistingDirectory + "/" + FILENAME;

    private static final List<String> casesNotExistingDir = List.of(case10Full, case11, case12, case13, case14, case15);

    static List<Pair<String, Consumer<Map>>> dataPairs;

    static {
        dataPairs = casesOutsideDir.stream()
                // Cases that resolve outside the import directory should yield an "outside the import directory" error
                .map(i -> Pair.of(i, EXCEPTION_OUTDIR_CONSUMER))
                .collect(Collectors.toList());
        List<Pair<String, Consumer<Map>>> notExistingDirList = casesNotExistingDir.stream()
                .map(i -> Pair.of(i, EXCEPTION_NOT_FOUND_CONSUMER))
                .toList();
        dataPairs.addAll(notExistingDirList);
    }

    static final Consumer<Map> MAIN_DIR_CONSUMER = (r) -> assertTrue(((String) r.get("file")).contains("" + FILENAME));
    static final Consumer<Map> SUB_DIR_CONSUMER =
            (r) -> assertTrue(((String) r.get("file")).contains("tests/" + FILENAME));

    private static final String caseBase = "./" + FILENAME;
    private static final String tcase01 = "file:///..//..//..//..//apoc//..//..//..//..//" + FILENAME;
    private static final String tcase02 = "file:///..//..//..//..//apoc//..//" + FILENAME;
    private static final String tcase03 = "file:///../import/../import//..//" + FILENAME;
    private static final String tcase04 = "file://" + FILENAME;
    private static final String tcase05 = "file://tests/../" + FILENAME;
    private static final String tcase06 = "file:///tests//..//" + FILENAME;
    private static final String tcase07 = "" + FILENAME;
    private static final String tcase08 = "file:///..//..//..//..//" + FILENAME;
    private static final String tcase09 = "file:///%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f/" + FILENAME;

    private static final List<String> mainDirCases =
            List.of(caseBase, tcase01, tcase02, tcase03, tcase04, tcase05, tcase06, tcase07, tcase08, tcase09, case10);

    private static final String scase11 = "file:///../import/../import//..//tests/" + FILENAME;
    private static final String scase12 = "file:///..//..//..//..//apoc//..//tests/" + FILENAME;
    private static final String scase13 = "file:///../import/../import//..//tests/../tests/" + FILENAME;
    private static final String scase14 = "file:///tests/" + FILENAME;
    private static final String scase15 = "tests/" + FILENAME;
    private static final List<String> subDirCases = List.of(scase11, scase12, scase13, scase14, scase15);

    private static List<Object[]> getArrowParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors) {
        return fileAndErrors.stream()
                .flatMap(fileName -> EXPORT_PROCEDURES.stream()
                        .flatMap(procPair -> APOC_EXPORT_PROCEDURE_NAME.stream().map(procName -> new Object[] {
                            procName, procPair.getLeft(), procPair.getRight(), fileName.getLeft(), fileName.getRight()
                        })))
                .toList();
    }

    private static Stream<Arguments> illegalExternalData() {
        return getArrowParameterData(dataPairs).stream()
                .map(arr -> Arguments.of(
                        getApocProcedure((String) arr[0], (String) arr[1], (String) arr[2]),
                        (String) arr[3],
                        (Consumer<Map>) arr[4]));
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("apoc.export.arrow.ExportArrowSecurityTest#illegalExternalData")
    void testsWithExportDisabled(String apocProcedure, String fileName, Consumer<Map> consumer) {
        SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName, EXPORT_TO_FILE_ARROW_ERROR);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("apoc.export.arrow.ExportArrowSecurityTest#illegalExternalData")
    void testIllegalExternalFSAccessExportWithExportAndUseNeo4jConfEnabled(
            String apocProcedure, String fileName, Consumer<Map> consumer) {
        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, true, false);
        SecurityTestUtil.assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, true, true);
        SecurityTestUtil.assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("apoc.export.arrow.ExportArrowSecurityTest#illegalExternalData")
    void testWithUseNeo4jConfDisabledExternal(String apocProcedure, String fileName, Consumer<Map> consumer) {
        // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=false`

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, false, true);
        try {
            // with `apoc.import.file.use_neo4j_config=false` this file export could be outside the project
            if (!fileName.equals(case10)) {
                SecurityTestUtil.assertPathTraversalWithoutErrors(
                        db, apocProcedure, fileName, new File("../", FILENAME));
            }
        } catch (QueryExecutionException e) {
            EXCEPTION_NOT_FOUND_CONSUMER.accept(Util.map(ERROR_KEY, e, PROCEDURE_KEY, apocProcedure));
        }

        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, false, false);
        try {
            if (!fileName.equals(case10)) {
                SecurityTestUtil.assertPathTraversalWithoutErrors(
                        db, apocProcedure, fileName, new File("../", FILENAME));
            }
        } catch (QueryExecutionException e) {
            EXCEPTION_NOT_FOUND_CONSUMER.accept(Util.map(ERROR_KEY, e, PROCEDURE_KEY, apocProcedure));
        }
    }

    private static Stream<Arguments> traversalData() {
        List<Pair<String, Consumer<Map>>> collect =
                mainDirCases.stream().map(i -> Pair.of(i, MAIN_DIR_CONSUMER)).collect(Collectors.toList());
        List<Pair<String, Consumer<Map>>> collect2 =
                subDirCases.stream().map(i -> Pair.of(i, SUB_DIR_CONSUMER)).toList();
        collect.addAll(collect2);

        return getArrowParameterData(collect).stream()
                .map(arr -> Arguments.of(
                        getApocProcedure((String) arr[0], (String) arr[1], (String) arr[2]),
                        (String) arr[3],
                        (Consumer<Map>) arr[4]));
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("apoc.export.arrow.ExportArrowSecurityTest#traversalData")
    void testPathTraversal(String apocProcedure, String fileName, Consumer<Map> consumer) {
        setExportFileApocConfigs(true, true, false);
        File dir = subDirCases.contains(fileName) ? subDirectory : directory;
        File file = new File(dir.getAbsolutePath(), FILENAME);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("apoc.export.arrow.ExportArrowSecurityTest#traversalData")
    void testIllegalFSAccessExport(String apocProcedure, String fileName, Consumer<Map> consumer) {
        SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName, EXPORT_TO_FILE_ARROW_ERROR);
    }

    @ParameterizedTest(name = PARAM_NAMES)
    @MethodSource("apoc.export.arrow.ExportArrowSecurityTest#traversalData")
    void testWithUseNeo4jConfDisabledNormalised(String apocProcedure, String fileName, Consumer<Map> consumer) {
        // apoc.import.file.allow_read_from_filesystem=false
        setExportFileApocConfigs(true, true, false);
        File dir = subDirCases.contains(fileName) ? subDirectory : directory;
        File file = new File(dir.getAbsolutePath(), FILENAME);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);

        // apoc.import.file.allow_read_from_filesystem=true
        setExportFileApocConfigs(true, true, true);
        SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
    }
}
