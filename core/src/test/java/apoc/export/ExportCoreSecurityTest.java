package apoc.export;

import apoc.ApocConfig;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import com.nimbusds.jose.util.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static apoc.export.SecurityTestUtil.ERROR_KEY;
import static apoc.export.SecurityTestUtil.EXPORT_PROCEDURES;
import static apoc.export.SecurityTestUtil.PROCEDURE_KEY;
import static apoc.export.SecurityTestUtil.assertPathTraversalError;
import static apoc.export.SecurityTestUtil.getApocProcedure;
import static apoc.export.SecurityTestUtil.setExportFileApocConfigs;
import static apoc.util.TestUtil.assertError;
import static org.junit.Assert.assertTrue;

@RunWith(Enclosed.class)
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

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class);
    }

    public static void setFileExport(boolean allowed) {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, allowed);
    }

    private static List<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors) {
        return getParameterData(fileAndErrors, EXPORT_PROCEDURES, APOC_EXPORT_PROCEDURE_NAME);
    }

    public static List<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors, List<Pair<String, String>> importAndLoadProcedures, List<String> procedureNames) {
        // from a stream of fileNames and a List of Pair<procName, procArgs>
        // returns a List of String[]{ procName, procArgs, fileName }
        return fileAndErrors.stream()
                .flatMap(fileName -> importAndLoadProcedures
                        .stream()
                        .flatMap(procPair -> procedureNames
                                .stream()
                                .map(procName ->
                                        new Object[] { procName, procPair.getLeft(), procPair.getRight(), fileName.getLeft(), fileName.getRight() }
                                )
                        )
                ).toList();
    }

    /**
     * These test with `apoc.import.file.use_neo4j_config=true`
     * should fail because they access a directory which isn't the configured directory
     */
    @RunWith(Parameterized.class)
    public static class TestIllegalExternalFSAccess {

        // these 2 consumers accept a Map.of("error", <errorMsg>, "procedure", <procedureQuery>)
        public static final Consumer<Map> EXCEPTION_OUTDIR_CONSUMER = (Map e) -> assertError(
                (Exception) e.get(ERROR_KEY), FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, (String) e.get(PROCEDURE_KEY)
        );
        public static final Consumer<Map> EXCEPTION_NOT_FOUND_CONSUMER = (Map e) -> assertTrue(
                ((Exception) e.get(ERROR_KEY)).getMessage().contains("test.txt (No such file or directory)")
        );

        private final String apocProcedure;
        private final String fileName;
        private final Consumer consumer;

        public TestIllegalExternalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments,
                                           String fileName,
                                           Consumer consumer) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
            this.fileName = fileName;
            this.consumer = consumer;
        }

        /*
         * These test cases attempt to access a directory with the same prefix as the import directory. This is design to
         * test "directoryName.startsWith" logic which is a common path traversal bug.
         * All these tests should fail because they access a directory which isn't the configured directory
         */
        private static final String case1 = "../imported/" + FILENAME;
        private static final String case2 = "tests/../../imported/" + FILENAME;
        private static final String case3 = "../" + FILENAME;
        private static final String case4 = "file:../" + FILENAME;
        private static final String case5 = "file:..//" + FILENAME;

        // non-failing cases, with apoc.import.file.use_neo4j_config=false
        public static final List<String> casesAllowed = Arrays.asList(case3, case4, case5);

        private static final String case16 = "file:///%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2f%2e%2e%2f%2fapoc/" + FILENAME;
        public static final String case26 = "file:///%2e%2e%2f%2f" + FILENAME;
        private static final String case46 = "tests/../../" + FILENAME;
        private static final String case56 = "tests/..//..//" + FILENAME;

        public static final List<String> casesOutsideDir = Arrays.asList(case1, case2, case3, case4, case5,
                case16, case26, case46, case56);

        /*
         All of these will resolve to a local path after normalization which will point to
         a non-existing directory in our import folder: /apoc. Causing them to error that is
         not found. They all attempt to exit the import folder back to the apoc folder:
         Directory Layout: .../apoc/core/target/import
         */
        private static final String case111 = "file://%2e%2e%2f%2e%2e%2f%2e%2e%2f%2e%2e%2f/apoc/" + FILENAME;
        private static final String case211 = "file://../../../../apoc/" + FILENAME;
        private static final String case311 = "file:///..//..//..//..//apoc//core//..//" + FILENAME;
        private static final String case411 = "file:///..//..//..//..//apoc/" + FILENAME;
        private static final String case511 = "file://" + directory.getAbsolutePath() + "//..//..//..//..//apoc/" + FILENAME;
        private static final String case611 = "file:///%252e%252e%252f%252e%252e%252f%252e%252e%252f%252e%252e%252f/apoc/" + FILENAME;

        public static final List<String> casesNotExistingDir = Arrays.asList(case111, case211, case311, case411, case511, case611);

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

        @Parameterized.Parameters(name = PARAM_NAMES)
        public static Collection<Object[]> data() {
            return ExportCoreSecurityTest.getParameterData(dataPairs);
        }

        @Test
        public void testsWithExportDisabled() {
            SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName);
        }

        @Test
        public void testIllegalExternalFSAccessExportWithExportAndUseNeo4jConfEnabled() {
            // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=true`

            // apoc.import.file.allow_read_from_filesystem=false
            setExportFileApocConfigs(true, true, false);
            assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);

            // apoc.import.file.allow_read_from_filesystem=true
            setExportFileApocConfigs(true, true, true);
            assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);
        }

        @Test
        public void testWithUseNeo4jConfDisabled() {
            // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=false`

            // apoc.import.file.allow_read_from_filesystem=true
            setExportFileApocConfigs(true, false, true);
            testWithUseNeo4jConfFalse();

            // apoc.import.file.allow_read_from_filesystem=false
            setExportFileApocConfigs(true, false, false);
            testWithUseNeo4jConfFalse();
        }

        private void testWithUseNeo4jConfFalse() {
            // with `apoc.import.file.use_neo4j_config=false` this file export could outside the project
            if (fileName.equals(case26)) {
                return;
            }

            if (casesAllowed.contains(fileName)) {
                assertPathTraversalWithoutErrors();
            } else {
                assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), EXCEPTION_NOT_FOUND_CONSUMER);
            }
        }

        private void assertPathTraversalWithoutErrors() {
            SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, new File("../", FILENAME));
        }
    }


    /**
     * These tests normalize the path to be within the import directory (or subdirectory) and make the file there.
     * Some attempt to exit the directory.
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsNormalisedWithinDirectory {

        public static final Consumer<Map> MAIN_DIR_CONSUMER = (r) -> assertTrue(((String) r.get("file")).contains("" + FILENAME));
        public static final Consumer<Map> SUB_DIR_CONSUMER = (r) -> assertTrue(((String) r.get("file")).contains("tests/" + FILENAME));

        private final String apocProcedure;
        private final String fileName;
        private final Consumer consumer;

        public TestPathTraversalIsNormalisedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments,
                                                            String fileName,
                                                            Consumer consumer) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
            this.fileName = fileName;
            this.consumer = consumer;
        }

        /*
         These tests normalize the path to be within the import directory and make the file there.
         They result in a file being created (and deleted after).
         */
        private static final String case0 = "./" + FILENAME;

        private static final String case1 = "file:///..//..//..//..//apoc//..//..//..//..//" + FILENAME;
        private static final String case2 = "file:///..//..//..//..//apoc//..//" + FILENAME;
        private static final String case3 = "file:///../import/../import//..//" + FILENAME;
        private static final String case4 = "file://" + FILENAME;
        private static final String case5 = "file://tests/../" + FILENAME;
        private static final String case6 = "file:///tests//..//" + FILENAME;
        private static final String case7 = "" + FILENAME;
        private static final String case8 = "file:///..//..//..//..//" + FILENAME;

        public static final List<String> mainDirCases = Arrays.asList(case0, case1, case2, case3, case4, case5, case6, case7, case8);

        /*
         These tests normalize the path to be within the import directory and step into a subdirectory
         to make the file there.
         They result in a file in the directory /tests being created (and deleted after).
         */
        private static final String case16 = "file:///../import/../import//..//tests/" + FILENAME;
        private static final String case26 = "file:///..//..//..//..//apoc//..//tests/" + FILENAME;
        private static final String case36 = "file:///../import/../import//..//tests/../tests/" + FILENAME;
        private static final String case46 = "file:///tests/" + FILENAME;
        private static final String case56 = "tests/" + FILENAME;

        public static final List<String> subDirCases = Arrays.asList(case16, case26, case36, case46, case56);


        @Parameterized.Parameters(name = PARAM_NAMES)
        public static Collection<Object[]> data() {
            List<Pair<String, Consumer<Map>>> collect = mainDirCases.stream().map(i -> Pair.of(i, MAIN_DIR_CONSUMER)).collect(Collectors.toList());
            List<Pair<String, Consumer<Map>>> collect2 = subDirCases.stream().map(i -> Pair.of(i, SUB_DIR_CONSUMER)).toList();
            collect.addAll(collect2);

            return getParameterData(collect);
        }

        @Test
        public void testPathTraversal() {
            File dir = getDir();
            setExportFileApocConfigs(true, true, false);

            assertPathTraversalWithoutErrors(dir);
        }

        @Test
        public void testIllegalFSAccessExport() {
            SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName);
        }

        @Test
        public void testWithUseNeo4jConfDisabled() {
            // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=true`

            File dir = getDir();

            // apoc.import.file.allow_read_from_filesystem=false
            setExportFileApocConfigs(true, true, false);
            assertPathTraversalWithoutErrors(dir);

            // apoc.import.file.allow_read_from_filesystem=true
            setExportFileApocConfigs(true, true, true);
            assertPathTraversalWithoutErrors(dir);
        }

        private File getDir() {
            if (subDirCases.contains(fileName)) {
                return subDirectory;
            }
            return directory;
        }

        private void assertPathTraversalWithoutErrors(File directory) {
            File file = new File(directory.getAbsolutePath(), FILENAME);
            SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
        }

        // tests with `apoc.import.file.use_neo4j_config=false` not implemented because the results can be different
        // e.g. based on project folder name, so the exported file can be basically everywhere

    }



    public static class TestCypherSchema {
        private final String apocProcedure = "CALL apoc.export.cypher.schema(%s)";

        @Test
        public void testIllegalFSAccessExportCypherSchema() {
            setFileExport(false);
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, String.format(apocProcedure, "'./hello', {}"), (r) -> {
                    })
            );
            assertError(e, ApocConfig.EXPORT_TO_FILE_ERROR, RuntimeException.class, apocProcedure);
        }

        @Test
        public void testIllegalExternalFSAccessExportCypherSchema() {
            setFileExport(true);
            assertPathTraversalError(db, String.format(apocProcedure, "'../hello', {}"), Map.of(),
                    e -> assertError((Exception) e.get(ERROR_KEY), FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, (String) e.get(PROCEDURE_KEY)));
        }
    }
}