package apoc.load;

import apoc.util.CompressionAlgo;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import apoc.util.Utils;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Session;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertTrue;

@Ignore
public class LoadCoreEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void before() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        TestUtil.registerProcedure(db, LoadJson.class, Utils.class);
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true);
        neo4jContainer.start();

        // todo - s3 and hdfs tests

        // todo - create file with other compression algos


        // https://commons.apache.org/proper/commons-compress/examples.html
        String file = ClassLoader.getSystemResource("superFile.gzip").getFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file);
//        OutputStream outputStream = apoc.util.FileUtils.getOutputStream("superFile.gzip", new ExportConfig(Map.of("compression", "GZIP")));
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(CompressionAlgo.GZIP.getOutputStream(fileOutputStream));
        PrintWriter writer = new PrintWriter(bufferedOutputStream);

        writer.write("{\"test\":\"");

//        IntStream.range(0, 999)
        LongStream.range(0, 99999999L)
                .forEach(__ -> writer.write("000000000000000000000000000000000000000000000000000000000000"));

        writer.write("\"}");

        writer.close();
        bufferedOutputStream.close();
        fileOutputStream.close();

//        InputStream fin = Files.newInputStream(Paths.get("archive.tar.gz"));
//        BufferedInputStream in = new BufferedInputStream(fin);
//        OutputStream out = Files.newOutputStream(Paths.get("archive.tar"));
//        GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
//        final byte[] buffer = new byte[1000];
//        int n = 0;
//        while (-1 != (n = gzIn.read(buffer))) {
//            out.write(buffer, 0, n);
//        }
//        out.close();
//        gzIn.close();


        // todo - util??
        extracted("42.zip");
        extracted("superZip.zip");
        extracted("superZip1.zip");
        extracted("zbsm.zip");
        extracted("42.zip.gz");
        extracted("surprise.gz");
        extracted("superFile.gzip");


        session = neo4jContainer.getSession();

        assertTrue(neo4jContainer.isRunning());
    }

    private static void extracted(String name) throws IOException {
        URL url = ClassLoader.getSystemResource(name);
        FileUtils.copyURLToFile(url, new File(TestContainerUtil.importFolder, name));
    }

    @AfterClass
    public static void afterClass() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testLoadShouldPreventZipBombAttack() {

        testCall(session, "CALL apoc.load.json($file)",
                Map.of("file", "superZip1.zip!zbsm.zip"),
                r -> {}
        );

//        testCall(session, "CALL apoc.load.json($file)",
//                Map.of("file", "superZip.zip"),
//                r -> {}
//        );
    }

    @Test
    public void testLoadShouldPreventZipBombAttack11() {
        TestUtil.testCall(db, "CALL apoc.load.json($file, '', {compression: 'GZIP'})",
                Map.of("file", ClassLoader.getSystemResource("superFile.gzip").getPath()),
                r -> {
                    System.out.println("r = " + r);
                }
        );
    }

    @Test
    public void testLoadShouldPreventZipBombAttack11Bytes() throws IOException {
        String path = ClassLoader.getSystemResource("superFile.gzip").getPath();
        byte[] bytes = Files.readAllBytes(Paths.get(path));

        TestUtil.testCall(db, "CALL apoc.load.json($file, '', {compression: 'GZIP'})",
                Map.of("file", bytes),
                r -> {
                    System.out.println("r = " + r);
                }
        );
    }

    @Test
    public void testLoadShouldPreventZipBombAttack1() {
        testCall(session, "CALL apoc.load.json($file, '', {compression: 'GZIP'})",
                Map.of("file", "superFile.gzip"),
                r -> {
                    System.out.println("r = " + r);
                }
        );
    }

    @Test
    public void testLoadShouldPreventZipBombAttackUtilDecompress() throws IOException {
        String path = ClassLoader.getSystemResource("superFile.gzip").getPath();
        byte[] bytes = Files.readAllBytes(Paths.get(path));

        TestUtil.testCall(db, "RETURN apoc.util.decompress($file)",
                Map.of("file", bytes),
                r -> {
                    System.out.println("r = " + r);
                }
        );
    }
}
