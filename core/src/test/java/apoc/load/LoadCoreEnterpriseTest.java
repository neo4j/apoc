package apoc.load;

import apoc.util.CompressionAlgo;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

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

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertTrue;


public class LoadCoreEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeClass() throws Exception {

        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true);
        neo4jContainer.start();



        // todo - create file with other compression algos


        // https://commons.apache.org/proper/commons-compress/examples.html
        String file = ClassLoader.getSystemResource("superFile.gzip").getFile();
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(CompressionAlgo.GZIP.getOutputStream(fileOutputStream));
        PrintWriter writer = new PrintWriter(bufferedOutputStream);

        writer.write("{\"test\":\"");

        LongStream.range(0, 99999999L)
                .forEach(__ -> writer.write("000000000000000000000000000000000000000000000000000000000000"));

        writer.write("\"}");

        writer.close();
        bufferedOutputStream.close();
        fileOutputStream.close();


        // todo - util??
        moveFileToContainer("42.zip");
        moveFileToContainer("surprise.gz");
        moveFileToContainer("superFile.gzip");


        session = neo4jContainer.getSession();

        assertTrue(neo4jContainer.isRunning());
    }

    private static void moveFileToContainer(String name) throws IOException {
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
        testCall(session, "CALL apoc.load.json($file, '', {compression: 'GZIP'})",
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

        testCall(session, "CALL apoc.load.json($file, '', {compression: 'GZIP'})",
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
