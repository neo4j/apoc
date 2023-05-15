package apoc.load;

import apoc.util.CompressionAlgo;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.apache.commons.io.FileUtils;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.LongStream;

import static apoc.export.util.LimitedSizeInputStream.SIZE_EXCEEDED_ERROR;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class LoadCoreEnterpriseTest {
    private static final File directory = new File("target/import");
    public static final String COMPRESSED_JSON_FILE = "compressedFile.";
    public static final String COMPRESSED_XML_FILE = "compressedXmlFile.";

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void beforeClass() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig("server.memory.heap.max_size", "2GB");
        neo4jContainer.start();

        assertTrue(neo4jContainer.isRunning());

        loopAllCompressionAlgos(algo -> {
            writeCompressedFile(COMPRESSED_JSON_FILE + algo.name(), algo, writer -> {
                writer.write("{\"test\":\"");
                LongStream.range(0, 9999999L)
                        .forEach(__ -> writer.write("000000000000000000000000000000000000000000000000000000000000"));
                writer.write("\"}");
            });
        });

        loopAllCompressionAlgos(algo -> {
            writeCompressedFile(COMPRESSED_XML_FILE + algo.name(), algo, writer -> {
                writer.write("<?xml version=\"1.0\"?><catalog>");
                LongStream.range(0, 9999999L)
                        .forEach(__ -> writer.write("000000000000000000000000000000000000000000000000000000000000"));
                writer.write("</catalog>");
            });
        });

        session = neo4jContainer.getSession();
    }

    private static void writeCompressedFile(String fileName, CompressionAlgo algo, Consumer<PrintWriter> supplier) {
        try {
            File file = new File(directory, fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(algo.getOutputStream(fileOutputStream));
            PrintWriter writer = new PrintWriter(bufferedOutputStream);

            supplier.accept(writer);

            writer.close();
            bufferedOutputStream.close();
            fileOutputStream.close();

            moveFileToContainer(fileName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void moveFileToContainer(String name) throws IOException {
        File url = new File(directory, name).getCanonicalFile();
        FileUtils.copyFile(url, new File(TestContainerUtil.importFolder, name));
    }

    @AfterClass
    public static void afterClass() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testLoadJsonShouldPreventZipBombAttack() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall( session, "CALL apoc.load.json($url)",
                        Map.of("url", "https://github.com/iamtraction/ZOD/raw/master/42.zip"),
                        r -> {} )
        );

        Assertions.assertThat(e.getMessage()).contains("Invalid UTF-8 start byte");
    }

    @Test
    public void testLoadXmlShouldPreventZipBombAttack() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall( session, "CALL apoc.load.xml($url)",
                        Map.of("url", "https://github.com/iamtraction/ZOD/raw/master/42.zip"),
                        r -> {} )
        );

        Assertions.assertThat(e.getMessage()).contains("Invalid UTF-8 start byte");
    }

    @Test
    public void testLoadJsonShouldPreventCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            String algoName = algo.name();
            testMaxSizeExceeded("CALL apoc.load.json($file, '', {compression: $compression})",
                    Map.of("file", COMPRESSED_JSON_FILE + algoName, "compression", algoName));
        });
    }

    @Test
    public void testLoadXmlShouldPreventCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            try {
                String algoName = algo.name();
                byte[] bytes = bytesFromFile(COMPRESSED_XML_FILE, algoName);

                testMaxSizeExceeded("CALL apoc.load.xml($file, null, {compression: $compression})",
                        Map.of("file", bytes, "compression", algoName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadJsonShouldPreventBinaryCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            try {
                String algoName = algo.name();
                byte[] bytes = bytesFromFile(COMPRESSED_JSON_FILE, algoName);

                testMaxSizeExceeded("CALL apoc.load.json($file, '', {compression: $compression})",
                        Map.of("file", bytes, "compression", algoName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testLoadXmlShouldPreventBinaryCompressionBombAttack() {
        loopAllCompressionAlgos(algo -> {
            String algoName = algo.name();
            testMaxSizeExceeded("CALL apoc.load.xml($file, null, {compression: $compression})",
                    Map.of("file", COMPRESSED_XML_FILE + algoName, "compression", algoName));
        });
    }

    @Test
    public void testLoadShouldPreventZipBombAttackUtilDecompress() {
        loopAllCompressionAlgos(algo -> {
            try {
                String algoName = algo.name();
                byte[] bytes = bytesFromFile(COMPRESSED_JSON_FILE, algoName);

                testMaxSizeExceeded("RETURN apoc.util.decompress($file, {compression: $algo})",
                        Map.of("file", bytes, "algo", algoName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void loopAllCompressionAlgos(Consumer<CompressionAlgo> compressionAlgoConsumer) {
        Arrays.stream(CompressionAlgo.values())
                .filter(algo -> !algo.equals(CompressionAlgo.NONE))
                .forEach(compressionAlgoConsumer);
    }

    private static byte[] bytesFromFile(String compressedXmlFile, String algoName) throws IOException {
        Path path = new File(directory, compressedXmlFile + algoName).toPath();
        byte[] bytes = Files.readAllBytes(path);
        return bytes;
    }

    private void testMaxSizeExceeded(String query, Map<String, Object> params) {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> testCall( session, query, params, r -> {} )
        );

        Assertions.assertThat(e.getMessage()).contains(SIZE_EXCEEDED_ERROR);
    }
}
