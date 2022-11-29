package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.s3.S3BaseTest;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.s3.S3TestUtil.getS3Object;
import static junit.framework.TestCase.assertEquals;


public class ExportS3PerformanceTest extends S3BaseTest {
    private final static int REPEAT_TEST = 3;

    private void verifyFileUploaded(String s3Url, String fileName) throws IOException {
        final S3Object s3Object = getS3Object(s3Url);
        assertEquals(fileName, s3Object.getKey());
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        baseBeforeClass();
        
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);

        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testExportAllCsvS3() throws Exception {
        System.out.println("Data creation started.");
        final String query = Util.readResourceFile("movies.cypher");
        IntStream.range(0, 5000).forEach(__-> db.executeTransactionally(query));
        System.out.println("Data creation finished.");

        System.out.println("Test started.");
        for (int repeat=1; repeat<=REPEAT_TEST; repeat++) {
            String fileName = String.format("performanceTest_%d.csv", repeat);
            String s3Url = s3Container.getUrl(fileName);

            // Run the performance testing
            final Instant start = Instant.now();
            TestUtil.testCall(db, "CALL apoc.export.csv.all($s3,null)",
                    map("s3", s3Url),
                    (r) -> {});
            final Instant end = Instant.now();
            final Duration diff = Duration.between(start, end);
            System.out.println("Time to upload" + ": " + diff.toMillis() + " ms.");

            // Verify the file was successfully uploaded.
            verifyFileUploaded(s3Url, fileName);
        }
        System.out.println("Test finished.");
    }
}
