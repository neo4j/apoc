package apoc.export.csv;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.csv.ExportCsvTest.CALL_DATA;
import static apoc.export.csv.ExportCsvTest.CALL_GRAPH;
import static apoc.export.csv.ExportCsvTest.assertResults;
import static apoc.export.csv.ExportCsvTest.readFile;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

// Created to not affect ExportCsvTest results
public class ExportCsvUseTypeTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, ExportCsvTest.directory.toPath().toAbsolutePath());


    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        db.executeTransactionally("""
                CREATE (n:SuperNode { one: datetime('2018-05-10T10:30[Europe/Berlin]'), two: time('18:02:33'), three: localtime('17:58:30'), four: localdatetime('2021-06-08'), five: date('2020'), six: duration({months: 5, days: 1.5}), seven : '2020'})
                WITH n CREATE (n)-[:REL_TYPE {rel: point({x: 56.7, y: 12.78, crs: 'cartesian'})}]->(m:AnotherNode)""");
        
        try(Transaction tx = db.beginTx()) {
            final Node node = tx.findNodes(Label.label("AnotherNode")).next();
            // force property type
            node.setProperty("alpha", (short) 1);
            node.setProperty("beta", "qwerty".getBytes());
            node.setProperty("gamma", 'A');
            node.setProperty("epsilon", 1);
            node.setProperty("zeta", 1.1F);
            node.setProperty("eta", 1L);
            node.setProperty("theta", 10.1D);
            node.setProperty("iota", "bar");
            tx.commit();
        }
    }

    @Test
    public void testExportCsvAll() {
        String fileName = "output.csv";
        testCall(db, "CALL apoc.export.csv.all($file, {useTypes: true, quotes: 'none'})", map("file", fileName),
                (r) -> assertResults(fileName, r, "database", 2L, 1L, 16L, true));
        final String expected = Util.readResourceFile("manyTypes.csv");
        assertEquals(expected, readFile(fileName));

        // -- streaming mode
        String statement = "CALL apoc.export.csv.all(null, {stream:true, useTypes: true, quotes: 'none'})";
        testCall(db, statement, (r) -> assertEquals(expected, r.get("data")));
    }

    @Test
    public void testExportCsvGraph() {
        String fileName = "output.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", "none");
        testCall(db, CALL_GRAPH, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "graph", 2L, 1L, 16L, true));
        final String expected = Util.readResourceFile("manyTypes.csv");
        assertEquals(expected, readFile(fileName));
    }

    @Test
    public void testExportCsvData() {
        String fileName = "output.csv";
        Map<String, Object> config = Map.of("useTypes", true, "quotes", "none");
        testCall(db, CALL_DATA, map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "data", 2L, 1L, 16L, true));
        final String expected = Util.readResourceFile("manyTypes.csv");
        assertEquals(expected, readFile(fileName));
    }
}
