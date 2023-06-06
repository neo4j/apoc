package apoc.it.core;

import apoc.util.Neo4jContainerExtension;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;

import static apoc.cypher.CypherTestUtil.SET_AND_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.SIMPLE_RETURN_QUERIES;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSetAndReturnResults;
import static apoc.cypher.CypherTestUtil.testRunProcedureWithSimpleReturnResults;
import static apoc.util.TestContainerUtil.*;

public class CypherEnterpriseTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testRunManyWithSetAndResults() {
        String query = "CALL apoc.cypher.runMany($statement, {})";
        Map<String, Object> params = Map.of("statement", SET_AND_RETURN_QUERIES);

        testRunProcedureWithSetAndReturnResults(session, query, params);
    }

    @Test
    public void testRunManyWithResults() {
        String query = "CALL apoc.cypher.runMany($statement, {})";
        Map<String, Object> params = Map.of("statement", SIMPLE_RETURN_QUERIES);

        testRunProcedureWithSimpleReturnResults(session, query, params);
    }

}
