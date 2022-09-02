package apoc.warmup;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Session;

import java.util.List;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;

public class WarmupEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig(GraphDatabaseInternalSettings.include_versions_under_development.name(), "true")
                .withNeo4jConfig(GraphDatabaseSettings.db_format.name(), "freki");
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @Test(expected = RuntimeException.class)
    public void testWarmupIsntAllowedWithOtherStorageEngines() {
        testCall(session, "CALL apoc.warmup.run()", (r) -> {});
    }
}
