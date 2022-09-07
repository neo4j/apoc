package apoc.warmup;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Session;

import java.util.List;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class WarmupEnterpriseTest {

    @Test(expected = RuntimeException.class)
    public void testWarmupIsntAllowedWithOtherStorageEngines() {
        Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig(GraphDatabaseInternalSettings.include_versions_under_development.name(), "true")
                .withNeo4jConfig(GraphDatabaseSettings.db_format.name(), "freki");
        neo4jContainer.start();
        Session session = neo4jContainer.getSession();

        testCall(session, "CALL apoc.warmup.run()", (r) -> {});

        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testWarmupOnEnterpriseStorageEngine() {
        Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)
                .withNeo4jConfig(GraphDatabaseInternalSettings.include_versions_under_development.name(), "true")
                .withNeo4jConfig(GraphDatabaseSettings.db_format.name(), "high_limit");
        neo4jContainer.start();
        Session session = neo4jContainer.getSession();

        testCall(session, "CALL apoc.warmup.run(true,true,true)", r -> {
            assertEquals(true, r.get("indexesLoaded"));
            assertNotEquals( 0L, r.get("indexPages") );
        });

        session.close();
        neo4jContainer.close();
    }
}
