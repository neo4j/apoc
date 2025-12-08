package apoc.it.core;

import static apoc.util.TestContainerUtil.createNeo4jContainer;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Session;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDockerTestBase {
    Neo4jContainerExtension neo4jContainer;
    Session session;

    abstract TestContainerUtil.Neo4jVersion neo4jEdition();

    @BeforeAll
    void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createNeo4jContainer(
                List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI(), neo4jEdition());
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    void afterAll() {
        try {
            if (session != null) session.close();
        } finally {
            if (neo4jContainer != null) neo4jContainer.close();
        }
    }
}
