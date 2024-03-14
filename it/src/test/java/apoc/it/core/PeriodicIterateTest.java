package apoc.it.core;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createDB;
import static apoc.util.TestContainerUtil.dockerImageForNeo4j;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class PeriodicIterateTest {

    // The Query Log is only accessible on Enterprise
    @Test
    public void check_metadata_in_batches() {
        try {
            Neo4jContainerExtension neo4jContainer = createDB(
                    TestContainerUtil.Neo4jVersion.ENTERPRISE, List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI())
                    .withNeo4jConfig("dbms.transaction.timeout", "60s");

            neo4jContainer.start();


            Session session = neo4jContainer.getSession();
            session.run(
                    "CALL apoc.periodic.iterate(\"MATCH (p:Person) RETURN p\"," +
                            "\"SET p.name='test'\"," +
                            "{batchSize:1, parallel:false})",
                    TransactionConfig.builder()
                            .withMetadata(Map.of("shouldAppear", "inBatches"))
                            .build()
                    ).stream().count();
            var queryLogs = neo4jContainer.queryLogs();
            assertTrue(queryLogs.contains("SET p.name='test' - {_batch: [], _count: 0} - runtime=pipelined - {shouldAppear: 'inBatches'}"));
            session.close();
            neo4jContainer.close();
        } catch (Exception ex) {
            // if Testcontainers wasn't able to retrieve the docker image we ignore the test
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            } else if (!TestUtil.isRunningInCI()) {
                fail("The docker image " + dockerImageForNeo4j(TestContainerUtil.Neo4jVersion.ENTERPRISE)
                        + " could not be loaded. Check whether it's available locally / in the CI. Exception:"
                        + ex);
            }
        }
    }
}
