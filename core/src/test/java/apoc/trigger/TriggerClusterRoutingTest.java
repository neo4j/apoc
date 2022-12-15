package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class TriggerClusterRoutingTest {
    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster( List.of(TestContainerUtil.ApocPackage.CORE), 
                        3, 1, 
                        Collections.emptyMap(), 
                        Map.of( "NEO4J_dbms_routing_enabled", "true", 
                                "apoc.trigger.enabled", "true" )
                );
    }

    @AfterClass
    public static void bringDownCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }
    
    // TODO: fabric tests

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        triggerInSysWriterMemberCommon(query, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.remove($name)";
        triggerInSysWriterMemberCommon(query, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysWriterMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        triggerInSysWriterMemberCommon(query, SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysWriterMember() {
        final String query = "CALL apoc.trigger.drop('neo4j', $name)";
        triggerInSysWriterMemberCommon(query, SYSTEM_DATABASE_NAME);
    }

    private static void triggerInSysWriterMemberCommon(String query, String dbName) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members
            final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
            final Driver driver = container.getDriver();
            if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            final String name = UUID.randomUUID().toString();
            testCall( session, query,
                    Map.of("name", name),
                    row -> assertEquals(name, row.get("name")) );
        }
    }
}
