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

import static apoc.trigger.Trigger.SYS_DB_NON_WRITER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TriggerClusterRoutingTest {
    private static TestcontainersCausalCluster cluster;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster( List.of(TestContainerUtil.ApocPackage.CORE), 
                        3, 0,
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
    
    // TODO: fabric tests once the @SystemOnlyProcedure annotation is added to Neo4j

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String addTrigger = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        String triggerName = randomTriggerName();

        succeedsInLeader(addTrigger, triggerName, DEFAULT_DATABASE_NAME);
        failsInFollowers(addTrigger, triggerName, SYS_DB_NON_WRITER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String addTrigger = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        final String removeTrigger = "CALL apoc.trigger.remove($name)";
        String triggerName = randomTriggerName();

        succeedsInLeader(addTrigger, triggerName, DEFAULT_DATABASE_NAME);
        succeedsInLeader(removeTrigger, triggerName, DEFAULT_DATABASE_NAME);
        failsInFollowers(removeTrigger, triggerName, SYS_DB_NON_WRITER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String installTrigger = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        String triggerName = randomTriggerName();
        succeedsInLeader(installTrigger, triggerName, SYSTEM_DATABASE_NAME);
        failsInFollowers(installTrigger, triggerName, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String installTrigger = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        final String dropTrigger = "CALL apoc.trigger.drop('neo4j', $name)";
        String triggerName = randomTriggerName();

        succeedsInLeader(installTrigger, triggerName, SYSTEM_DATABASE_NAME);
        succeedsInLeader(dropTrigger, triggerName, SYSTEM_DATABASE_NAME);
        failsInFollowers(dropTrigger, triggerName, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    private static void succeedsInLeader(String triggerOperation, String triggerName, String dbName) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(3, members.size());
        for (Neo4jContainerExtension container: members) {
            final Driver driver = container.getDriver();
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            final String address = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");

            if (dbIsWriter(session, SYSTEM_DATABASE_NAME, address)) {
                testCall( session, triggerOperation,
                        Map.of("name", triggerName),
                        row -> assertEquals(triggerName, row.get("name")) );
            }
        }
    }

    private static void failsInFollowers(String triggerOperation, String triggerName, String expectedError, String dbName) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(3, members.size());
        for (Neo4jContainerExtension container: members) {
            final Driver driver = container.getDriver();
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            final String address = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");

            if (!dbIsWriter(session, SYSTEM_DATABASE_NAME, address)) {
                try {
                    testCall(session, triggerOperation,
                            Map.of("name", triggerName),
                            row -> fail("Should fail because of non writer trigger addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(expectedError));
                }
            }
        }
    }

    private static String randomTriggerName() {
        return UUID.randomUUID().toString();
    }

    private static boolean dbIsWriter(Session session, String dbName, String address) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $address",
                        Map.of("dbName", dbName, "address", address) )
                .single().get("writer")
                .asBoolean();
    }
}
