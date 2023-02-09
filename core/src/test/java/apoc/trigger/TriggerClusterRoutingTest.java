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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static apoc.trigger.Trigger.SYS_DB_NON_WRITER_ERROR;
import static apoc.trigger.TriggerNewProcedures.TRIGGER_NOT_ROUTED_ERROR;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TriggerClusterRoutingTest {
    private static TestcontainersCausalCluster cluster;
    private static List<Neo4jContainerExtension> clusterMembers;

    @BeforeClass
    public static void setupCluster() {
        cluster = TestContainerUtil
                .createEnterpriseCluster(List.of(TestContainerUtil.ApocPackage.CORE),
                        3, 0,
                        Collections.emptyMap(), 
                        Map.of( "NEO4J_dbms_routing_enabled", "true", 
                                "apoc.trigger.enabled", "true" )
                );

        clusterMembers = cluster.getClusterMembers();
        assertEquals(3, clusterMembers.size());
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

    @Test
    public void testTriggerShowAllowedInAllSystemInstances() {
        final String installTrigger = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        final String showTrigger = "CALL apoc.trigger.show('neo4j')";

        String triggerName = randomTriggerName();
        Consumer<Iterator<Map<String, Object>>> checkTriggerIsListed = rows -> {
            AtomicBoolean showedTrigger = new AtomicBoolean(false);
            rows.forEachRemaining(row -> {
                if (row.get("name").equals(triggerName))
                    showedTrigger.set(true);
            });

            assertTrue(showedTrigger.get());
        };

        succeedsInLeader(installTrigger, triggerName, SYSTEM_DATABASE_NAME);
        succeedsInLeader(showTrigger, triggerName, SYSTEM_DATABASE_NAME, checkTriggerIsListed);
        succeedsInFollowers(showTrigger, triggerName, SYSTEM_DATABASE_NAME, checkTriggerIsListed);
    }

    private static void succeedsInLeader(String triggerOperation, String triggerName, String dbName) {
        for (Neo4jContainerExtension instance: clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                testCall(session, triggerOperation,
                        Map.of("name", triggerName),
                        row -> assertEquals(triggerName, row.get("name")) );
            }
        }
    }

    private static void succeedsInLeader(String triggerOperation, String triggerName, String dbName, Consumer<Iterator<Map<String, Object>>> assertion) {
        for (Neo4jContainerExtension instance: clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                testResult(session, triggerOperation, Map.of("name", triggerName), assertion);
            }
        }
    }

    private static void succeedsInFollowers(String triggerOperation, String triggerName, String dbName, Consumer<Iterator<Map<String, Object>>> assertion) {
        for (Neo4jContainerExtension instance: clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (!dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
                testResult(session, triggerOperation, Map.of("name", triggerName), assertion);
            }
        }
    }

    private static void failsInFollowers(String triggerOperation, String triggerName, String expectedError, String dbName) {
        for (Neo4jContainerExtension instance: clusterMembers) {
            Session session = getSessionForDb(instance, dbName);

            if (!dbIsWriter(SYSTEM_DATABASE_NAME, session, getBoltAddress(instance))) {
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

    private static boolean dbIsWriter(String dbName, Session session, String boltAddress) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $boltAddress",
                        Map.of("dbName", dbName, "boltAddress", boltAddress) )
                .single().get("writer")
                .asBoolean();
    }

    private static String getBoltAddress(Neo4jContainerExtension instance) {
        return instance.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
    }

    private static Session getSessionForDb(Neo4jContainerExtension instance, String dbName) {
        final Driver driver = instance.getDriver();
        return driver.session(SessionConfig.forDatabase(dbName));
    }
}
