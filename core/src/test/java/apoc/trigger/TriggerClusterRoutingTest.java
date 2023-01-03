package apoc.trigger;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestcontainersCausalCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
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
    
    // TODO: fabric tests once the @SystemOnlyProcedure annotation is added to Neo4j

    @Test
    public void testTriggerAddAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.add($name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, SYS_DB_NON_WRITER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerRemoveAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.remove($name)";
        triggerInSysLeaderMemberCommon(query, SYS_DB_NON_WRITER_ERROR, DEFAULT_DATABASE_NAME);
    }

    @Test
    public void testTriggerInstallAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.install('neo4j', $name, 'RETURN 1', {})";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    @Test
    public void testTriggerDropAllowedOnlyInSysLeaderMember() {
        final String query = "CALL apoc.trigger.drop('neo4j', $name)";
        triggerInSysLeaderMemberCommon(query, TRIGGER_NOT_ROUTED_ERROR, SYSTEM_DATABASE_NAME);
    }

    private static void triggerInSysLeaderMemberCommon(String query, String triggerNotRoutedError, String dbName) {
        final List<Neo4jContainerExtension> members = cluster.getClusterMembers();
        assertEquals(4, members.size());
        for (Neo4jContainerExtension container: members) {
            // we skip READ_REPLICA members
            final Driver driver = getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            Session session = driver.session(SessionConfig.forDatabase(dbName));
            final String address = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
            if (dbIsWriter(session, SYSTEM_DATABASE_NAME, address)) {
                final String name = UUID.randomUUID().toString();
                testCall( session, query,
                        Map.of("name", name),
                        row -> assertEquals(name, row.get("name")) );
            } else {
                try {
                    testCall(session, query,
                            Map.of("name", UUID.randomUUID().toString()),
                            row -> fail("Should fail because of non writer trigger addition"));
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    assertTrue("The actual message is: " + errorMsg, errorMsg.contains(triggerNotRoutedError));
                }
            }
        }
    }

    @Test
    public void testTriggersAllowedOnlyWithAdmin() {
        for (Neo4jContainerExtension container: cluster.getClusterMembers()) {
            // we skip READ_REPLICA members
            final Driver driver = getDriverIfNotReplica(container);
            if (driver == null) {
                continue;
            }
            final String address = container.getEnvMap().get("NEO4J_dbms_connector_bolt_advertised__address");
            String noAdminUser = "nonadmin";
            String noAdminPwd = "test1234";
            try (Session sysSession = driver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME))) {
                if (!dbIsWriter(sysSession, SYSTEM_DATABASE_NAME, address)) {
                    return;
                }
                sysSession.run(String.format("CREATE USER %s SET PASSWORD '%s' SET PASSWORD CHANGE NOT REQUIRED",
                        noAdminUser, noAdminPwd));
            }

            try (Driver userDriver = GraphDatabase.driver(cluster.getURI(), AuthTokens.basic(noAdminUser, noAdminPwd))) {
                
                try (Session sysUserSession = userDriver.session(SessionConfig.forDatabase(SYSTEM_DATABASE_NAME))) {
                    failsWithNonAdminUser(sysUserSession, "apoc.trigger.install", "call apoc.trigger.install('neo4j', 'qwe', 'return 1', {})");
                    failsWithNonAdminUser(sysUserSession, "apoc.trigger.drop", "call apoc.trigger.drop('neo4j', 'qwe')");
                    failsWithNonAdminUser(sysUserSession, "apoc.trigger.dropAll", "call apoc.trigger.dropAll('neo4j')");
                    failsWithNonAdminUser(sysUserSession, "apoc.trigger.stop", "call apoc.trigger.stop('neo4j', 'qwe')");
                    failsWithNonAdminUser(sysUserSession, "apoc.trigger.start", "call apoc.trigger.start('neo4j', 'qwe')");
                    failsWithNonAdminUser(sysUserSession, "apoc.trigger.show", "call apoc.trigger.show('neo4j')");
                }
                
                try (Session neo4jUserSession = userDriver.session(SessionConfig.forDatabase(DEFAULT_DATABASE_NAME))) {
                    failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.add", "call apoc.trigger.add('abc', 'return 1', {})");
                    failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.remove", "call apoc.trigger.remove('abc')");
                    failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.removeAll", "call apoc.trigger.removeAll");
                    failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.pause", "call apoc.trigger.pause('abc')");
                    failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.resume", "call apoc.trigger.resume('abc')");
                    failsWithNonAdminUser(neo4jUserSession, "apoc.trigger.list", "call apoc.trigger.list");
                }
            }
        }
    }

    private static Driver getDriverIfNotReplica(Neo4jContainerExtension container) {
        final String readReplica = TestcontainersCausalCluster.ClusterInstanceType.READ_REPLICA.toString();
        final Driver driver = container.getDriver();
        if (readReplica.equals(container.getEnvMap().get("NEO4J_dbms_mode")) || driver == null) {
            return null;
        }
        return driver;
    }

    private static boolean dbIsWriter(Session session, String dbName, String address) {
        return session.run( "SHOW DATABASE $dbName WHERE address = $address",
                        Map.of("dbName", dbName, "address", address) )
                .single().get("writer")
                .asBoolean();
    }
    
    private void failsWithNonAdminUser(Session session, String procName, String query) {
        try {
            testCall(session, query,
                    row -> fail("Should fail because of non admin user") );
        } catch (Exception e) {
            String actual = e.getMessage();
            final String expected = String.format("Executing admin procedure '%s' permission has not been granted for user 'nonadmin'",
                    procName);
            assertTrue("Actual error message is: " + actual, actual.contains(expected));
        }
    }


}
