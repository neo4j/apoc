/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.trigger;

import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.trigger.TriggerTestUtil.TIMEOUT;
import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

import apoc.nodes.Nodes;
import apoc.util.TestUtil;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EnterpriseImpermanentDbmsRule;

public class TriggerEnterpriseDbTest {
    // we cannot set via apocConfig().setProperty(apoc.trigger.enabled, ...) in `@Before`, because is too late
    @ClassRule
    public static final ProvideSystemProperty systemPropertyRule =
            new ProvideSystemProperty(APOC_TRIGGER_ENABLED, String.valueOf(true));

    @Rule
    public DbmsRule db = new EnterpriseImpermanentDbmsRule().withSetting(procedure_unrestricted, List.of("apoc*"));

    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, TriggerNewProcedures.class, Nodes.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testStopStartOfTrigger() {
        GraphDatabaseService sysDb = db.getManagementService().database("system");
        GraphDatabaseService neo4jDb = db.getManagementService().database("neo4j");

        neo4jDb.executeTransactionally("CREATE (:Counter {count:0})");
        neo4jDb.executeTransactionally("CREATE (f:Foo)");
        final String name = "count-removals";
        String query = "MATCH (c:Counter) SET c.count = c.count + size([f IN $deletedNodes WHERE id(f) > 0])";

        sysDb.executeTransactionally(
                "CALL apoc.trigger.install('neo4j', $name, $query, {})", Map.of("name", name, "query", query));

        db.getManagementService().shutdownDatabase("neo4j");
        db.getManagementService().startDatabase("neo4j");

        // Re-fetch db
        neo4jDb = db.getManagementService().database("neo4j");
        neo4jDb.executeTransactionally("MATCH (f:Foo) DELETE f");
        testCallEventually(
                neo4jDb,
                "MATCH (c:Counter) RETURN c.count as count",
                (row) -> assertEquals(1L, row.get("count")),
                TIMEOUT);
    }
}
