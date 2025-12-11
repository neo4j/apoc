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
package apoc.it.core;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import apoc.util.TestUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Session;

class MetaEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeAll
    public static void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), !TestUtil.isRunningInCI());
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    public static boolean hasRecordMatching(
            List<Map<String, Object>> records, Predicate<Map<String, Object>> predicate) {
        return records.stream().filter(predicate).count() > 0;
    }

    public static List<Map<String, Object>> gatherRecords(Iterator<Map<String, Object>> r) {
        List<Map<String, Object>> rows = new ArrayList<>();
        while (r.hasNext()) {
            Map<String, Object> row = r.next();
            rows.add(row);
        }
        return rows;
    }

    @Test
    void testNodeTypePropertiesBasic() {
        session.executeWriteWithoutResult(tx -> tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.s) IS NOT NULL;"));
        session.executeWriteWithoutResult(
                tx -> tx.run("CREATE (:Foo { l: 1, s: 'foo', d: datetime(), ll: ['a', 'b'], dl: [2.0, 3.0] });"));
        testResult(session, "CALL apoc.meta.nodeTypeProperties();", (r) -> {
            List<Map<String, Object>> records = gatherRecords(r);
            Assertions.assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("nodeType").equals(":`Foo`")
                            && ((List) m.get("nodeLabels")).get(0).equals("Foo")
                            && m.get("propertyName").equals("s")
                            && m.get("mandatory").equals(true)));

            Assertions.assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("propertyName").equals("s")
                            && ((List) m.get("propertyTypes")).get(0).equals("String")));

            Assertions.assertTrue(hasRecordMatching(
                    records,
                    m -> m.get("nodeType").equals(":`Foo`")
                            && ((List) m.get("nodeLabels")).get(0).equals("Foo")
                            && m.get("propertyName").equals("dl")
                            && m.get("mandatory").equals(false)));

            Assertions.assertEquals(5, records.size());
        });
    }
}
