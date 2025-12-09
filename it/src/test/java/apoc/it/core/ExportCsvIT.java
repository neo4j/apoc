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

import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;

import apoc.csv.CsvTestUtil;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil.ApocPackage;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Session;

class ExportCsvIT {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeAll
    public static void beforeAll() {
        neo4jContainer = createEnterpriseDB(List.of(ApocPackage.CORE), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    void testExportQueryCsvIssue1188() {
        String copyright = "\n"
                + "(c) 2018 Hovsepian, Albanese, et al. \"\"ASCB(r),\"\" \"\"The American Society for Cell Biology(r),\"\" and \"\"Molecular Biology of the Cell(r)\"\" are registered trademarks of The American Society for Cell Biology.\n"
                + "2018\n"
                + "\n"
                + "This article is distributed by The American Society for Cell Biology under license from the author(s). Two months after publication it is available to the public under an Attribution-Noncommercial-Share Alike 3.0 Unported Creative Commons License.\n"
                + "\n";
        String pk = "5921569";
        session.executeWrite(tx -> tx.run(
                        "CREATE (n:Document{pk:$pk, copyright: $copyright})", map("copyright", copyright, "pk", pk))
                .consume());
        String query = "MATCH (n:Document{pk:'5921569'}) return n.pk as pk, n.copyright as copyright";
        testCall(
                session,
                "CALL apoc.export.csv.query($query, null, $config)",
                map("query", query, "config", map("stream", true)),
                (r) -> {
                    List<String[]> csv = CsvTestUtil.toCollection(r.get("data").toString());
                    Assertions.assertEquals(2, csv.size());
                    Assertions.assertArrayEquals(new String[] {"pk", "copyright"}, csv.get(0));
                    Assertions.assertArrayEquals(new String[] {"5921569", copyright}, csv.get(1));
                });
        session.executeWrite(tx -> tx.run("MATCH (d:Document) DETACH DELETE d").consume());
    }
}
