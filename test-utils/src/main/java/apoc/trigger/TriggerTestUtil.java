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

import static apoc.util.TestUtil.testCallEventually;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.neo4j.graphdb.GraphDatabaseService;

public class TriggerTestUtil {
    public static final long TIMEOUT = 10L;
    public static final long TRIGGER_DEFAULT_REFRESH = 3000;

    public static void awaitTriggerDiscovered(GraphDatabaseService db, String name, String query) {
        awaitTriggerDiscovered(db, name, query, false);
    }

    public static void awaitTriggerDiscovered(GraphDatabaseService db, String name, String query, boolean paused) {
        testCallEventually(
                db,
                "CALL apoc.trigger.list() YIELD name, query, paused WHERE name = $name RETURN query, paused",
                Map.of("name", name),
                row -> {
                    assertEquals(query, row.get("query"));
                    assertEquals(paused, row.get("paused"));
                },
                TIMEOUT);
    }
}
