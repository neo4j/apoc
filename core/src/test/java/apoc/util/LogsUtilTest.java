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
package apoc.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CypherVersion;

public class LogsUtilTest {

    @Test
    public void shouldRedactPasswords() {
        String sanitized = LogsUtil.sanitizeQuery(
                Config.defaults(), "CREATE USER dummy IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED", CypherVersion.Cypher5);
        assertEquals(sanitized, "CREATE USER dummy IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED");
    }

    @Test
    public void shouldSanitizeCypher25Query() {
        String sanitized = LogsUtil.sanitizeQuery(
                Config.defaults(), "RETURN CASE $x WHEN IN ['a', 'b'] THEN true ELSE false END AS res", CypherVersion.Cypher25);
        assertEquals(sanitized, "CREATE USER dummy IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED");
    }

    @Test
    public void shouldReturnInputIfInvalidQuery() {
        String invalidQuery = "MATCH USER dummy IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED";
        String sanitized = LogsUtil.sanitizeQuery(Config.defaults(), invalidQuery, CypherVersion.Cypher5);

        assertEquals(sanitized, invalidQuery);
    }

    @Test
    public void whitespaceDeprecationSucceedsSanitization() {
        String sanitized = LogsUtil.sanitizeQuery(
                Config.defaults(),
                "CREATE USER dum\u0085my IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED",
                CypherVersion.Cypher5);
        assertEquals(sanitized, "CREATE USER `dum\u0085my` IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED");
    }
}
