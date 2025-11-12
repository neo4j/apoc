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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CypherVersion;

class LogsUtilTest {

    @Test
    void shouldRedactPasswords() {
        String sanitized = LogsUtil.sanitizeQuery(
                Config.defaults(),
                "CREATE USER dummy IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED",
                CypherVersion.Cypher5);
        Assertions.assertEquals("CREATE USER dummy IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED", sanitized);
    }

    @Test
    void shouldReturnInputIfInvalidQuery() {
        String invalidQuery = "MATCH USER dummy IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED";
        String sanitized = LogsUtil.sanitizeQuery(Config.defaults(), invalidQuery, CypherVersion.Cypher5);

        Assertions.assertEquals(invalidQuery, sanitized);
    }

    @Test
    void whitespaceDeprecationSucceedsSanitization() {
        String sanitized = LogsUtil.sanitizeQuery(
                Config.defaults(),
                "CREATE USER dum\u0085my IF NOT EXISTS SET PASSWORD 'pass12345' CHANGE NOT REQUIRED",
                CypherVersion.Cypher5);
        Assertions.assertEquals(
                "CREATE USER `dum\u0085my` IF NOT EXISTS SET PASSWORD '******' CHANGE NOT REQUIRED", sanitized);
    }
}
