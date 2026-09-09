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
package apoc.cypher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class CypherUtilsTest {

    @Test
    void withParamMappingLeavesOrdinaryKeysUnaffected() {
        String result = CypherUtils.withParamMapping("RETURN 1", List.of("a", "b"));
        assertEquals(" WITH  $`a` as `a` ,  $`b` as `b` RETURN 1", result);
    }

    @Test
    void withParamMappingEscapesEmbeddedBackticksByDoubling() {
        // a single backtick in a key must be doubled so it stays a literal
        // character inside the quoted identifier instead of terminating it
        String result = CypherUtils.withParamMapping("RETURN 1", List.of("a`b"));
        assertEquals(" WITH  $`a``b` as `a``b` RETURN 1", result);
    }

    @Test
    void withParamMappingNeverProducesAnUnescapedBacktickForMaliciousKeys() {
        // this key is a Cypher-injection payload: if backticks weren't escaped,
        // it would close the quoted identifier and splice `DETACH DELETE` into the query text
        String maliciousKey = "z` AS zz MATCH (n) DETACH DELETE n WITH 1 AS q //ignored";
        String result = CypherUtils.withParamMapping("RETURN 1 AS r", List.of(maliciousKey));

        // every backtick in the malicious key must have been doubled (escaped),
        // so together with the two wrapping backticks the total count per occurrence is even,
        // meaning no lone backtick is ever left to break out of the identifier context
        long backtickCount = result.chars().filter(c -> c == '`').count();
        long backticksInKey = maliciousKey.chars().filter(c -> c == '`').count();
        // the key is spliced in twice (once as the $-accessor, once as the alias)
        long expected = 2 * (2 + 2 * backticksInKey);
        assertEquals(expected, backtickCount);

        // the injected clause must not appear as literal, unquoted query text:
        // it must be fully contained inside a backtick-quoted identifier
        assertTrue(result.contains("`" + maliciousKey.replace("`", "``") + "`"));
    }

    @Test
    void withParamMappingReturnsFragmentUnchangedWhenNoKeys() {
        String result = CypherUtils.withParamMapping("RETURN 1", List.of());
        assertEquals("RETURN 1", result);
    }
}
