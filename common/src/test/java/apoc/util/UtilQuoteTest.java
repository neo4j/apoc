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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension
public class UtilQuoteTest {

    @Inject
    GraphDatabaseService db;

    static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of("abc", true),
                Arguments.of("_id", true),
                Arguments.of("some_var", true),
                Arguments.of("$lock", false),
                Arguments.of("has$inside", false),
                Arguments.of("ähhh", true),
                Arguments.of("rübe", true),
                Arguments.of("rådhuset", true),
                Arguments.of("1first", false),
                Arguments.of("first1", true),
                Arguments.of("a weird identifier", false),
                Arguments.of("^n", false),
                Arguments.of("$$n", false),
                Arguments.of(" var ", false),
                Arguments.of("foo.bar.baz", false));
    }

    @ParameterizedTest(name = "should quote if needed for identifier=''{0}'' (avoidQuote={1})")
    @MethodSource("parameters")
    void shouldQuoteIfNeededForUsageAsParameterName(String identifier, boolean shouldAvoidQuote) {
        db.executeTransactionally(String.format("CREATE (n:TestNode) SET n.%s = true", Util.quote(identifier)));
        // If the query did not fail entirely, did it create the expected property?
        TestUtil.testCallCount(db, String.format("MATCH (n:TestNode) WHERE n.`%s` RETURN id(n)", identifier), 1);
    }

    @ParameterizedTest(name = "should not quote when avoidQuote is true for identifier=''{0}''")
    @MethodSource("parameters")
    void shouldNotQuoteWhenAvoidQuoteIsTrue(String identifier, boolean shouldAvoidQuote) {
        final String expectedIdentifier = shouldAvoidQuote ? identifier : '`' + identifier + '`';
        assertEquals(expectedIdentifier, Util.quote(identifier));
    }
}
