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
package apoc.text;

import static apoc.util.MapUtil.map;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

/**
 * The `apoc.text` functions that evaluate a user-supplied regular expression must remain terminable, since
 * {@link java.util.regex.Matcher} never checks for interruption by itself.
 * <p>
 * The hostile pattern is `(x+x+)+y` against a long non-matching input, which backtracks cubically. The `(a+)+$` shape
 * commonly quoted for this weakness is pruned by the JDK and completes in under a millisecond, so a test built on it
 * would pass without exercising anything.
 */
@EnterpriseDbmsExtension
class StringsTerminationTest {

    private static final long TIMEOUT_SECONDS = 10L;

    private static final Map<String, Object> HOSTILE_INPUT = map("text", "x".repeat(20000) + "z", "regex", "(x+x+)+y");

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Strings.class);
    }

    @Test
    void terminateRegexGroups() {
        checkTerminationGuard(
                db, TIMEOUT_SECONDS, "RETURN apoc.text.regexGroups($text, $regex) AS value", HOSTILE_INPUT);
    }

    @Test
    void terminateRegexGroupsByName() {
        checkTerminationGuard(
                db, TIMEOUT_SECONDS, "RETURN apoc.text.regexGroupsByName($text, $regex) AS value", HOSTILE_INPUT);
    }

    @Test
    void terminateReplace() {
        checkTerminationGuard(
                db, TIMEOUT_SECONDS, "RETURN apoc.text.replace($text, $regex, '') AS value", HOSTILE_INPUT);
    }

    @Test
    void terminateRegreplace() {
        checkTerminationGuard(
                db, TIMEOUT_SECONDS, "CYPHER 5 RETURN apoc.text.regreplace($text, $regex, '') AS value", HOSTILE_INPUT);
    }

    @Test
    void terminateSplit() {
        checkTerminationGuard(db, TIMEOUT_SECONDS, "RETURN apoc.text.split($text, $regex) AS value", HOSTILE_INPUT);
    }
}
