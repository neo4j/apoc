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
import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension
public class StringCleanTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Strings.class);
    }

    static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of("&N[]eo  4 #J-(3.0)  ", "neo4j30"),
                Arguments.of("German umlaut Ä Ö Ü ä ö ü ß ", "germanumlautaeoeueaeoeuess"),
                Arguments.of("French çÇéèêëïîôœàâæùûü", "frenchcceeeeiioœaaæuuue"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("data")
    void testClean(String dirty, String clean) {
        testCall(
                db,
                "RETURN apoc.text.clean($a) AS value",
                map("a", dirty),
                row -> assertEquals(clean, row.get("value")));
    }
}
