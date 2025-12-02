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

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension()
public class PhoneticTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Phonetic.class);
    }

    @Test
    void shouldComputeSimpleSoundexEncoding() {
        testCall(
                db,
                "RETURN apoc.text.phonetic('HellodearUser!') as value",
                (row) -> assertEquals("H436", row.get("value")));
    }

    @Test
    void shouldComputeSimpleSoundexEncodingOfNull() {
        testCall(db, "RETURN apoc.text.phonetic(null) as value", (row) -> assertNull(row.get("value")));
    }

    @Test
    void shouldComputeEmptySoundexEncodingForTheEmptyString() {
        testCall(db, "RETURN apoc.text.phonetic('') as value", (row) -> assertEquals("", row.get("value")));
    }

    @Test
    void shouldComputeSoundexEncodingOfManyWords() {
        testCall(
                db,
                "RETURN apoc.text.phonetic('Hello, dear User!') as value",
                (row) -> assertEquals("H400D600U260", row.get("value")));
    }

    @Test
    void shouldComputeSoundexEncodingOfManyWordsEvenIfTheStringContainsSomeExtraChars() {
        testCall(
                db,
                "RETURN apoc.text.phonetic('  ,Hello,  dear User 5!') as value",
                (row) -> assertEquals("H400D600U260", row.get("value")));
    }

    @Test
    void shouldComputeSoundexDifference() {
        testCall(
                db,
                "CALL apoc.text.phoneticDelta('Hello Mr Rabbit', 'Hello Mr Ribbit')",
                (row) -> assertEquals(4L, row.get("delta")));
    }

    @Test
    void shoudlComputeDoubleMetaphone() {
        testCall(
                db,
                "RETURN apoc.text.doubleMetaphone('Apoc') as value",
                (row) -> assertEquals("APK", row.get("value")));
    }

    @Test
    void shouldComputeDoubleMetaphoneOfNull() {
        testCall(db, "RETURN apoc.text.doubleMetaphone(NULL) as value", (row) -> assertNull(row.get("value")));
    }

    @Test
    void shouldComputeDoubleMetaphoneForTheEmptyString() {
        testCall(db, "RETURN apoc.text.doubleMetaphone('') as value", (row) -> assertEquals("", row.get("value")));
    }

    @Test
    void shouldComputeDoubleMetaphoneOfManyWords() {
        testCall(
                db,
                "RETURN apoc.text.doubleMetaphone('Hello, dear User!') as value    ",
                (row) -> assertEquals("HLTRASR", row.get("value")));
    }
}
