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
package apoc.load;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SimpleXmlTest {

    static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of("some text with spaces", Arrays.asList("some", " ", "text", " ", "with", " ", "spaces")),
                Arguments.of(
                        " some text with spaces", Arrays.asList(" ", "some", " ", "text", " ", "with", " ", "spaces")));
    }

    @ParameterizedTest
    @MethodSource("data")
    void testParseTextIntoPartsAndDelimiters(String input, List<String> expected) {
        Xml cut = new Xml();
        List<String> result = cut.parseTextIntoPartsAndDelimiters(input, Pattern.compile("\\s"));

        MatcherAssert.assertThat(result, Matchers.equalTo(expected));
    }
}
