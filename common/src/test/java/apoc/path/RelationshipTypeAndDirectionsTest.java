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
package apoc.path;

import static apoc.util.collection.Iterables.iterable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Direction.*;
import static org.neo4j.graphdb.RelationshipType.withName;

import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

public class RelationshipTypeAndDirectionsTest {

    static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(null, iterable(Pair.of(null, BOTH))),
                Arguments.of("", iterable(Pair.of(null, BOTH))),
                Arguments.of(">", iterable(Pair.of(null, OUTGOING))),
                Arguments.of("<", iterable(Pair.of(null, INCOMING))),
                Arguments.of("SIMPLE", iterable(Pair.of(withName("SIMPLE"), BOTH))),
                Arguments.of("SIMPLE>", iterable(Pair.of(withName("SIMPLE"), OUTGOING))),
                Arguments.of("SIMPLE<", iterable(Pair.of(withName("SIMPLE"), INCOMING))),
                Arguments.of("<SIMPLE", iterable(Pair.of(withName("SIMPLE"), INCOMING))),
                Arguments.of(">SIMPLE", iterable(Pair.of(withName("SIMPLE"), OUTGOING))),
                Arguments.of("SIMPLE", iterable(Pair.of(withName("SIMPLE"), BOTH))),
                Arguments.of(
                        "TYPE1|TYPE2", iterable(Pair.of(withName("TYPE1"), BOTH), Pair.of(withName("TYPE2"), BOTH))),
                Arguments.of(
                        "TYPE1>|TYPE2<",
                        iterable(Pair.of(withName("TYPE1"), OUTGOING), Pair.of(withName("TYPE2"), INCOMING))));
    }

    @ParameterizedTest(name = "{index}: {0} -> {1}")
    @MethodSource("data")
    void parse(String input, Iterable<Pair<RelationshipType, Direction>> expected) {
        assertEquals(expected, RelationshipTypeAndDirections.parse(input));
    }
}
