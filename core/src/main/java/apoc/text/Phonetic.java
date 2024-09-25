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

import static org.apache.commons.codec.language.Soundex.US_ENGLISH;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

public class Phonetic {

    private static final DoubleMetaphone DOUBLE_METAPHONE = new DoubleMetaphone();

    @UserFunction("apoc.text.phonetic")
    @Description("Returns the US_ENGLISH phonetic soundex encoding of all words of the `STRING`.")
    public String phonetic(
            final @Name(value = "text", description = "The string to encode using US_ENGLISH phonetic soundex.") String
                            value) {
        if (value == null) return null;
        return Stream.of(value.split("\\W+")).map(US_ENGLISH::soundex).collect(Collectors.joining(""));
    }

    @Procedure("apoc.text.phoneticDelta")
    @Description("Returns the US_ENGLISH soundex character difference between the two given `STRING` values.")
    public Stream<PhoneticResult> phoneticDelta(
            final @Name(value = "text1", description = "The first string to be compared against the second.") String
                            text1,
            final @Name(value = "text2", description = "The second string to be compared against the first.") String
                            text2) {
        try {
            return Stream.of(new PhoneticResult(
                    US_ENGLISH.soundex(text1), US_ENGLISH.soundex(text2), US_ENGLISH.difference(text1, text2)));
        } catch (EncoderException e) {
            throw new RuntimeException("Error encoding text " + text1 + " or " + text2 + " for delta measure", e);
        }
    }

    @UserFunction("apoc.text.doubleMetaphone")
    @Description("Returns the double metaphone phonetic encoding of all words in the given `STRING` value.")
    public String doubleMetaphone(
            final @Name(
                            value = "value",
                            description = "The string to be encoded using the double metaphone phonetic encoding.")
                    String value) {
        if (value == null || value.trim().isEmpty()) return value;
        return Stream.of(value.split("\\W+"))
                .map(DOUBLE_METAPHONE::doubleMetaphone)
                .collect(Collectors.joining(""));
    }

    public static class PhoneticResult {
        @Description("The phonetic representation of the first string.")
        public final String phonetic1;

        @Description("The phonetic representation of the second string.")
        public final String phonetic2;

        @Description("The soundex character difference between the two given strings.")
        public final long delta;

        public PhoneticResult(String phonetic1, String phonetic2, Number delta) {
            this.phonetic1 = phonetic1;
            this.phonetic2 = phonetic2;
            this.delta = delta.longValue();
        }
    }
}
