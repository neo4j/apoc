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

import org.apache.commons.codec.language.DoubleMetaphone;
import org.neo4j.procedure.Description;
import org.apache.commons.codec.EncoderException;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.codec.language.Soundex.US_ENGLISH;

public class Phonetic {

    private static final DoubleMetaphone DOUBLE_METAPHONE = new DoubleMetaphone();

    @UserFunction("apoc.text.phonetic")
    @Description("Returns the US_ENGLISH phonetic soundex encoding of all words of the string.")
    public String phonetic(final @Name("text") String value) {
        if (value == null) return null;
        return Stream.of(value.split("\\W+")).map(US_ENGLISH::soundex).collect(Collectors.joining(""));
    }

    @Procedure("apoc.text.phoneticDelta")
    @Description("Returns the US_ENGLISH soundex character difference between the two given strings.")
    public Stream<PhoneticResult> phoneticDelta(final @Name("text1") String text1, final @Name("text2") String text2) {
        try {
            return Stream.of(new PhoneticResult(US_ENGLISH.soundex(text1),US_ENGLISH.soundex(text2),US_ENGLISH.difference(text1,text2)));
        } catch (EncoderException e) {
            throw new RuntimeException("Error encoding text "+text1+" or "+text2+" for delta measure",e);
        }
    }

    @UserFunction("apoc.text.doubleMetaphone")
    @Description("Returns the double metaphone phonetic encoding of all words in the given string value.")
    public String doubleMetaphone(final @Name("value") String value)
    {
        if (value == null || value.trim().isEmpty()) return value;
        return Stream.of(value.split("\\W+")).map(DOUBLE_METAPHONE::doubleMetaphone).collect(Collectors.joining(""));
    }

    public static class PhoneticResult {
        public final String phonetic1, phonetic2;
        public final long delta;

        public PhoneticResult(String phonetic1, String phonetic2, Number delta) {
            this.phonetic1 = phonetic1;
            this.phonetic2 = phonetic2;
            this.delta = delta.longValue();
        }
    }
}
