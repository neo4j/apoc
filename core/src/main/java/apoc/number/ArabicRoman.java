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
package apoc.number;

import static apoc.number.ArabicRoman.RomanNumerals.getRoman;
import static apoc.number.ArabicRoman.RomanNumerals.toArabic;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class ArabicRoman {

    @UserFunction("apoc.number.romanToArabic")
    @Description("Converts the given Roman numbers to Arabic numbers.")
    public Number romanToArabic(
            final @Name(value = "romanNumber", description = "A Roman number to be converted.") String number) {
        if (number == null || number.isEmpty()) return 0L;
        return toArabic(number.toUpperCase());
    }

    @UserFunction("apoc.number.arabicToRoman")
    @Description("Converts the given Arabic numbers to Roman numbers.")
    public String arabicToRoman(
            final @Name(
                            value = "number",
                            description = "A number to be converted to a Roman number represented as a string.") Object
                            value) {
        Number number = validateNumberParam(value);
        if (number == null) return null;
        return getRoman(number.intValue());
    }

    private Number validateNumberParam(Object number) {
        return number instanceof Number ? (Number) number : null;
    }

    static class RomanNumerals {
        private static String roman[] = {
            "M", "XM", "CM", "D", "XD", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"
        };

        public static String getRoman(int number) {
            int arab[] = {1000, 990, 900, 500, 490, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1};
            StringBuilder result = new StringBuilder();
            int i = 0;
            while (number > 0 || arab.length == (i - 1)) {
                while ((number - arab[i]) >= 0) {
                    number -= arab[i];
                    result.append(roman[i]);
                }
                i++;
            }
            return result.toString();
        }

        static long toArabic(String number) {
            if (number == null || number.isEmpty()) return 0L;
            long result = 0;
            int i = 0;
            while (i < number.length()) {
                if (number.startsWith("M", i)) {
                    result += 1000;
                    i += 1;
                } else if (number.startsWith("CM", i)) {
                    result += 900;
                    i += 2;
                } else if (number.startsWith("D", i)) {
                    result += 500;
                    i += 1;
                } else if (number.startsWith("CD", i)) {
                    result += 400;
                    i += 2;
                } else if (number.startsWith("C", i)) {
                    result += 100;
                    i += 1;
                } else if (number.startsWith("XC", i)) {
                    result += 90;
                    i += 2;
                } else if (number.startsWith("L", i)) {
                    result += 50;
                    i += 1;
                } else if (number.startsWith("XL", i)) {
                    result += 40;
                    i += 2;
                } else if (number.startsWith("X", i)) {
                    result += 10;
                    i += 1;
                } else if (number.startsWith("IX", i)) {
                    result += 9;
                    i += 2;
                } else if (number.startsWith("V", i)) {
                    result += 5;
                    i += 1;
                } else if (number.startsWith("IV", i)) {
                    result += 4;
                    i += 2;
                } else if (number.startsWith("I", i)) {
                    result += 1;
                    i += 1;
                } else {
                    return result;
                }
            }
            return result;
        }
    }
}
