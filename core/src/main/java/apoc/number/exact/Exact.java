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
package apoc.number.exact;

import static java.lang.Math.pow;

import apoc.util.ProcedureMemoryUtil;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.procedure.memory.ProcedureMemory;

/**
 * @author AgileLARUS
 *
 * @since 17 May 2017
 */
public class Exact {

    @Context
    public ProcedureMemory procedureMemory;

    @UserFunction("apoc.number.exact.add")
    @Description("Returns the result of adding the two given large numbers (using Java BigDecimal).")
    public String add(
            @Name(
                            value = "stringA",
                            description = "A string representation of a number to be added to the second number.")
                    String stringA,
            @Name(
                            value = "stringB",
                            description = "A string representation of a number to be added to the first number.")
                    String stringB) {
        if (stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
        final BigDecimal a = new BigDecimal(stringA);
        final BigDecimal b = new BigDecimal(stringB);
        return chargeAndCompute(a, b, () -> a.add(b).toPlainString());
    }

    @UserFunction("apoc.number.exact.sub")
    @Description(
            "Returns the result of subtracting a given large number from another given large number (using Java BigDecimal).")
    public String sub(
            @Name(
                            value = "stringA",
                            description =
                                    "A string representation of a number to have a second number subtracted from.")
                    String stringA,
            @Name(
                            value = "stringB",
                            description = "A string representation of a number to subtract from the first number.")
                    String stringB) {
        if (stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
        final BigDecimal a = new BigDecimal(stringA);
        final BigDecimal b = new BigDecimal(stringB);
        return chargeAndCompute(a, b, () -> a.subtract(b).toPlainString());
    }

    @UserFunction("apoc.number.exact.mul")
    @Description("Returns the result of multiplying two given large numbers (using Java BigDecimal).")
    public String mul(
            @Name(
                            value = "stringA",
                            description = "A string representation of a number to multiply by the second number.")
                    String stringA,
            @Name(
                            value = "stringB",
                            description = "A string representation of a number to multiply by the first number.")
                    String stringB,
            @Name(value = "precision", defaultValue = "0", description = "The rounding precision.") Long precision,
            @Name(
                            value = "roundingMode",
                            defaultValue = "HALF_UP",
                            description =
                                    "A precision rounding mode (`UP`, `DOWN`, `CEILING`, `FLOOR`, `HALF_UP`, `HALF_DOWN`, `HALF_EVEN`).")
                    String roundingMode) {
        if (stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
        final BigDecimal a = new BigDecimal(stringA);
        final BigDecimal b = new BigDecimal(stringB);
        final MathContext mathContext = createMathContext(precision, roundingMode);
        return chargeAndCompute(a, b, () -> a.multiply(b, mathContext).toPlainString());
    }

    @UserFunction("apoc.number.exact.div")
    @Description(
            "Returns the result of dividing a given large number with another given large number (using Java BigDecimal).")
    public String div(
            @Name(
                            value = "stringA",
                            description = "A string representation of a number to be divided by the second number.")
                    String stringA,
            @Name(value = "stringB", description = "A string representation of a number to divide the first number by.")
                    String stringB,
            @Name(value = "precision", defaultValue = "0", description = "The rounding precision.") Long precision,
            @Name(
                            value = "roundingMode",
                            defaultValue = "HALF_UP",
                            description =
                                    "A precision rounding mode (`UP`, `DOWN`, `CEILING`, `FLOOR`, `HALF_UP`, `HALF_DOWN`, `HALF_EVEN`).")
                    String roundingMode) {
        if (stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
        final BigDecimal a = new BigDecimal(stringA);
        final BigDecimal b = new BigDecimal(stringB);
        final MathContext mathContext = createMathContext(precision, roundingMode);
        return chargeAndCompute(a, b, () -> a.divide(b, mathContext).toPlainString());
    }

    @UserFunction("apoc.number.exact.toInteger")
    @Description("Returns the `INTEGER` of the given large number (using Java BigDecimal).")
    public Long toInteger(
            @Name(value = "string", description = "A large number represented as a string.") String string,
            @Name(value = "precision", defaultValue = "0", description = "The rounding precision.") Long precision,
            @Name(
                            value = "roundingMode",
                            defaultValue = "HALF_UP",
                            description =
                                    "A precision rounding mode (`UP`, `DOWN`, `CEILING`, `FLOOR`, `HALF_UP`, `HALF_DOWN`, `HALF_EVEN`).")
                    String roundingMode) {
        if (string == null || string.isEmpty()) return null;
        return new BigDecimal(string, createMathContextLong(precision, roundingMode)).longValue();
    }

    @UserFunction("apoc.number.exact.toFloat")
    @Description("Returns the `FLOAT` of the given large number (using Java BigDecimal).")
    public Double toFloat(
            @Name(value = "string", description = "A large number represented as a string.") String string,
            @Name(value = "precision", defaultValue = "0", description = "The rounding precision.") Long precision,
            @Name(
                            value = "roundingMode",
                            defaultValue = "HALF_UP",
                            description =
                                    "A precision rounding mode (`UP`, `DOWN`, `CEILING`, `FLOOR`, `HALF_UP`, `HALF_DOWN`, `HALF_EVEN`).")
                    String roundingMode) {
        if (string == null || string.isEmpty()) return null;
        return new BigDecimal(string, createMathContext(precision, roundingMode)).doubleValue();
    }

    @UserFunction("apoc.number.exact.toExact")
    @Description("Returns the exact value of the given number (using Java BigDecimal).")
    public Long toExact(
            @Name(value = "number", description = "An integer to receive the exact value of.") Long number) {
        if (number == null) return null;
        return new BigDecimal(number).longValueExact();
    }

    /**
     * Charges the plain-string forms of both operands and of the result before running the arithmetic.
     * Parsing is cheap even for `1e1000000000` (unscaled value 1, scale -10^9), but `toPlainString()` on it
     * expands the exponent into a two-gigabyte string, so the charge has to precede the computation.
     */
    private String chargeAndCompute(BigDecimal a, BigDecimal b, Supplier<String> computation) {
        long lengthA = plainStringLength(a);
        long lengthB = plainStringLength(b);
        // Whichever of add/sub/mul/div is applied, the result's plain form is no longer than both operands'
        // together, plus room for a sign, a decimal point and a carried digit.
        long lengthResult = Math.addExact(Math.addExact(lengthA, lengthB), 4L);
        long bytes = Math.addExact(
                Math.addExact(ProcedureMemoryUtil.sizeOfString(lengthA), ProcedureMemoryUtil.sizeOfString(lengthB)),
                ProcedureMemoryUtil.sizeOfString(lengthResult));
        try (var tracker = ProcedureMemoryUtil.charge(procedureMemory, bytes)) {
            return computation.get();
        }
    }

    /** Upper bound on the length of bd.toPlainString(). */
    private static long plainStringLength(BigDecimal bd) {
        return Math.addExact(bd.precision(), Math.abs((long) bd.scale()));
    }

    private MathContext createMathContext(Long precision, String roundingMode) {

        if (precision == null) {
            precision = Long.valueOf(0);
        }

        RoundingMode rm = RoundingMode.HALF_UP;
        if (!StringUtils.isEmpty(roundingMode) || roundingMode != null) {
            rm = RoundingMode.valueOf(roundingMode);
        }

        return new MathContext(precision.intValue(), rm);
    }

    private MathContext createMathContextLong(Long precision, String roundingMode) {
        if (precision == null) {
            precision = Long.valueOf(0);
        } else {
            Double pow = pow(10, precision);
            precision = precision * pow.longValue();
        }
        RoundingMode rm = RoundingMode.HALF_UP;
        if (!StringUtils.isEmpty(roundingMode) || roundingMode != null) {
            rm = RoundingMode.valueOf(roundingMode);
        }

        return new MathContext(precision.intValue(), rm);
    }
}
