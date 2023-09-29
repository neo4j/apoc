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

import org.apache.commons.lang3.StringUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static java.lang.Math.pow;

/**
 * @author AgileLARUS
 *
 * @since 17 May 2017
 */
public class Exact {

	@UserFunction("apoc.number.exact.add")
	@Description("Returns the result of adding the two given large numbers (using Java BigDecimal).")
	public String add(@Name("stringA")String stringA, @Name("stringB")String stringB){
		if(stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
		return new BigDecimal(stringA).add(new BigDecimal(stringB)).toPlainString();
	}

	@UserFunction("apoc.number.exact.sub")
	@Description("Returns the result of subtracting a given large number from another given large number (using Java BigDecimal).")
	public String sub(@Name("stringA")String stringA, @Name("stringB")String stringB){
		if(stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
		return new BigDecimal(stringA).subtract(new BigDecimal(stringB)).toPlainString();
	}

	@UserFunction("apoc.number.exact.mul")
	@Description("Returns the result of multiplying two given large numbers (using Java BigDecimal).")
	public String mul(@Name("stringA")String stringA, @Name("stringB")String stringB, @Name(value = "precision" , defaultValue = "0")Long precision, @Name(value = "roundingMode", defaultValue = "HALF_UP")String roundingMode){
		if(stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
		String s = new BigDecimal(stringA).multiply(new BigDecimal(stringB), createMathContext(precision, roundingMode)).toPlainString();
		return s;
	}

	@UserFunction("apoc.number.exact.div")
	@Description("Returns the result of dividing a given large number with another given large number (using Java BigDecimal).")
	public String div(@Name("stringA")String stringA, @Name("stringB")String stringB, @Name(value = "precision" , defaultValue = "0")Long precision, @Name(value = "roundingMode", defaultValue = "HALF_UP")String roundingMode){
		if(stringA == null || stringA.isEmpty() || stringB == null || stringB.isEmpty()) return null;
		return new BigDecimal(stringA).divide(new BigDecimal(stringB), createMathContext(precision, roundingMode)).toPlainString();
	}

	@UserFunction("apoc.number.exact.toInteger")
	@Description("Returns the `INTEGER` of the given large number (using Java BigDecimal).")
	public Long toInteger(@Name("string")String string, @Name(value = "precision" , defaultValue = "0")Long precision, @Name(value = "roundingMode", defaultValue = "HALF_UP")String roundingMode){
		if(string == null || string.isEmpty()) return null;
			return new BigDecimal(string, createMathContextLong(precision, roundingMode)).longValue();
	}

	@UserFunction("apoc.number.exact.toFloat")
	@Description("Returns the `FLOAT` of the given large number (using Java BigDecimal).")
	public Double toFloat(@Name("string")String string, @Name(value = "precision" , defaultValue = "0")Long precision, @Name(value = "roundingMode", defaultValue = "HALF_UP")String roundingMode){
		if(string == null || string.isEmpty()) return null;
		return new BigDecimal(string, createMathContext(precision, roundingMode)).doubleValue();
	}

	@UserFunction("apoc.number.exact.toExact")
	@Description("Returns the exact value of the given number (using Java BigDecimal).")
	public Long toExact(@Name("number")Long number){
		if(number == null) return null;
		return new BigDecimal(number).longValueExact();
	}

	private MathContext createMathContext(Long precision, String roundingMode){

		if (precision == null) {
			precision = Long.valueOf(0);
		}

		RoundingMode rm = RoundingMode.HALF_UP;
		if (!StringUtils.isEmpty(roundingMode) || roundingMode != null) {
			rm = RoundingMode.valueOf(roundingMode);
		}

		return new MathContext(precision.intValue(), rm);
	}

	private MathContext createMathContextLong(Long precision, String roundingMode){
		if (precision == null) {
			precision = Long.valueOf(0);
		}
		else {
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
