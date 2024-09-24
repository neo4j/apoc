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
package apoc.math;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

/**
 * @author mh
 * @since 12.12.16
 */
public class Maths {

    @UserFunction("apoc.math.maxLong")
    @Description("Returns the maximum value of a long.")
    public Long maxLong() {
        return Long.MAX_VALUE;
    }

    @UserFunction("apoc.math.minLong")
    @Description("Returns the minimum value of a long.")
    public Long minLong() {
        return Long.MIN_VALUE;
    }

    @UserFunction("apoc.math.maxDouble")
    @Description("Returns the largest positive finite value of type double.")
    public Double maxDouble() {
        return Double.MAX_VALUE;
    }

    @UserFunction("apoc.math.minDouble")
    @Description("Returns the smallest positive non-zero value of type double.")
    public Double minDouble() {
        return Double.MIN_VALUE;
    }

    @UserFunction("apoc.math.maxInt")
    @Description("Returns the maximum value of an integer.")
    public Long maxInt() {
        return Long.valueOf(Integer.MAX_VALUE);
    }

    @UserFunction("apoc.math.minInt")
    @Description("Returns the minimum value of an integer.")
    public Long minInt() {
        return Long.valueOf(Integer.MIN_VALUE);
    }

    @UserFunction("apoc.math.maxByte")
    @Description("Returns the maximum value of a byte.")
    public Long maxByte() {
        return Long.valueOf(Byte.MAX_VALUE);
    }

    @UserFunction("apoc.math.minByte")
    @Description("Returns the minimum value of a byte.")
    public Long minByte() {
        return Long.valueOf(Byte.MIN_VALUE);
    }

    @UserFunction("apoc.math.sigmoid")
    @Description("Returns the sigmoid of the given value.")
    public Double sigmoid(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null) return null;
        return 1.0 / (1.0 + Math.exp(-value));
    }

    @UserFunction("apoc.math.sigmoidPrime")
    @Description("Returns the sigmoid prime [ sigmoid(val) * (1 - sigmoid(val)) ] of the given value.")
    public Double sigmoidPrime(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null) return null;
        return sigmoid(value) * (1 - sigmoid(value));
    }

    @UserFunction("apoc.math.tanh")
    @Description("Returns the hyperbolic tangent of the given value.")
    public Double tanh(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null) return null;
        return sinh(value) / cosh(value);
    }

    @UserFunction("apoc.math.coth")
    @Description("Returns the hyperbolic cotangent.")
    public Double coth(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null || value.equals(0D)) return null;
        return cosh(value) / sinh(value);
    }

    @UserFunction("apoc.math.cosh")
    @Description("Returns the hyperbolic cosine.")
    public Double cosh(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null) return null;
        return (Math.exp(value) + Math.exp(-value)) / 2;
    }

    @UserFunction("apoc.math.sinh")
    @Description("Returns the hyperbolic sine of the given value.")
    public Double sinh(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null) return null;
        return (Math.exp(value) - Math.exp(-value)) / 2;
    }

    @UserFunction("apoc.math.sech")
    @Description("Returns the hyperbolic secant of the given value.")
    public Double sech(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null) return null;
        return 1 / cosh(value);
    }

    @UserFunction("apoc.math.csch")
    @Description("Returns the hyperbolic cosecant.")
    public Double csch(@Name(value = "value", description = "An angle in radians.") Double value) {
        if (value == null || value.equals(0D)) return null;
        return 1 / sinh(value);
    }
}
