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
package apoc.coll;

/**
 * This code was copied from project commons-math3
 */
final class StandardDeviation {

    private StandardDeviation() {}

    public static double stdDev(double[] values, boolean isBiasCorrected) {
        return Math.sqrt(variance(values, isBiasCorrected));
    }

    private static double variance(double[] values, boolean isBiasCorrected) {
        double variance = Double.NaN;

        int length = values.length;
        if (length == 1) {
            variance = 0.0;
        } else if (length > 1) {
            double m = meanValue(values, length);
            double accum = 0.0;
            double dev;
            double accum2 = 0.0;
            for (double value : values) {
                dev = value - m;
                accum += dev * dev;
                accum2 += dev;
            }
            double len = length;
            if (isBiasCorrected) {
                variance = (accum - (accum2 * accum2 / len)) / (len - 1.0);
            } else {
                variance = (accum - (accum2 * accum2 / len)) / len;
            }
        }

        return variance;
    }

    private static double meanValue(double[] values, int length) {
        // Compute initial estimate using definitional formula
        double sum = 0.0;
        for (double i : values) {
            sum += i;
        }
        double xbar = sum / length;

        // Compute correction factor in second pass
        double correction = 0;
        for (double value : values) {
            correction += value - xbar;
        }
        return xbar + (correction / length);
    }
}
