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

/**
 * This code was copied from project commons-math3
 */
class SimpleRegression {
    private double sumXX = 0d;
    private double sumYY = 0d;
    private double sumXY = 0d;
    private long n = 0;

    public void addData(final double x, final double y) {
        sumXX += x * x;
        sumYY += y * y;
        sumXY += x * y;
        n++;
    }

    public double getSlope() {
        if (n < 2) {
            return Double.NaN; // not enough data
        }
        if (Math.abs(sumXX) < 10 * Double.MIN_VALUE) {
            return Double.NaN; // not enough variation in x
        }
        return sumXY / sumXX;
    }

    public double getSumSquaredErrors() {
        return Math.max(0d, sumYY - sumXY * sumXY / sumXX);
    }

    public double getTotalSumSquares() {
        if (n < 2) {
            return Double.NaN;
        }
        return sumYY;
    }

    public double getR() {
        double b1 = getSlope();
        double result = Math.sqrt(getRSquare());
        if (b1 < 0) {
            result = -result;
        }
        return result;
    }

    public double getRSquare() {
        double ssto = getTotalSumSquares();
        return (ssto - getSumSquaredErrors()) / ssto;
    }
}
