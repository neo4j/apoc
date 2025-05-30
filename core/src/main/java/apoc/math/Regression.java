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

import java.util.stream.Stream;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

/**
 * @author <a href="mailto:ali.arslan@rwth-aachen.de">AliArslan</a>
 */
public class Regression {
    @Context
    public Transaction tx;

    // Result class
    public static class Output {
        @Description("The coefficient of determination.")
        public double r2;

        @Description("The average of the x values.")
        public double avgX;

        @Description("The average of the y values.")
        public double avgY;

        @Description("The calculated slope.")
        public double slope;

        public Output(double r2, double avgX, double avgY, double slope) {
            this.r2 = r2;
            this.avgX = avgX;
            this.avgY = avgY;
            this.slope = slope;
        }
    }

    @Procedure(name = "apoc.math.regr", mode = Mode.READ)
    @Description(
            "Returns the coefficient of determination (R-squared) for the values of propertyY and propertyX in the given label.")
    public Stream<Output> regr(
            @Name(value = "label", description = "The label of the nodes to perform the regression on.") String label,
            @Name(value = "propertyY", description = "The name of the y property.") String y,
            @Name(value = "propertyX", description = "The name of the x property.") String x) {

        SimpleRegression regr = new SimpleRegression();
        double regrAvgX = 0;
        double regrAvgY = 0;
        int count = 0;

        try (ResourceIterator<Node> it = tx.findNodes(Label.label(label))) {
            while (it.hasNext()) {
                Node node = it.next();
                Number propX = (Number) node.getProperty(x, null);
                Number propY = (Number) node.getProperty(y, null);
                if (propX != null && propY != null) {
                    regrAvgX = regrAvgX + propX.doubleValue();
                    regrAvgY = regrAvgY + propY.doubleValue();
                    regr.addData(propX.doubleValue(), propY.doubleValue());
                    count++;
                }
            }
        }
        regrAvgX = regrAvgX / count;
        regrAvgY = regrAvgY / count;
        return Stream.of(new Output(regr.getRSquare(), regrAvgX, regrAvgY, regr.getSlope()));
    }
}
