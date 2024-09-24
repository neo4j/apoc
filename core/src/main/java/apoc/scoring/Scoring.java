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
package apoc.scoring;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class Scoring {
    @UserFunction("apoc.scoring.existence")
    @Description("Returns the given score if true, 0 if false.")
    public double existence(
            final @Name(value = "score", description = "The score to return if the exists is true.") long score,
            final @Name(value = "exists", description = "Whether or not to return the score.") boolean exists) {
        return (double) (exists ? score : 0);
    }

    @UserFunction("apoc.scoring.pareto")
    @Description("Applies a Pareto scoring function over the given `INTEGER` values.")
    public double pareto(
            final @Name(value = "minimumThreshold", description = "The minimum threshold for the score.") long
                            minimumThreshold,
            final @Name(value = "eightyPercentValue", description = "The eighty percent value.") long
                            eightyPercentValue,
            final @Name(value = "maximumValue", description = "The maximum value.") long maximumValue,
            final @Name(value = "score", description = "The score.") long score) {
        if (score < minimumThreshold) {
            return 0.0d;
        } else {
            double alpha = Math.log((double) 5) / eightyPercentValue;
            double exp = Math.exp(-alpha * score);

            return maximumValue * (1 - exp);
        }
    }
}
