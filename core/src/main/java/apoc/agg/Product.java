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
package apoc.agg;

import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.*;

/**
 * @author mh
 * @since 18.12.17
 */
public class Product {
    @UserAggregationFunction("apoc.agg.product")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description("Returns the product of all non-null `INTEGER` and `FLOAT` values in the collection.")
    public ProductFunction productCypher5() {
        return new ProductFunction();
    }

    @Deprecated
    @UserAggregationFunction(
            name = "apoc.agg.product",
            deprecatedBy = "Cypher's `reduce()`: `RETURN reduce(x = 1, i IN values | x * i)`.")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Returns the product of all non-null `INTEGER` and `FLOAT` values in the collection.")
    public ProductFunction product() {
        return new ProductFunction();
    }

    public static class ProductFunction {

        private double doubleProduct = 1.0D;
        private long longProduct = 1L;
        private int count = 0;

        @UserAggregationUpdate
        public void aggregate(
                @Name(value = "value", description = "A value to be multiplied in the aggregate.") Number number) {
            if (number != null) {
                if (number instanceof Long) {
                    longProduct = Math.multiplyExact(longProduct, number.longValue());
                } else if (number instanceof Double) {
                    doubleProduct *= number.doubleValue();
                }
                count++;
            }
        }

        @UserAggregationResult
        public Number result() {
            if (count == 0) return 0D;
            if (longProduct == 1L) return doubleProduct;
            if (doubleProduct == 1.0) return longProduct;
            return doubleProduct * longProduct;
        }
    }
}
