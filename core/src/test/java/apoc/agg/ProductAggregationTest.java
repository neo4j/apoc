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

import static apoc.util.TestUtil.testCall;
import static org.junit.jupiter.api.Assertions.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@ImpermanentEnterpriseDbmsExtension()
public class ProductAggregationTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    public void setUp() {
        TestUtil.registerProcedure(db, Product.class);
    }

    @Test
    public void testProduct() {
        testCall(db, "UNWIND [] as value RETURN apoc.agg.product(value) as p", (row) -> {
            assertEquals(0D, row.get("p"));
        });
        testCall(db, "UNWIND RANGE(0,3) as value RETURN apoc.agg.product(value) as p", (row) -> {
            assertEquals(0L, row.get("p"));
        });
        testCall(db, "UNWIND RANGE(1,3) as value RETURN apoc.agg.product(value) as p", (row) -> {
            assertEquals(6L, row.get("p"));
        });
        testCall(db, "UNWIND RANGE(2,6) as value RETURN apoc.agg.product(value/2.0) as p", (row) -> {
            assertEquals(22.5D, row.get("p"));
        });
    }
}
