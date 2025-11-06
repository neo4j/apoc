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

import static org.junit.Assert.assertEquals;

import apoc.util.TestUtil;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension(createDatabasePerTest = false)
public class RegressionTest {

    @Inject
    GraphDatabaseService db;

    @BeforeAll
    void setUp() {
        TestUtil.registerProcedure(db, Regression.class);
    }

    @Test
    public void testCalculateRegr() {
        db.executeTransactionally(
                """
                CREATE (:REGR_TEST {x_property: 1 , y_property: 2 }),
                (:REGR_TEST {x_property: 2 , y_property: 3 }),
                (:REGR_TEST {y_property: 10000 }),
                (:REGR_TEST {x_property: 3 , y_property: 6 })""");

        SimpleRegression expectedRegr = new SimpleRegression();
        expectedRegr.addData(1, 1);
        expectedRegr.addData(2, 3);
        expectedRegr.addData(3, 6);
        TestUtil.testCall(db, "CALL apoc.math.regr('REGR_TEST', 'y_property', 'x_property')", result -> {
            assertEquals(expectedRegr.getRSquare(), (Double) result.get("r2"), 0.1);
            assertEquals(2.0, (Double) result.get("avgX"), 0.1);
            assertEquals(3.67, (Double) result.get("avgY"), 0.1);
            assertEquals(expectedRegr.getSlope(), (Double) result.get("slope"), 0.1);
        });
    }

    @Test
    public void testRegrR2isOne() {
        db.executeTransactionally(
                """
                CREATE (:REGR_TEST2 {x_property: 1 , y_property: 1 }),
                (:REGR_TEST2 {x_property: 1 , y_property: 1 }),
                (:REGR_TEST2 {y_property: 10000 }),
                (:REGR_TEST2 {x_property: 1 , y_property: 1 })""");

        SimpleRegression expectedRegr = new SimpleRegression();
        expectedRegr.addData(1, 1);
        expectedRegr.addData(1, 1);
        expectedRegr.addData(1, 1);

        TestUtil.testCall(db, "CALL apoc.math.regr('REGR_TEST2', 'y_property', 'x_property')", result -> {
            assertEquals(expectedRegr.getRSquare(), (Double) result.get("r2"), 0.1);
            assertEquals(expectedRegr.getSlope(), (Double) result.get("slope"), 0.1);
        });
    }
}
