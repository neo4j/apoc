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
package apoc;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import apoc.agg.CollAggregation;
import apoc.agg.Graph;
import apoc.agg.MaxAndMinItems;
import apoc.agg.Median;
import apoc.agg.Percentiles;
import apoc.agg.Product;
import apoc.agg.Statistics;
import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.convert.Convert;
import apoc.convert.Json;
import apoc.create.Create;
import apoc.cypher.CypherFunctions;
import apoc.data.url.ExtractURL;
import apoc.date.Date;
import apoc.diff.Diff;
import apoc.export.csv.ExportCSV;
import apoc.graph.Graphs;
import apoc.hashing.Fingerprinting;
import apoc.label.Label;
import apoc.load.Xml;
import apoc.map.Maps;
import apoc.math.Maths;
import apoc.meta.Meta;
import apoc.nodes.Nodes;
import apoc.number.ArabicRoman;
import apoc.number.Numbers;
import apoc.number.exact.Exact;
import apoc.path.Paths;
import apoc.schema.Schemas;
import apoc.scoring.Scoring;
import apoc.temporal.TemporalProcedures;
import apoc.text.Phonetic;
import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Utils;
import apoc.version.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ArgumentDescriptionsTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, singletonList("apoc.*"));

    @Before
    public void setUp() {
        TestUtil.registerProcedure(
                db,
                ArabicRoman.class,
                BitwiseOperations.class,
                Coll.class,
                CollAggregation.class,
                Convert.class,
                Create.class,
                CypherFunctions.class,
                Date.class,
                Diff.class,
                Exact.class,
                ExportCSV.class,
                ExtractURL.class,
                Fingerprinting.class,
                Graph.class,
                Graphs.class,
                Json.class,
                Label.class,
                Maps.class,
                Maths.class,
                MaxAndMinItems.class,
                Median.class,
                Meta.class,
                Nodes.class,
                Numbers.class,
                Paths.class,
                Percentiles.class,
                Phonetic.class,
                Product.class,
                Schemas.class,
                Scoring.class,
                Statistics.class,
                Strings.class,
                TemporalProcedures.class,
                Utils.class,
                Version.class,
                Xml.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void functionArgumentDescriptionsTest() throws IOException {
        final var query =
                """
                  SHOW FUNCTIONS YIELD
                  name, category, description, signature, isBuiltIn,
                  argumentDescription, returnDescription, aggregating,
                  isDeprecated, deprecatedBy
                  WHERE name STARTS WITH 'apoc'
                  RETURN *
                """;
        final var result =
                db.executeTransactionally(query, Map.of(), r -> r.stream().toList());

        final var json = new ObjectMapper();
        final var expected =
                json.reader().readTree(ArgumentDescriptionsTest.class.getResourceAsStream("/functions.json"));
        final var actual = json.valueToTree(result);
        // Uncomment to print out how the file should look :)
        // System.out.println("Actual:\n" + json.writer().withDefaultPrettyPrinter().writeValueAsString(actual));
        for (int i = 0; i < expected.size(); ++i) assertThat(expected.get(i)).isEqualTo(actual.get(i));
        assertThat(actual).isEqualTo(expected);
    }
}
