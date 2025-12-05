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
import apoc.algo.Cover;
import apoc.algo.PathFinding;
import apoc.atomic.Atomic;
import apoc.bitwise.BitwiseOperations;
import apoc.coll.Coll;
import apoc.convert.Convert;
import apoc.convert.Json;
import apoc.create.Create;
import apoc.cypher.Cypher;
import apoc.cypher.CypherFunctions;
import apoc.cypher.Timeboxed;
import apoc.data.url.ExtractURL;
import apoc.date.Date;
import apoc.diff.Diff;
import apoc.example.Examples;
import apoc.export.arrow.ExportArrow;
import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.export.json.ImportJson;
import apoc.graph.Graphs;
import apoc.hashing.Fingerprinting;
import apoc.help.Help;
import apoc.index.SchemaIndex;
import apoc.label.Label;
import apoc.load.LoadArrow;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.lock.Lock;
import apoc.log.Neo4jLogStream;
import apoc.map.Maps;
import apoc.math.Maths;
import apoc.math.Regression;
import apoc.merge.Merge;
import apoc.meta.Meta;
import apoc.neighbors.Neighbors;
import apoc.nodes.Grouping;
import apoc.nodes.Nodes;
import apoc.number.ArabicRoman;
import apoc.number.Numbers;
import apoc.number.exact.Exact;
import apoc.path.PathExplorer;
import apoc.path.Paths;
import apoc.periodic.Periodic;
import apoc.refactor.GraphRefactoring;
import apoc.refactor.rename.Rename;
import apoc.schema.Schemas;
import apoc.scoring.Scoring;
import apoc.search.ParallelNodeSearch;
import apoc.spatial.Distance;
import apoc.spatial.Geocode;
import apoc.stats.DegreeDistribution;
import apoc.temporal.TemporalProcedures;
import apoc.text.Phonetic;
import apoc.text.Strings;
import apoc.trigger.Trigger;
import apoc.trigger.TriggerNewProcedures;
import apoc.util.TestUtil;
import apoc.util.Utils;
import apoc.version.Version;
import apoc.warmup.Warmup;
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
                Atomic.class,
                BitwiseOperations.class,
                Coll.class,
                CollAggregation.class,
                Convert.class,
                Cover.class,
                Create.class,
                Cypher.class,
                CypherFunctions.class,
                Date.class,
                DegreeDistribution.class,
                Diff.class,
                Distance.class,
                Exact.class,
                Examples.class,
                ExportArrow.class,
                ExportCSV.class,
                ExportJson.class,
                ExportGraphML.class,
                ExportCypher.class,
                ExtractURL.class,
                Fingerprinting.class,
                Geocode.class,
                Graph.class,
                GraphRefactoring.class,
                Graphs.class,
                Grouping.class,
                Help.class,
                ImportCsv.class,
                ImportJson.class,
                Json.class,
                Label.class,
                LoadArrow.class,
                LoadJson.class,
                Lock.class,
                Maps.class,
                Maths.class,
                MaxAndMinItems.class,
                Median.class,
                Merge.class,
                Meta.class,
                Neighbors.class,
                Neo4jLogStream.class,
                Nodes.class,
                Numbers.class,
                ParallelNodeSearch.class,
                PathExplorer.class,
                PathFinding.class,
                Paths.class,
                Percentiles.class,
                Periodic.class,
                Phonetic.class,
                Product.class,
                Rename.class,
                Regression.class,
                SchemaIndex.class,
                Schemas.class,
                Scoring.class,
                Statistics.class,
                Strings.class,
                TemporalProcedures.class,
                Timeboxed.class,
                Trigger.class,
                TriggerNewProcedures.class,
                Utils.class,
                Version.class,
                Warmup.class,
                Xml.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void functionArgumentDescriptionsTest() throws IOException {
        final var query = """
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

    @Test
    public void procedureArgumentDescriptionsTest() throws IOException {
        final var query = """
                  SHOW PROCEDURES YIELD
                  name, description, signature, argumentDescription,
                  returnDescription, isDeprecated, deprecatedBy
                  WHERE name STARTS WITH 'apoc'
                  RETURN *
                """;
        final var result =
                db.executeTransactionally(query, Map.of(), r -> r.stream().toList());

        final var json = new ObjectMapper();
        final var expected =
                json.reader().readTree(ArgumentDescriptionsTest.class.getResourceAsStream("/procedures.json"));
        final var actual = json.valueToTree(result);
        // Uncomment to print out how the file should look :)
        // System.out.println("Actual:\n" + json.writer().withDefaultPrettyPrinter().writeValueAsString(actual));
        for (int i = 0; i < expected.size(); ++i) assertThat(expected.get(i)).isEqualTo(actual.get(i));
        assertThat(actual).isEqualTo(expected);
    }
}
