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
package apoc.it.core;

import static apoc.it.core.ApocSplitTest.CORE_FUNCTIONS;
import static apoc.it.core.ApocSplitTest.CORE_PROCEDURES;

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
import apoc.meta.MetaRestricted;
import apoc.neighbors.Neighbors;
import apoc.nodes.Grouping;
import apoc.nodes.Nodes;
import apoc.nodes.NodesRestricted;
import apoc.number.ArabicRoman;
import apoc.number.Numbers;
import apoc.number.exact.Exact;
import apoc.path.PathExplorer;
import apoc.path.Paths;
import apoc.periodic.Periodic;
import apoc.refactor.GraphRefactoring;
import apoc.refactor.rename.Rename;
import apoc.schema.SchemaRestricted;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/*
This test is just to verify the differences between Cypher 5 and Cypher 25 for APOC Core
*/
public class ApocVersionsTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, List.of(
                    "apoc.nodes.link",
                    "apoc.node.relationship.exists",
                    "apoc.nodes.connected",
                    "apoc.nodes.isDense",
                    "apoc.schema.nodes",
                    "apoc.schema.relationship"
            ));

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
                MetaRestricted.class,
                Neighbors.class,
                Neo4jLogStream.class,
                Nodes.class,
                NodesRestricted.class,
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
                SchemaRestricted.class,
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

    public static final Set<String> DEPRECATED_CORE_PROCEDURES_5 = Set.of(
            "apoc.export.arrow.all",
            "apoc.export.arrow.graph",
            "apoc.export.arrow.query",
            "apoc.export.arrow.stream.all",
            "apoc.export.arrow.stream.graph",
            "apoc.export.arrow.stream.query",
            "apoc.load.arrow",
            "apoc.load.arrow.stream",
            "apoc.load.jsonParams",
            "apoc.log.stream",
            "apoc.trigger.add",
            "apoc.trigger.remove",
            "apoc.trigger.removeAll",
            "apoc.trigger.pause",
            "apoc.trigger.resume",
            "apoc.create.uuids",
            "apoc.convert.toTree",
            "apoc.warmup.run");

    public static final Set<String> DEPRECATED_CORE_FUNCTIONS_5 =
            Set.of("apoc.create.uuid", "apoc.map.setEntry", "apoc.text.regreplace", "apoc.text.levenshteinDistance");

    @Test
    public void test() {
        Set<String> deprecatedProcedureNames5 = db.executeTransactionally(
                "CYPHER 5 SHOW PROCEDURES YIELD name, isDeprecated WHERE name STARTS WITH 'apoc' AND isDeprecated RETURN name",
                Map.of(),
                r -> r.stream().map(row -> row.get("name").toString()).collect(Collectors.toSet()));

        Set<String> deprecatedFunctionNames5 = db.executeTransactionally(
                "CYPHER 5 SHOW FUNCTIONS YIELD name, isDeprecated WHERE name STARTS WITH 'apoc' AND isDeprecated RETURN name",
                Map.of(),
                r -> r.stream().map(row -> row.get("name").toString()).collect(Collectors.toSet()));

        Assert.assertEquals(DEPRECATED_CORE_PROCEDURES_5, deprecatedProcedureNames5);
        Assert.assertEquals(DEPRECATED_CORE_FUNCTIONS_5, deprecatedFunctionNames5);

        Set<String> procedureNames = db.executeTransactionally(
                "CYPHER 25 SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'apoc' RETURN name",
                Map.of(),
                r -> r.stream().map(row -> row.get("name").toString()).collect(Collectors.toSet()));

        Set<String> functionNames = db.executeTransactionally(
                "CYPHER 25 SHOW FUNCTIONS YIELD name WHERE name STARTS WITH 'apoc' RETURN name",
                Map.of(),
                r -> r.stream().map(row -> row.get("name").toString()).collect(Collectors.toSet()));

        Set<String> proceduresCypher25 = new HashSet<>(CORE_PROCEDURES);
        proceduresCypher25.removeAll(DEPRECATED_CORE_PROCEDURES_5);

        Set<String> functionsCypher25 = new HashSet<>(CORE_FUNCTIONS);
        functionsCypher25.removeAll(DEPRECATED_CORE_FUNCTIONS_5);

        Assert.assertEquals(proceduresCypher25, procedureNames);
        Assert.assertEquals(functionsCypher25, functionNames);
    }

    @After
    public void teardown() {
        db.shutdown();
    }
}
