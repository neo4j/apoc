package org.neo4j.apoc.benchmark;

import apoc.meta.Meta;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * Benchmarks apoc.meta.stats.
 * Note, this was thrown together very quickly to have some form of ground for optimisations done for a support card.
 * Probably all kinds of problems here.
 */
@State(Scope.Thread)
public class ApocMetaStatsBenchmark {
    private EmbeddedNeo4j embeddedNeo4j;
    private GraphDatabaseService db;

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public Object benchmarkMetaStats() {
        return db.executeTransactionally("CALL apoc.meta.stats()", Map.of(), r -> r.stream().toList());
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        System.out.println("Starting...");
        final var embeddedNeo4j = EmbeddedNeo4j.start();
        System.out.println("Started in " + embeddedNeo4j.directory);
        embeddedNeo4j.registerProcedure(Meta.class);
        this.db = embeddedNeo4j.db;
        this.embeddedNeo4j = embeddedNeo4j;
        System.out.println("Creating data...");
        createData();
    }

    private void createData() {
        final int labelCount = 30;
        final int totNodeCount = 10000;
        final int relTypeCount = 30;
        final int totRelCount = 10000;
        final var rand = new Random(23);
        final var labels = IntStream.range(0, labelCount).mapToObj(i -> Label.label("Label" + i)).toList();
        final var types = IntStream.range(0, relTypeCount).mapToObj(i -> RelationshipType.withName("Type" + i)).toList();

        try (final var tx = db.beginTx()) {
            final var nodes = new ArrayList<Node>();
            for (int i = 0; i < totNodeCount; ++i) {
                final var ls = IntStream.range(0, rand.nextInt(10)).mapToObj(x -> labels.get(rand.nextInt(labels.size()))).toArray(Label[]::new);
                nodes.add(tx.createNode(ls));
            }
            System.out.println("Created nodes " + totNodeCount);
            int relCount = 0;
            while (relCount < totRelCount) {
                final var a = nodes.get(rand.nextInt(nodes.size()));
                final var b = nodes.get(rand.nextInt(nodes.size()));
                final var t = types.get(rand.nextInt(types.size()));
                if (a.getRelationships().stream().noneMatch(r -> r.isType(t) && r.getEndNode().equals(b))) {
                    a.createRelationshipTo(b, t);
                    ++relCount;
                }
            }
            System.out.println("Created relationships " + totRelCount);
            tx.commit();
        }
        System.out.println("Created data");
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        embeddedNeo4j.managementService.shutdown();
    }
}