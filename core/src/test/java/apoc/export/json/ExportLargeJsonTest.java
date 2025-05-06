package apoc.export.json;

import apoc.util.TestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import com.neo4j.test.extension.ImpermanentEnterpriseDbmsExtension;
import net.javacrumbs.jsonunit.core.Option;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static net.javacrumbs.jsonunit.core.Option.IGNORING_ARRAY_ORDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

@SuppressWarnings("removal")
@ExtendWith(RandomExtension.class)
@ImpermanentEnterpriseDbmsExtension(configurationCallback = "configure")
public class ExportLargeJsonTest {
    private ObjectMapper mapper = new ObjectMapper();

    @Inject
    RandomSupport rand;

    @Inject
    GraphDatabaseService db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {}

    @BeforeAll
    void beforeAll() {
        TestUtil.registerProcedure(db, ExportJson.class);
    }

    @Test
    void exportLargeRandomGraph() {
        final var nodeCount = 12;
        final var relCount = 12;
        final var labels = IntStream.range(0, 128).mapToObj(i -> Label.label("L" + i)).toList();
        final var types = IntStream.range(0, 128).mapToObj(i -> RelationshipType.withName("L" + i)).toList();
        final var nodes = new ArrayList<Node>(nodeCount);
        try (final var tx = db.beginTx()) {
            for (int i = 0; i < nodeCount; i++) nodes.add(createNodeWithRandLabels(tx, labels));
            for (int i = 0; i < relCount; i++) createRelationshipWithRandType(tx, types, nodes);
            tx.commit();
        }
        try (final var tx = db.beginTx()) {
            System.out.println(tx.execute("call apoc.export.json.all(null, {stream:true, batchSize:20, writeNodeProperties: false})").stream().count());
            tx.execute("call apoc.export.json.all(null, {stream:true, batchSize:20})").forEachRemaining(r -> System.out.println("row:\n" + r));

            final var exportQuery = "call apoc.export.json.all(null, {stream:true, batchSize:20})";
            final var result = tx.execute(exportQuery).stream().toList();
            assertThat(result)
                    .allSatisfy(row -> {
                        assertThat(row)
                                .containsOnlyKeys("relationships",
                                        "batches",
                                        "file",
                                        "nodes",
                                        "data",
                                        "format",
                                        "source",
                                        "time",
                                        "rows",
                                        "batchSize",
                                        "done",
                                        "properties")
                                .hasEntrySatisfying("data", d -> isValidJsonLinesEntity(tx, d));
                    });
        }
    }

    private void isValidJsonLinesEntity(Transaction tx, Object jsonString) {
        assertThat(jsonString.toString().trim().lines()).allSatisfy(line -> isValidJsonEntity(tx, line));
    }


    private void isValidJsonEntity(Transaction tx, Object jsonString) {
        final JsonNode json;
        try {
            json = mapper.readTree(jsonString.toString());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        final var id = Long.parseLong(json.path("id").asText());
        switch (json.path("type").asText()) {
            case "node" ->
                    assertThatJson(json).when(IGNORING_ARRAY_ORDER).isEqualTo(asJson(tx.getNodeById(id), true));
            case "relationship" ->
                    assertThatJson(json).when(IGNORING_ARRAY_ORDER).isEqualTo(asJson(tx.getRelationshipById(id)));
        }
    }

    private ObjectNode asJson(Node node, boolean withType) {
        final var json = mapper.createObjectNode().put("id", String.valueOf(node.getId()));
        if (withType) json.put("type", "node");
        final var labels = StreamSupport.stream(node.getLabels().spliterator(), false)
                .map(Label::name)
                .toList();
        if (!labels.isEmpty()) json.put("labels", mapper.valueToTree(labels));
        final var props = node.getAllProperties();
        if (!props.isEmpty()) json.put("properties", mapper.valueToTree(props));
        return json;
    }

    private JsonNode asJson(Relationship rel) {
        final var json = mapper.createObjectNode()
                .put("id", String.valueOf(rel.getId()))
                .put("type", "relationship")
                .put("label", rel.getType().name())
                .<ObjectNode>set("start", asJson(rel.getStartNode(), false))
                .<ObjectNode>set("end", asJson(rel.getEndNode(), false));
        final var props = rel.getAllProperties();
        if (!props.isEmpty()) json.put("properties", mapper.valueToTree(props));
        return json;
    }

    /*
    private void isValidJson(Transaction tx, String json) throws JsonProcessingException {
        assertThatJson(json).isObject()
                .containsKeys("type", "id")
                .hasEntrySatisfying("type", t -> assertThat(t).isIn("node", "relationship"))
                .satisfies(map -> {
            switch ((String) map.get("type")) {
                case "node" -> {
                    final var node = tx.getNodeById(Long.parseLong(map.get("id").toString()));
                    final var labels = map.getOrDefault("labels", List.of());
                }
                case "relationship" -> assertThat(map.get("type")).isNotNull();
            }
        })
    }
     */


    private Node createNodeWithRandLabels(final Transaction tx, final List<Label> labels) {
        final var node = tx.createNode();
        for (int i = rand.nextInt(6); i > 0; i--) {
            node.addLabel(rand.among(labels));
        }
        for (int i = rand.nextInt(16); i > 0; i--) {
            node.setProperty("p" + i, rand.nextLong());
        }
        return node;
    }

    private Relationship createRelationshipWithRandType(final Transaction tx, final List<RelationshipType> types, final List<Node> nodes) {
        final var rel = rand.among(nodes).createRelationshipTo(rand.among(nodes), rand.among(types));
        for (int i = rand.nextInt(16); i > 0; i--) {
            rel.setProperty("p" + i, rand.nextLong());
        }
        return rel;
    }
}
