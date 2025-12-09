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
package apoc.graph.document.builder;

import apoc.graph.util.GraphsConfig;
import apoc.result.VirtualGraph;
import apoc.result.VirtualNode;
import apoc.result.VirtualRelationship;
import apoc.util.MapUtil;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

class DocumentToGraphTest {

    @Test
    void testWithNoRoot() {
        Map<String, Object> map = new HashMap<>();

        Map<String, String> mappings = new HashMap<>();
        mappings.put("$.entities", "Entity{!name,type,@metadata}");

        map.put("mappings", mappings);
        map.put("skipValidation", true);

        GraphsConfig config = new GraphsConfig(map);

        DocumentToGraph documentToGraph = new DocumentToGraph(null, config);

        Map<String, Object> document = new HashMap<>();
        List<Map<String, Object>> entities = new ArrayList<>();
        entities.add(MapUtil.map("name", 1, "type", "Artist"));
        entities.add(MapUtil.map("name", 2, "type", "Engineer"));

        document.put("entities", entities);

        VirtualGraph s = documentToGraph.create(document);
        Set<VirtualNode> nodes = (Set<VirtualNode>) s.graph.get("nodes");
        Iterator<VirtualNode> nodesIterator = nodes.iterator();

        nodesIterator.next(); // skip root node

        VirtualNode entityNode1 = nodesIterator.next();
        Assertions.assertEquals(1, entityNode1.getProperty("name"));
        Assertions.assertEquals("Artist", entityNode1.getProperty("type"));
        Assertions.assertEquals(entityNode1.getLabels(), Arrays.asList(Label.label("Artist"), Label.label("Entity")));

        VirtualNode entityNode2 = nodesIterator.next();
        Assertions.assertEquals(2, entityNode2.getProperty("name"));
        Assertions.assertEquals("Engineer", entityNode2.getProperty("type"));
        Assertions.assertEquals(entityNode2.getLabels(), Arrays.asList(Label.label("Engineer"), Label.label("Entity")));

        Set<VirtualRelationship> relationships = (Set<VirtualRelationship>) s.graph.get("relationships");
        Iterator<VirtualRelationship> relationshipIterator = relationships.iterator();

        VirtualRelationship rel1 = relationshipIterator.next();
        Assertions.assertEquals(rel1.getType(), RelationshipType.withName("ENTITIES"));
        Assertions.assertEquals(1, rel1.getEndNode().getProperty("name"));

        VirtualRelationship rel2 = relationshipIterator.next();
        Assertions.assertEquals(rel2.getType(), RelationshipType.withName("ENTITIES"));
        Assertions.assertEquals(2, rel2.getEndNode().getProperty("name"));
    }

    @Test
    void testWithRoot() {
        Map<String, Object> map = new HashMap<>();
        map.put("idField", "uri");

        Map<String, String> mappings = new HashMap<>();
        mappings.put("$.entities", "Entity{!name,type,@metadata}");

        map.put("mappings", mappings);
        map.put("skipValidation", true);

        GraphsConfig config = new GraphsConfig(map);

        DocumentToGraph documentToGraph = new DocumentToGraph(null, config);

        Map<String, Object> document = new HashMap<>();
        List<Map<String, Object>> entities = new ArrayList<>();
        entities.add(MapUtil.map("name", 1, "type", "Artist"));

        document.put("uri", "1234");
        document.put("type", "Article");
        document.put("entities", entities);

        VirtualGraph s = documentToGraph.create(document);
        Set<VirtualNode> nodes = (Set<VirtualNode>) s.graph.get("nodes");
        Iterator<VirtualNode> nodesIterator = nodes.iterator();

        VirtualNode rootNode = nodesIterator.next();
        Assertions.assertEquals("Article", rootNode.getProperty("type"));
        Assertions.assertEquals("1234", rootNode.getProperty("uri"));
        Assertions.assertEquals(rootNode.getLabels(), List.of(Label.label("Article")));

        VirtualNode entityNode = nodesIterator.next();
        Assertions.assertEquals(1, entityNode.getProperty("name"));
        Assertions.assertEquals("Artist", entityNode.getProperty("type"));
        Assertions.assertEquals(entityNode.getLabels(), Arrays.asList(Label.label("Artist"), Label.label("Entity")));

        Set<VirtualRelationship> relationships = (Set<VirtualRelationship>) s.graph.get("relationships");
        Iterator<VirtualRelationship> relationshipIterator = relationships.iterator();

        VirtualRelationship relationship = relationshipIterator.next();
        Assertions.assertEquals(relationship.getType(), RelationshipType.withName("ENTITIES"));
        Assertions.assertEquals("1234", relationship.getStartNode().getProperty("uri"));
        Assertions.assertEquals(1, relationship.getEndNode().getProperty("name"));
    }
}
