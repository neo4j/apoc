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
package apoc.result;

import static org.junit.Assert.*;

import apoc.util.Util;
import apoc.util.collection.Iterables;
import java.util.Iterator;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class VirtualNodeTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void shouldCreateVirtualNode() {
        Map<String, Object> props = Util.map("key", "value");
        Label[] labels = {Label.label("Test")};
        VirtualNode vn = new VirtualNode(labels, props);
        assertTrue("the id should be < 0", vn.getId() < 0);
        assertEquals(props, vn.getAllProperties());
        Iterator<Label> it = vn.getLabels().iterator();
        assertEquals(labels[0], it.next());
        assertFalse("should not have next label", it.hasNext());
    }

    @Test
    public void shouldCreateVirtualNodeWithRelationshipsTo() {
        Map<String, Object> startProps = Util.map("key", "value");
        Label[] startLabels = {Label.label("Test")};
        VirtualNode start = new VirtualNode(startLabels, startProps);
        assertTrue("the node id should be < 0", start.getId() < 0);
        assertEquals(startProps, start.getAllProperties());
        Iterator<Label> startLabelIt = start.getLabels().iterator();
        assertEquals(startLabels[0], startLabelIt.next());
        assertFalse("start should not have next label", startLabelIt.hasNext());

        Map<String, Object> endProps = Util.map("key", "value");
        Label[] endLabels = {Label.label("Test")};
        VirtualNode end = new VirtualNode(endLabels, endProps);
        assertTrue("the node id should be < 0", end.getId() < 0);
        assertEquals(endProps, end.getAllProperties());
        Iterator<Label> endLabelIt = end.getLabels().iterator();
        assertEquals(endLabels[0], endLabelIt.next());
        assertFalse("end should not have next label", endLabelIt.hasNext());

        RelationshipType relationshipType = RelationshipType.withName("TYPE");
        Relationship rel = start.createRelationshipTo(end, relationshipType);
        // Virtual Nodes/Relationships element ids are just String versions of ints.
        assertTrue("the rel id should be < 0", rel.getId() < 0);

        assertEquals(1, Iterables.count(start.getRelationships()));
        assertEquals(0, Iterables.count(start.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING, relationshipType)));
        assertEquals(end, start.getRelationships().iterator().next().getOtherNode(start));

        assertEquals(1, Iterables.count(end.getRelationships()));
        assertEquals(0, Iterables.count(end.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING, relationshipType)));
        assertEquals(start, end.getRelationships().iterator().next().getOtherNode(end));
    }

    @Test
    public void shouldCreateVirtualNodeWithRelationshipsFrom() {
        Map<String, Object> startProps = Util.map("key", "value");
        Label[] startLabels = {Label.label("Test")};
        VirtualNode start = new VirtualNode(startLabels, startProps);
        assertTrue("the node id should be < 0", start.getId() < 0);
        assertEquals(startProps, start.getAllProperties());
        Iterator<Label> startLabelIt = start.getLabels().iterator();
        assertEquals(startLabels[0], startLabelIt.next());
        assertFalse("start should not have next label", startLabelIt.hasNext());

        Map<String, Object> endProps = Util.map("key", "value");
        Label[] endLabels = {Label.label("Test")};
        VirtualNode end = new VirtualNode(endLabels, endProps);
        assertTrue("the node id should be < 0", end.getId() < 0);
        assertEquals(endProps, end.getAllProperties());
        Iterator<Label> endLabelIt = end.getLabels().iterator();
        assertEquals(endLabels[0], endLabelIt.next());
        assertFalse("end should not have next label", endLabelIt.hasNext());

        RelationshipType relationshipType = RelationshipType.withName("TYPE");
        Relationship rel = end.createRelationshipFrom(start, relationshipType);
        // Virtual Nodes/Relationships element ids are just String versions of ints.
        assertTrue("the rel id should be < 0", rel.getId() < 0);

        assertEquals(1, Iterables.count(start.getRelationships()));
        assertEquals(0, Iterables.count(start.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(start.getRelationships(Direction.OUTGOING, relationshipType)));
        assertEquals(end, start.getRelationships().iterator().next().getOtherNode(start));

        assertEquals(1, Iterables.count(end.getRelationships()));
        assertEquals(0, Iterables.count(end.getRelationships(Direction.OUTGOING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING)));
        assertEquals(1, Iterables.count(end.getRelationships(Direction.INCOMING, relationshipType)));
        assertEquals(start, end.getRelationships().iterator().next().getOtherNode(end));
    }

    @Test
    public void testVirtualNodesEqualEachother() {
        VirtualNode node1 = new VirtualNode(1L);
        VirtualNode node2 = new VirtualNode(2L);

        assertEquals(node1, node1);
        assertEquals(node2, node2);
        assertNotEquals(node1, node2);
    }
}
