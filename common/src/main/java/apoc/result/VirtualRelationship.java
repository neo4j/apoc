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

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * @author mh
 * @since 16.03.16
 */
public class VirtualRelationship implements Relationship {
    private static final String ERROR_NODE_NULL = "The inserted %s Node is null";
    public static final String ERROR_START_NODE_NULL = String.format(ERROR_NODE_NULL, "Start");
    public static final String ERROR_END_NODE_NULL = String.format(ERROR_NODE_NULL, "End");

    private static AtomicLong MIN_ID = new AtomicLong(-1);
    private final Node startNode;
    private final Node endNode;
    private final RelationshipType type;
    private final long id;
    private final String elementId;
    private final Map<String, Object> props = new HashMap<>();

    public VirtualRelationship(Node startNode, Node endNode, RelationshipType type, Map<String, Object> props) {
        this(startNode, endNode, type);
        this.props.putAll(props);
    }

    public VirtualRelationship(
            long id, String elementId, Node startNode, Node endNode, RelationshipType type, Map<String, Object> props) {
        validateNodes(startNode, endNode);
        this.id = id;
        this.elementId = elementId;
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
        this.props.putAll(props);
    }

    public VirtualRelationship(Node startNode, Node endNode, RelationshipType type) {
        validateNodes(startNode, endNode);
        this.id = MIN_ID.getAndDecrement();
        this.elementId = UUID.randomUUID().toString();
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
    }

    @SuppressWarnings("unused") // used from extended
    public VirtualRelationship(
            long id, Node startNode, Node endNode, RelationshipType type, Map<String, Object> props) {
        validateNodes(startNode, endNode);
        this.id = id;
        this.elementId = UUID.randomUUID().toString();
        this.startNode = startNode;
        this.endNode = endNode;
        this.type = type;
        this.props.putAll(props);
    }

    public static Relationship from(VirtualNode start, VirtualNode end, Relationship rel) {
        return new VirtualRelationship(start, end, rel.getType()).withProperties(rel.getAllProperties());
    }

    public static void validateNodes(Node startNode, Node endNode) {
        Objects.requireNonNull(startNode, ERROR_START_NODE_NULL);
        Objects.requireNonNull(endNode, ERROR_END_NODE_NULL);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getElementId() {
        return elementId;
    }

    @Override
    public void delete() {
        if (getStartNode() instanceof VirtualNode) ((VirtualNode) getStartNode()).delete(this);
        if (getEndNode() instanceof VirtualNode) ((VirtualNode) getEndNode()).delete(this);
    }

    @Override
    public Node getStartNode() {
        return startNode;
    }

    @Override
    public Node getEndNode() {
        return endNode;
    }

    @Override
    public Node getOtherNode(Node node) {
        return node.equals(startNode) ? endNode : node.equals(endNode) ? startNode : null;
    }

    @Override
    public Node[] getNodes() {
        return new Node[] {startNode, endNode};
    }

    @Override
    public RelationshipType getType() {
        return type;
    }

    @Override
    public boolean isType(RelationshipType relationshipType) {
        return relationshipType.name().equals(type.name());
    }

    @Override
    public boolean hasProperty(String s) {
        return props.containsKey(s);
    }

    @Override
    public Object getProperty(String s) {
        return props.get(s);
    }

    @Override
    public Object getProperty(String s, Object o) {
        Object res = props.get(s);
        return res == null ? o : res;
    }

    @Override
    public void setProperty(String s, Object o) {
        props.put(s, o);
    }

    @Override
    public Object removeProperty(String s) {
        return props.remove(s);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return props.keySet();
    }

    @Override
    public Map<String, Object> getProperties(String... strings) {
        Map<String, Object> res = new LinkedHashMap<>(props);
        res.keySet().retainAll(asList(strings));
        return res;
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return props;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || o instanceof Relationship && Objects.equals(getElementId(), ((Relationship) o).getElementId());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(elementId);
    }

    public Relationship withProperties(Map<String, Object> props) {
        this.props.putAll(props);
        return this;
    }

    @Override
    public String toString() {
        return "VirtualRelationship{" + "startNode=" + startNode.getLabels() + ", endNode=" + endNode.getLabels() + ", "
                + "type=" + type + '}';
    }
}
