package org.neo4j.cypher.export;

import apoc.util.Util;
import apoc.util.collection.Iterables;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CypherResultSubGraph implements SubGraph {

    private final SortedMap<String, Node> nodes = new TreeMap<>();
    private final SortedMap<String, Relationship> relationships = new TreeMap<>();
    private final Collection<Label> labels = new HashSet<>();
    private final Collection<RelationshipType> types = new HashSet<>();
    private final Collection<IndexDefinition> indexes = new HashSet<>();
    private final Collection<ConstraintDefinition> constraints = new HashSet<>();

    public void add(Node node) {
        final String id = node.getElementId();
        if (!nodes.containsKey(id)) {
            addNode(id, node);
        }
    }

    void addNode(String id, Node data) {
        nodes.put(id, data);
        labels.addAll(Iterables.asList(data.getLabels()));
    }

    public void add(Relationship rel) {
        final String id = rel.getElementId();
        if (!relationships.containsKey(id)) {
            addRel(id, rel);
            add(rel.getStartNode());
            add(rel.getEndNode());
        }
    }

    public static SubGraph from(Transaction tx, Result result, boolean addBetween) {
        final CypherResultSubGraph graph = new CypherResultSubGraph();
        final List<String> columns = result.columns();
        result.forEachRemaining(row -> {
            for (String column : columns) {
                final Object value = row.get(column);
                graph.addToGraph(value);
            }
        });
        for (IndexDefinition def : tx.schema().getIndexes()) {
            if (def.getIndexType() != IndexType.LOOKUP) {
                if (def.isNodeIndex()) {
                    for (Label label : def.getLabels()) {
                        if (graph.getLabels().contains(label)) {
                            graph.addIndex(def);
                            break;
                        }
                    }
                } else {
                    for (RelationshipType type : def.getRelationshipTypes()) {
                        if (graph.getTypes().contains(type)) {
                            graph.addIndex(def);
                            break;
                        }
                    }
                }
            }
        }
        for (ConstraintDefinition def : tx.schema().getConstraints()) {
            if (Util.isNodeCategory( def.getConstraintType() ) && graph.getLabels().contains(def.getLabel())) {
                graph.addConstraint(def);
            }
        } if (addBetween) {
            graph.addRelationshipsBetweenNodes();
        }
        return graph;
    }

    private void addIndex(IndexDefinition def) {
        indexes.add(def);
    }

    private void addConstraint(ConstraintDefinition def) {
        constraints.add(def);
    }

    private void addRelationshipsBetweenNodes() {
        Set<Node> newNodes = new HashSet<>();
        for (Node node : nodes.values()) {
            for (Relationship relationship : node.getRelationships()) {
                if (!relationships.containsKey(relationship.getElementId())) {
                    continue;
                }

                final Node other = relationship.getOtherNode(node);
                if (nodes.containsKey(other.getElementId()) || newNodes.contains(other)) {
                    continue;
                }
                newNodes.add(other);
            }
        }
        for (Node node : newNodes) {
            add(node);
        }
    }

    private void addToGraph(Object value) {
        if (value instanceof Node) {
            add((Node) value);
        }
        if (value instanceof Relationship) {
            add((Relationship) value);
        }
        if (value instanceof Iterable) {
            for (Object inner : (Iterable) value) {
                addToGraph(inner);
            }
        }
    }

    @Override
    public Iterable<Node> getNodes() {
        return nodes.values();
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return relationships.values();
    }

    public Collection<Label> getLabels() {
        return labels;
    }

    public Collection<RelationshipType> getTypes() {
        return types;
    }

    void addRel(String id, Relationship rel) {
        relationships.put(id, rel);
        types.add(rel.getType());
    }

    @Override
    public boolean contains(Relationship relationship) {
        return relationships.containsKey(relationship.getElementId());
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        return indexes;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        return constraints;
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(Label label) {
        return constraints.stream()
                .filter(c -> Util.isNodeCategory(c.getConstraintType()))
                .filter(c -> c.getLabel().equals(label))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(RelationshipType type) {
        return constraints.stream()
                .filter(c -> Util.isRelationshipCategory(c.getConstraintType()))
                .filter(c -> c.getRelationshipType().equals(type))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(Label label) {
        return indexes.stream()
                .filter(IndexDefinition::isNodeIndex)
                .filter(idx -> StreamSupport.stream(idx.getLabels().spliterator(), false).anyMatch(lb -> lb.equals(label)))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType type) {
        return indexes.stream()
                .filter(IndexDefinition::isRelationshipIndex)
                .filter(idx -> StreamSupport.stream(idx.getRelationshipTypes().spliterator(), false)
                        .anyMatch(relType -> relType.name().equals(type.name())))
                .collect(Collectors.toSet());
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse() {
        return Collections.unmodifiableCollection(types);
    }

    @Override
    public Iterable<Label> getAllLabelsInUse() {
        return Collections.unmodifiableCollection(labels);
    }

    @Override
    public long countsForRelationship(Label start, RelationshipType type, Label end) {
        return relationships.values().stream()
                .filter(r -> {
                    boolean matchType = r.getType().equals(type);
                    boolean matchStart = start == null || r.getStartNode().hasLabel(start);
                    boolean matchEnd = end == null || r.getEndNode().hasLabel(end);
                    return matchType && matchStart && matchEnd;
                })
                .count();
    }

    @Override
    public long countsForNode(Label label) {
        return nodes.values().stream()
                .filter(n -> n.hasLabel(label))
                .count();
    }

    @Override
    public Iterator<Node> findNodes(Label label) {
        return nodes.values().stream()
                .filter(n -> n.hasLabel(label))
                .iterator();
    }
}
