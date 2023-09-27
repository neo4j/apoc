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
package apoc.create;

import apoc.get.Get;
import apoc.result.*;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import apoc.uuid.UuidUtil;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.procedure.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class Create {

    public static final String[] EMPTY_ARRAY = new String[0];

    @Context
    public Transaction tx;

    @Procedure(name = "apoc.create.node", mode = Mode.WRITE)
    @Description("Creates a `NODE` with the given dynamic labels.")
    public Stream<NodeResult> node(@Name("labels") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return Stream.of(new NodeResult(setProperties(tx.createNode(Util.labels(labelNames)),props)));
    }


    @Procedure(name = "apoc.create.addLabels", mode = Mode.WRITE)
    @Description("Adds the given labels to the given `NODE` values.")
    public Stream<NodeResult> addLabels(@Name("nodes") Object nodes, @Name("labels") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get((InternalTransaction) tx).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure(name = "apoc.create.setProperty", mode = Mode.WRITE)
    @Description("Sets the given property to the given `NODE` values.")
    public Stream<NodeResult> setProperty(@Name("nodes") Object nodes, @Name("key") String key, @Name("value") Object value) {
        return new Get((InternalTransaction) tx).nodes(nodes).map((r) -> {
            setProperty(r.node, key,toPropertyValue(value));
            return r;
        });
    }

    @Procedure(name = "apoc.create.setRelProperty", mode = Mode.WRITE)
    @Description("Sets the given property on the `RELATIONSHIP` values.")
    public Stream<RelationshipResult> setRelProperty(@Name("rels") Object rels, @Name("key") String key, @Name("value") Object value) {
        return new Get((InternalTransaction) tx).rels(rels).map((r) -> {
            setProperty(r.rel,key,toPropertyValue(value));
            return r;
        });
    }

    @Procedure(name = "apoc.create.setProperties", mode = Mode.WRITE)
    @Description("Sets the given properties to the given `NODE` values.")
    public Stream<NodeResult> setProperties(@Name("nodes") Object nodes, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return new Get((InternalTransaction) tx).nodes(nodes).map((r) -> {
            setProperties(r.node, Util.mapFromLists(keys,values));
            return r;
        });
    }

    @Procedure(name = "apoc.create.removeProperties", mode = Mode.WRITE)
    @Description("Removes the given properties from the given `NODE` values.")
    public Stream<NodeResult> removeProperties(@Name("nodes") Object nodes, @Name("keys") List<String> keys) {
        return new Get((InternalTransaction) tx).nodes(nodes).map((r) -> {
            keys.forEach( r.node::removeProperty );
            return r;
        });
    }

    @Procedure(name = "apoc.create.setRelProperties", mode = Mode.WRITE)
    @Description("Sets the given properties on the `RELATIONSHIP` values.")
    public Stream<RelationshipResult> setRelProperties(@Name("rels") Object rels, @Name("keys") List<String> keys, @Name("values") List<Object> values) {
        return new Get((InternalTransaction) tx).rels(rels).map((r) -> {
            setProperties(r.rel, Util.mapFromLists(keys,values));
            return r;
        });
    }

    @Procedure(name = "apoc.create.removeRelProperties", mode = Mode.WRITE)
    @Description("Removes the given properties from the given `RELATIONSHIP` values.")
    public Stream<RelationshipResult> removeRelProperties(@Name("rels") Object rels, @Name("keys") List<String> keys) {
        return new Get((InternalTransaction) tx).rels(rels).map((r) -> {
            keys.forEach( r.rel::removeProperty);
            return r;
        });
    }

    @Procedure(name = "apoc.create.setLabels", mode = Mode.WRITE)
    @Description("Sets the given labels to the given `NODE` values. Non-matching labels are removed from the nodes.")
    public Stream<NodeResult> setLabels(@Name("nodes") Object nodes, @Name("labels") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get((InternalTransaction) tx).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : node.getLabels()) {
                if (labelNames.contains(label.name())) continue;
                node.removeLabel(label);
            }
            for (Label label : labels) {
                if (node.hasLabel(label)) continue;
                node.addLabel(label);
            }
            return r;
        });
    }

    @Procedure(name = "apoc.create.removeLabels", mode = Mode.WRITE)
    @Description("Removes the given labels from the given `NODE` values.")
    public Stream<NodeResult> removeLabels(@Name("nodes") Object nodes, @Name("labels") List<String> labelNames) {
        Label[] labels = Util.labels(labelNames);
        return new Get((InternalTransaction) tx).nodes(nodes).map((r) -> {
            Node node = r.node;
            for (Label label : labels) {
                node.removeLabel(label);
            }
            return r;
        });
    }

    @Procedure(name = "apoc.create.nodes", mode = Mode.WRITE)
    @Description("Creates `NODE` values with the given dynamic labels.")
    public Stream<NodeResult> nodes(@Name("labels") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = Util.labels(labelNames);
        return props.stream().map(p -> new NodeResult(setProperties(tx.createNode(labels), p)));
    }

    @Procedure(name = "apoc.create.relationship", mode = Mode.WRITE)
    @Description("Creates a `RELATIONSHIP` with the given dynamic relationship type.")
    public Stream<RelationshipResult> relationship(@Name("from") Node from,
                                                   @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                   @Name("to") Node to) {
        VirtualRelationship.validateNodes(from, to);
        return Stream.of(new RelationshipResult(setProperties(from.createRelationshipTo(to, withName(relType)), props)));
    }

    @Procedure("apoc.create.vNode")
    @Description("Returns a virtual `NODE`.")
    public Stream<NodeResult> vNode(@Name("labels") List<String> labelNames, @Name("props") Map<String, Object> props) {
        return Stream.of(new NodeResult(vNodeFunction(labelNames, props)));
    }

    @UserFunction("apoc.create.vNode")
    @Description("Returns a virtual `NODE`.")
    public Node vNodeFunction(@Name("labels") List<String> labelNames, @Name(value = "props",defaultValue = "{}") Map<String, Object> props) {
        return new VirtualNode(Util.labels(labelNames), props);
    }

    @UserFunction("apoc.create.virtual.fromNode")
    @Description("Returns a virtual `NODE` from the given existing `NODE`. The virtual `NODE` only contains the requested properties.")
    public Node virtualFromNodeFunction(@Name("node") Node node, @Name("propertyNames") List<String> propertyNames) {
        return new VirtualNode(node, propertyNames);
    }

    @Procedure("apoc.create.vNodes")
    @Description("Returns virtual `NODE` values.")
    public Stream<NodeResult> vNodes(@Name("labels") List<String> labelNames, @Name("props") List<Map<String, Object>> props) {
        Label[] labels = Util.labels(labelNames);
        return props.stream().map(p -> new NodeResult(new VirtualNode(labels, p)));
    }

    @Procedure("apoc.create.vRelationship")
    @Description("Returns a virtual `RELATIONSHIP`.")
    public Stream<RelationshipResult> vRelationship(@Name("from") Node from, @Name("relType") String relType, @Name("props") Map<String, Object> props, @Name("to") Node to) {
        return Stream.of(new RelationshipResult(vRelationshipFunction(from, relType, props, to)));
    }

    @UserFunction("apoc.create.vRelationship")
    @Description("Returns a virtual `RELATIONSHIP`.")
    public Relationship vRelationshipFunction(@Name("from") Node from, @Name("relType") String relType, @Name("props") Map<String, Object> props, @Name("to") Node to) {
        return new VirtualRelationship(from, to, withName(relType)).withProperties(props);
    }

    @Procedure("apoc.create.virtualPath")
    @Description("Returns a virtual `PATH`.")
    public Stream<VirtualPathResult> virtualPath(@Name("labelsN") List<String> labelsN, @Name("n") Map<String, Object> n,
                                                  @Name("relType") String relType, @Name("props") Map<String, Object> props,
                                                  @Name("labelsM") List<String> labelsM, @Name("m") Map<String, Object> m) {
        RelationshipType type = withName(relType);
        VirtualNode from = new VirtualNode(Util.labels(labelsN), n);
        VirtualNode to = new VirtualNode(Util.labels(labelsM), m);
        Relationship rel = new VirtualRelationship(from, to, type).withProperties(props);
        return Stream.of(new VirtualPathResult(from, rel, to));
    }

    @Procedure("apoc.create.clonePathToVirtual")
    @Description("Takes the given `PATH` and returns a virtual representation of it.")
    public Stream<PathResult> clonePathToVirtual(@Name("path") Path path) {
        // given that it accepts a single path as a input parameter
        // the `relationshipMap` (i.e. to avoid duplicated rels) is not necessary
        return Stream.of(createVirtualPath(path, null));
    }

    @Procedure("apoc.create.clonePathsToVirtual")
    @Description("Takes the given `LIST<PATH>` and returns a virtual representation of them.")
    public Stream<PathResult> clonePathsToVirtual(@Name("paths") List<Path> paths) {
        Map<String, Relationship> createdRelationships = new HashMap<>();
        return paths.stream().map(path -> createVirtualPath(path, createdRelationships));
    }

    private PathResult createVirtualPath(Path path, Map<String, Relationship> createdRelationships) {
        final Iterable<Relationship> relationships = path.relationships();
        final Node first = path.startNode();
        VirtualPath virtualPath = new VirtualPath(new VirtualNode(first, Iterables.asList(first.getPropertyKeys())));
        for (Relationship rel : relationships) {
            final Relationship vRel = getVirtualRelPossiblyFromCache(createdRelationships, rel);
            virtualPath.addRel(vRel);
        }
        return new PathResult(virtualPath);
    }

    private static Relationship getVirtualRelPossiblyFromCache(Map<String, Relationship> cacheRel, Relationship rel) {
        if (cacheRel == null) {
            return getVirtualRel(rel);
        }
        return cacheRel.compute(rel.getElementId(), (k, v) -> {
            if (v == null) {
                return getVirtualRel(rel);
            }
            return v;
        });
    }

    private static Relationship getVirtualRel(Relationship rel) {
        VirtualNode start = VirtualNode.from(rel.getStartNode());
        VirtualNode end = VirtualNode.from(rel.getEndNode());
        return VirtualRelationship.from(start, end, rel);
    }

    private <T extends Entity> T setProperties(T pc, Map<String, Object> p) {
        if (p == null) return pc;
        for (Map.Entry<String, Object> entry : p.entrySet()) {
            setProperty(pc, entry.getKey(), entry.getValue());
        }
        return pc;
    }

    private <T extends Entity> void setProperty(T pc, String key, Object value) {
        if (value == null) pc.removeProperty(key);
        else pc.setProperty(key, toPropertyValue(value));
    }

    @UserFunction(name = "apoc.create.uuid", deprecatedBy = "Neo4j randomUUID() function")
    @Deprecated
    @Description("Returns a UUID.")
    public String uuid() {
        return UUID.randomUUID().toString();
    }

    @UserFunction("apoc.create.uuidBase64")
    @Description("Returns a UUID encoded with base64.")
    public String uuidBase64() {
        return UuidUtil.generateBase64Uuid(UUID.randomUUID());
    }

    @UserFunction("apoc.create.uuidBase64ToHex")
    @Description("Takes the given base64 encoded UUID and returns it as a hexadecimal `STRING`.")
    public String uuidBase64ToHex(@Name("base64Uuid") String base64Uuid) {
        return UuidUtil.fromBase64ToHex(base64Uuid);
    }

    @UserFunction("apoc.create.uuidHexToBase64")
    @Description("Takes the given UUID represented as a hexadecimal `STRING` and returns it encoded with base64.")
    public String uuidHexToBase64(@Name("uuid") String uuidHex) {
        return UuidUtil.fromHexToBase64(uuidHex);
    }

    private Object toPropertyValue(Object value) {
        if (value instanceof Iterable) {
            Iterable it = (Iterable) value;
            Object first = Iterables.firstOrNull(it);
            if (first == null) return EMPTY_ARRAY;
            return Iterables.asArray(first.getClass(), it);
        }
        return value;
    }

    @Procedure(name = "apoc.create.uuids", deprecatedBy = "Neo4j's randomUUID() function can be used as a replacement, for example: `UNWIND range(0,$count) AS row RETURN row, randomUUID() AS uuid`")
    @Deprecated
    @Description("Returns a stream of UUIDs.")
    public Stream<UUIDResult> uuids(@Name("count") long count) {
        return LongStream.range(0, count).mapToObj(UUIDResult::new);
    }

    public static class UUIDResult {
        public final long row;
        public final String uuid;

        public UUIDResult(long row) {
            this.row = row;
            this.uuid = UUID.randomUUID().toString();
            // TODO Long.toHexString(uuid.getMostSignificantBits())+Long.toHexString(uuid.getLeastSignificantBits());
        }
    }

}
