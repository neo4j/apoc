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
package apoc.index;

import apoc.util.QueueBasedSpliterator;
import apoc.util.Util;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

/**
 * @author mh
 * @since 23.05.16
 */
public class SchemaIndex {

    private static final PropertyValueCount POISON = new PropertyValueCount("poison", "poison", "poison", -1);

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Transaction tx;

    @Context
    public TerminationGuard terminationGuard;

    public record SchemaListResult(
            @Description("The list of distinct values for the given property.") List<Object> value) {}

    @NotThreadSafe
    @Procedure("apoc.schema.properties.distinct")
    @Description("Returns all distinct `NODE` property values for the given key.")
    public Stream<SchemaListResult> distinct(
            @Name(value = "label", description = "The node label to find distinct properties on.") String label,
            @Name(value = "key", description = "The name of the property to find distinct values of.") String key) {
        List<Object> values = distinctCount(label, key)
                .map(propertyValueCount -> propertyValueCount.value)
                .collect(Collectors.toList());
        return Stream.of(new SchemaListResult(values));
    }

    @NotThreadSafe
    @Procedure("apoc.schema.properties.distinctCount")
    @Description("Returns all distinct property values and counts for the given key.")
    public Stream<PropertyValueCount> distinctCount(
            @Name(value = "label", defaultValue = "", description = "The node label to count distinct properties on.")
                    String labelName,
            @Name(
                            value = "key",
                            defaultValue = "",
                            description = "The name of the property to count distinct values of.")
                    String keyName) {

        BlockingQueue<PropertyValueCount> queue = new LinkedBlockingDeque<>(100);
        Iterable<IndexDefinition> indexDefinitions =
                (labelName.isEmpty()) ? Util.getIndexes(tx) : Util.getIndexes(tx, Label.label(labelName));

        Util.newDaemonThread(() -> StreamSupport.stream(indexDefinitions.spliterator(), true)
                        .filter(IndexDefinition::isNodeIndex)
                        .filter(indexDefinition -> isIndexCoveringProperty(indexDefinition, keyName))
                        .map(indexDefinition -> scanIndexDefinitionForKeys(indexDefinition, keyName, queue, labelName))
                        .collect(new QueuePoisoningCollector(queue, POISON)))
                .start();

        return StreamSupport.stream(
                        new QueueBasedSpliterator<>(queue, POISON, terminationGuard, Integer.MAX_VALUE), false)
                .distinct();
    }

    private Object scanIndexDefinitionForKeys(
            IndexDefinition indexDefinition,
            @Name(value = "key", defaultValue = "") String keyName,
            BlockingQueue<PropertyValueCount> queue,
            String labelName) {
        try (Transaction threadTx = db.beginTx()) {
            KernelTransaction ktx = ((InternalTransaction) threadTx).kernelTransaction();
            Iterable<String> keys =
                    keyName.isEmpty() ? indexDefinition.getPropertyKeys() : Collections.singletonList(keyName);
            for (String key : keys) {
                try (KernelStatement ignored = (KernelStatement) ktx.acquireStatement()) {
                    SchemaRead schemaRead = ktx.schemaRead();
                    TokenRead tokenRead = ktx.tokenRead();
                    Read read = ktx.dataRead();
                    CursorFactory cursors = ktx.cursors();

                    int[] propertyKeyIds = StreamSupport.stream(
                                    indexDefinition.getPropertyKeys().spliterator(), false)
                            .mapToInt(tokenRead::propertyKey)
                            .toArray();

                    SchemaDescriptor schema;
                    if (isFullText(indexDefinition)) {
                        // since fullText idx are multi-label, we need to handle them differently
                        int[] labelIds = Iterables.stream(indexDefinition.getLabels())
                                .mapToInt(lbl -> tokenRead.nodeLabel(lbl.name()))
                                .toArray();
                        schema = SchemaDescriptors.fulltext(EntityType.NODE, labelIds, propertyKeyIds);
                    } else {
                        String label =
                                Iterables.single(indexDefinition.getLabels()).name();
                        schema = SchemaDescriptors.forLabel(tokenRead.nodeLabel(label), propertyKeyIds);
                    }

                    IndexDescriptor indexDescriptor = Iterators.firstOrNull(schemaRead.index(schema));
                    if (indexDescriptor == null) {
                        return null;
                    }
                    scanIndex(queue, indexDefinition, key, read, cursors, indexDescriptor, ktx, labelName, threadTx);
                }
            }
            threadTx.commit();
            return null;
        }
    }

    private void scanIndex(
            BlockingQueue<PropertyValueCount> queue,
            IndexDefinition indexDefinition,
            String key,
            Read read,
            CursorFactory cursors,
            IndexDescriptor indexDescriptor,
            KernelTransaction ktx,
            String lblName,
            Transaction threadTx) {
        try (NodeValueIndexCursor cursor =
                cursors.allocateNodeValueIndexCursor(ktx.cursorContext(), ktx.memoryTracker())) {
            // we need to using IndexOrder.NONE here to prevent an exception
            // however the index guarantees to be scanned in order unless
            // there are writes done in the same tx beforehand - which we don't do.

            final IndexReadSession indexSession;
            try {
                indexSession = read.indexReadSession(indexDescriptor);
            } catch (Exception e) {
                // we skip indexScan if it's still populating
                if (e.getMessage().contains("Index is still populating")) {
                    return;
                }
                throw e;
            }
            if (isFullText(indexDefinition)) {
                // similar to db.index.fulltext.queryNodes procedure
                read.nodeIndexSeek(
                        ktx.queryContext(),
                        indexSession,
                        cursor,
                        IndexQueryConstraints.unconstrained(),
                        PropertyIndexQuery.fulltextSearch("*"));
            } else {
                read.nodeIndexScan(indexSession, cursor, IndexQueryConstraints.unorderedValues());
            }

            Map<String, Map<Object, Integer>> valueCountMap = new HashMap<>();

            final Iterable<Label> labels =
                    lblName.isEmpty() ? indexDefinition.getLabels() : Collections.singleton(Label.label(lblName));

            while (cursor.next()) {
                final Node node = threadTx.getNodeById(cursor.nodeReference());

                // we increment count only if corresponding prop is present
                final Object property = node.getProperty(key, null);
                if (property == null) continue;

                labels.forEach(label -> {
                    // we increment count only if corresponding label is present
                    final boolean hasLabel = node.hasLabel(label);
                    if (hasLabel) {

                        final Map<Object, Integer> orDefault =
                                valueCountMap.computeIfAbsent(label.name(), i -> new HashMap<>());

                        // increment map count
                        orDefault.merge(property, 1, Integer::sum);
                    }
                });
            }

            valueCountMap.forEach((label, propMap) -> {
                propMap.forEach((k, v) -> {
                    putIntoQueue(queue, indexDefinition, key, k, v, label);
                });
            });
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isFullText(IndexDefinition indexDefinition) {
        return indexDefinition.getIndexType().equals(IndexType.FULLTEXT);
    }

    private void putIntoQueue(
            BlockingQueue<PropertyValueCount> queue,
            IndexDefinition indexDefinition,
            String key,
            Object value,
            long count,
            String labelName) {
        // if no value returned, like in testDistinctWithNoPreviousNodesShouldNotHangs
        if (value == null) {
            return;
        }
        try {
            queue.put(new PropertyValueCount(labelName, key, value, count));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isIndexCoveringProperty(IndexDefinition indexDefinition, String propertyKeyName) {
        return propertyKeyName.isEmpty() || contains(indexDefinition.getPropertyKeys(), propertyKeyName);
    }

    private boolean contains(Iterable<String> list, String search) {
        for (String element : list) {
            if (element.equals(search)) {
                return true;
            }
        }
        return false;
    }

    public static class PropertyValueCount {
        @Description("The label of the node.")
        public String label;

        @Description("The name of the property key.")
        public String key;

        @Description("The distinct value.")
        public Object value;

        @Description("The number of occurrences of the value.")
        public long count;

        public PropertyValueCount(String label, String key, Object value, long count) {
            this.label = label;
            this.key = key;
            this.value = value;
            this.count = count;
        }

        @Override
        public String toString() {
            return "PropertyValueCount{" + "label='"
                    + label + '\'' + ", key='"
                    + key + '\'' + ", value='"
                    + value + '\'' + ", count="
                    + count + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PropertyValueCount that = (PropertyValueCount) o;

            return count == that.count
                    && Objects.equals(label, that.label)
                    && Objects.equals(key, that.key)
                    && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(label, key, value, count);
        }
    }
}
