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
package apoc.util.collection;

import static apoc.util.collection.CollectionTestHelper.resourceIterator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.neo4j.graphdb.ResourceIterator;

public class ResourceClosingIteratorTest {
    @Test
    public void fromResourceIterableShouldCloseParentIterable() {
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        final var items = Arrays.asList(0, 1, 2);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator(items.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };
        ResourceIterator<Integer> iterator = ResourceClosingIterator.fromResourceIterable(iterable);

        // Then
        assertThat(Iterators.asList(iterator)).containsExactlyElementsOf(items);
        assertTrue(iteratorClosed.booleanValue());
        assertTrue(iterableClosed.booleanValue());
    }
}
