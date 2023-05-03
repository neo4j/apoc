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

import static apoc.util.collection.CollectionTestHelper.emptyResourceIterator;
import static apoc.util.collection.ResourceClosingIterator.newResourceIterator;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.neo4j.graphdb.ResourceIterator;

public class IterablesTest {

    @Test
    public void count() {
        // Given
        final var subjects = asList(1, 2, 3, 4, 5);
        final var iteratorClosed = new MutableBoolean(false);
        final var iterableClosed = new MutableBoolean(false);
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return newResourceIterator(subjects.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // when
        long count = Iterables.count(iterable);

        // then
        assertThat(count).isEqualTo(subjects.size());
        assertTrue(iteratorClosed.booleanValue());
        assertTrue(iterableClosed.booleanValue());
    }

    @Test
    public void firstNoItems() {
        // Given
        final var iteratorClosed = new MutableBoolean(false);
        final var iterableClosed = new MutableBoolean(false);
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return newResourceIterator( emptyResourceIterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // when
        assertThatThrownBy(() -> Iterables.first(iterable));

        // then
        assertTrue(iteratorClosed.booleanValue());
        assertTrue(iterableClosed.booleanValue());
    }

    @Test
    public void firstWithItems() {
        // Given
        final var subjects = asList(1, 2, 3, 4, 5);
        final var iteratorClosed = new MutableBoolean(false);
        final var iterableClosed = new MutableBoolean(false);
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return newResourceIterator(subjects.iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // when
        long first = Iterables.first(iterable);

        // then
        assertThat(first).isEqualTo(1);
        assertTrue(iteratorClosed.booleanValue());
        assertTrue(iterableClosed.booleanValue());
    }
}
