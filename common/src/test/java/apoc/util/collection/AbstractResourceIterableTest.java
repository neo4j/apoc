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

import static apoc.util.collection.CollectionTestHelper.asIterator;
import static apoc.util.collection.CollectionTestHelper.emptyResourceIterator;
import static apoc.util.collection.CollectionTestHelper.resourceIterator;
import static apoc.util.collection.ResourceClosingIterator.newResourceIterator;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;

@RunWith(JUnitParamsRunner.class)
public class AbstractResourceIterableTest {
    @Test
    public void shouldDelegateToUnderlyingIterableForData() {
        // Given
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
        final var iterator = iterable.iterator();

        // Then
        assertThat( Iterators.asList(iterator)).containsExactlyElementsOf(items);
        assertTrue(iteratorClosed.booleanValue());
        assertFalse(iterableClosed.booleanValue());
    }

    @Test
    @Parameters({"0", "1", "2", "3", "10"})
    public void callToIteratorShouldCreateNewIterators(int numberOfIterators) {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorCount = new MutableInt();

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                iteratorCount.increment();
                return resourceIterator( asIterator(0), Resource.EMPTY);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        final var iterators = new ArrayList<ResourceIterator<Integer>>();
        for (int i = 0; i < numberOfIterators; i++) {
            iterators.add(iterable.iterator());
        }
        iterable.close();

        // Then
        assertTrue(iterableClosed.booleanValue());
        assertThat(iteratorCount.getValue()).isEqualTo(numberOfIterators);
        assertThat(iterators).containsOnlyOnceElementsOf(new HashSet<>(iterators));
    }

    @Test
    public void shouldCloseAllIteratorsIfCloseCalledOnIterable() {
        // Given
        final var iteratorsClosed = Arrays.asList(false, false, false, false);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            private int created;

            @Override
            protected ResourceIterator<Integer> newIterator() {
                var pos = created;
                created++;
                return resourceIterator(
                        asIterator(0), () -> iteratorsClosed.set(pos, true));
            }
        };
        iterable.iterator();
        iterable.iterator();
        iterable.iterator();
        iterable.close();

        // Then
        assertThat(iteratorsClosed.get(0)).isTrue();
        assertThat(iteratorsClosed.get(1)).isTrue();
        assertThat(iteratorsClosed.get(2)).isTrue();
        assertThat(iteratorsClosed.get(3)).isFalse();
    }

    @Test
    public void shouldCloseAllIteratorsEvenIfOnlySomeCloseCalled() {
        // Given
        final var iteratorsClosed = new MutableInt();

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator( asIterator(0), iteratorsClosed::increment);
            }
        };
        final var iterator1 = iterable.iterator();
        iterable.iterator();
        final var iterator2 = iterable.iterator();
        iterable.iterator();
        final var iterator3 = iterable.iterator();
        iterable.iterator();
        iterable.iterator();

        // go out of order
        iterator3.close();
        iterator1.close();
        iterator2.close();
        iterable.close();

        // Then
        assertThat(iteratorsClosed.getValue()).isEqualTo(7);
    }

    @Test
    public void failIteratorCreationAfterIterableClosed() {
        // Given
        final var iteratorCreated = new MutableBoolean(false);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                iteratorCreated.setTrue();
                return emptyResourceIterator();
            }
        };
        iterable.close();

        // Then
        assertThatThrownBy(iterable::iterator);
        assertFalse(iteratorCreated.booleanValue());
    }

    @Test
    public void shouldCloseIteratorIfCloseCalled() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorCreated = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                iteratorCreated.setTrue();
                return resourceIterator(List.of(0).iterator(), iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };
        assertThat(iterable.iterator().hasNext()).isTrue();
        iterable.close();

        // Then
        assertTrue(iteratorCreated.booleanValue());
        assertTrue(iteratorClosed.booleanValue());
        assertTrue(iterableClosed.booleanValue());
    }

    @Test
    public void shouldCloseIteratorOnForEachFailure() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);

        @SuppressWarnings("unchecked")
        final var intIterator = (Iterator<Integer>) mock(Iterator.class);
        when(intIterator.hasNext()).thenReturn(true).thenReturn(true);
        when(intIterator.next()).thenReturn(1).thenThrow(IllegalStateException.class);

        // When
        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator(intIterator, iteratorClosed::setTrue);
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // Then
        final var emitted = new ArrayList<Integer>();
        assertThatThrownBy(() -> {
            try (iterable) {
                for (var item : iterable) {
                    emitted.add(item);
                }
            }
        });
        assertThat(emitted).isEqualTo(List.of(1));
        assertTrue(iteratorClosed.booleanValue());
        assertTrue(iterableClosed.booleanValue());
    }

    @Test
    public void shouldCloseIteratorOnForEachCompletion() {
        // Given
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

        final var emitted = new ArrayList<Integer>();
        for (var item : iterable) {
            emitted.add(item);
        }

        // Then
        assertThat(emitted).isEqualTo(items);
        assertTrue(iteratorClosed.booleanValue());
        assertFalse(iterableClosed.booleanValue());
    }

    @Test
    public void streamShouldCloseIteratorAndIterable() {
        // Given
        final var iterableClosed = new MutableBoolean(false);
        final var iteratorClosed = new MutableBoolean(false);
        final var resourceIterator = newResourceIterator(asIterator(1, 2, 3), iteratorClosed::setTrue);

        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator;
            }

            @Override
            protected void onClosed() {
                iterableClosed.setTrue();
            }
        };

        // When
        try (Stream<Integer> stream = iterable.stream()) {
            final var result = stream.toList();
            assertThat(result).isEqualTo(asList(1, 2, 3));
        }

        // Then
        assertTrue(iterableClosed.booleanValue());
        assertTrue(iteratorClosed.booleanValue());
    }

    @Test
    public void streamShouldCloseMultipleOnCompleted() {
        // Given
        final var closed = new MutableInt();
        Resource resource = closed::incrementAndGet;
        final var resourceIterator = newResourceIterator(asIterator(1, 2, 3), resource, resource);

        final var iterable = new AbstractResourceIterable<Integer>() {
            @Override
            protected ResourceIterator<Integer> newIterator() {
                return resourceIterator;
            }
        };

        // When
        final var result = iterable.stream().toList();

        // Then
        assertThat(result).isEqualTo(asList(1, 2, 3));
        assertThat(closed.intValue()).isEqualTo(2);
    }
}
