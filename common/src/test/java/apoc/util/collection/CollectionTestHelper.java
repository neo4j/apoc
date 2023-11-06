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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;

/**
 * Methods from Neo4js Iterators.java which are only needed in tests in APOC
 */
public class CollectionTestHelper {
    public static <T> ResourceIterator<T> resourceIterator(final Iterator<T> iterator, final Resource resource) {
        return new PrefetchingResourceIterator<>() {
            @Override
            public void close() {
                resource.close();
            }

            @Override
            protected T fetchNextOrNull() {
                return iterator.hasNext() ? iterator.next() : null;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> ResourceIterator<T> emptyResourceIterator() {
        return (ResourceIterator<T>) EmptyResourceIterator.EMPTY_RESOURCE_ITERATOR;
    }

    private static class EmptyResourceIterator<E> implements ResourceIterator<E> {
        private static final ResourceIterator<Object> EMPTY_RESOURCE_ITERATOR = new EmptyResourceIterator<>();

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            throw new NoSuchElementException();
        }
    }

    public static Iterator<Integer> asIterator(final int... array) {
        return new Iterator<>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < array.length;
            }

            @Override
            public Integer next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return array[index++];
            }
        };
    }
}
