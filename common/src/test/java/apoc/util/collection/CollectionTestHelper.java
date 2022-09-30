package apoc.util.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;

/**
 * Methods from Neo4js Iterators.java which are only needed in tests in APOC
 */
public class CollectionTestHelper
{
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
