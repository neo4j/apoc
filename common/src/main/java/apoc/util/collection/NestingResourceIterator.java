package apoc.util.collection;


import java.util.Iterator;
import org.neo4j.graphdb.ResourceIterator;

public abstract class NestingResourceIterator<T, U> extends PrefetchingResourceIterator<T>
{
    private final Iterator<U> source;
    private ResourceIterator<T> currentNestedIterator;

    protected NestingResourceIterator(Iterator<U> source) {
        this.source = source;
    }

    protected abstract ResourceIterator<T> createNestedIterator(U item);

    @Override
    protected T fetchNextOrNull() {
        if (currentNestedIterator == null || !currentNestedIterator.hasNext()) {
            while (source.hasNext()) {
                U currentSurfaceItem = source.next();
                close();
                currentNestedIterator = createNestedIterator(currentSurfaceItem);
                if (currentNestedIterator.hasNext()) {
                    break;
                }
            }
        }
        return currentNestedIterator != null && currentNestedIterator.hasNext() ? currentNestedIterator.next() : null;
    }

    @Override
    public void close() {
        if (currentNestedIterator != null) {
            currentNestedIterator.close();
        }
    }
}
