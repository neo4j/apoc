package apoc.util.collection;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * An iterator which filters another iterator, only letting items with certain
 * criteria pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterator<T> extends PrefetchingIterator<T>
{
    private final Iterator<T> source;
    private final Predicate<T> predicate;

    public FilteringIterator(Iterator<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    protected T fetchNextOrNull() {
        while (source.hasNext()) {
            T testItem = source.next();
            if (predicate.test(testItem)) {
                return testItem;
            }
        }
        return null;
    }
}
