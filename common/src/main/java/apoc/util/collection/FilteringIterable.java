package apoc.util.collection;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criteria pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterable<T> implements Iterable<T> {
    private final Iterable<T> source;
    private final Predicate<T> predicate;

    public FilteringIterable(Iterable<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public Iterator<T> iterator() {
        return new FilteringIterator<>(source.iterator(), predicate);
    }
}