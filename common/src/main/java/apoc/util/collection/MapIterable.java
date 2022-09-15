package apoc.util.collection;


import java.util.Iterator;
import java.util.function.Function;

class MapIterable<FROM, TO> implements Iterable<TO> {
    private final Iterable<FROM> from;
    private final Function<? super FROM, ? extends TO> function;

    MapIterable(Iterable<FROM> from, Function<? super FROM, ? extends TO> function) {
        this.from = from;
        this.function = function;
    }

    @Override
    public Iterator<TO> iterator() {
        return new MapIterator<>(from.iterator(), function);
    }

    static class MapIterator<FROM, TO> implements Iterator<TO> {
        private final Iterator<FROM> fromIterator;
        private final Function<? super FROM, ? extends TO> function;

        MapIterator(Iterator<FROM> fromIterator, Function<? super FROM, ? extends TO> function) {
            this.fromIterator = fromIterator;
            this.function = function;
        }

        @Override
        public boolean hasNext() {
            return fromIterator.hasNext();
        }

        @Override
        public TO next() {
            FROM from = fromIterator.next();

            return function.apply(from);
        }

        @Override
        public void remove() {
            fromIterator.remove();
        }
    }
}

