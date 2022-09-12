package apoc.util.collection;

import java.util.Objects;

/**
 * Utility to handle pairs of objects.
 *
 * @param <T1> the type of the {@link #first() first value} of the pair.
 * @param <T2> the type of the {@link #other() other value} of the pair.
 */
public abstract class Pair<T1, T2> {
    /**
     * Create a new pair of objects.
     *
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     * @param <T1> the type of the first object in the pair
     * @param <T2> the type of the second object in the pair
     * @return a new pair of the two parameters.
     */
    public static <T1, T2> Pair<T1, T2> pair(final T1 first, final T2 other) {
        return new Pair<>() {
            @Override
            public T1 first() {
                return first;
            }

            @Override
            public T2 other() {
                return other;
            }
        };
    }

    /**
     * Alias of {@link #pair(Object, Object)}.
     * @param first the first object in the pair.
     * @param other the other object in the pair.
     * @param <T1> the type of the first object in the pair
     * @param <T2> the type of the second object in the pair
     * @return a new pair of the two parameters.
     */
    public static <T1, T2> Pair<T1, T2> of(final T1 first, final T2 other) {
        return pair(first, other);
    }

    Pair() {
        // package private, limited number of subclasses
    }

    /**
     * @return the first object in the pair.
     */
    public abstract T1 first();

    /**
     * @return the other object in the pair.
     */
    public abstract T2 other();

    @Override
    public String toString() {
        return "(" + first() + ", " + other() + ")";
    }

    @Override
    public int hashCode() {
        return (31 * hashCode(first())) | hashCode(other());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Pair<?, ?> that) {
            return Objects.equals(this.other(), that.other()) && Objects.equals(this.first(), that.first());
        }
        return false;
    }

    private static int hashCode(Object obj) {
        return obj == null ? 0 : obj.hashCode();
    }
}
