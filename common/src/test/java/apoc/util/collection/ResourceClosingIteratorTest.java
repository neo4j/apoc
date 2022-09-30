package apoc.util.collection;

import static apoc.util.collection.CollectionTestHelper.resourceIterator;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat( Iterators.asList(iterator)).containsExactlyElementsOf(items);
        assertThat(iteratorClosed.isTrue()).isTrue();
        assertThat(iterableClosed.isTrue()).isTrue();
    }
}
