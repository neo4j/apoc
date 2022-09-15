package apoc.util.collection;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;

public abstract class PrefetchingResourceIterator<T> extends PrefetchingIterator<T> implements ResourceIterator<T> {}
