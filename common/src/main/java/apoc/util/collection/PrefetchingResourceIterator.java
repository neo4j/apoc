package apoc.util.collection;

import org.neo4j.graphdb.ResourceIterator;

public abstract class PrefetchingResourceIterator<T> extends PrefetchingIterator<T> implements ResourceIterator<T> {}
