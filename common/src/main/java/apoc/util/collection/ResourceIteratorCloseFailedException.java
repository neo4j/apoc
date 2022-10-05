package apoc.util.collection;

import org.neo4j.graphdb.Resource;

/**
 * This exception is thrown when a checked exception occurs inside {@link Resource#close()}.
 * It is a RuntimeException since {@link Resource#close()} is not allowed to throw checked exceptions.
 */
public class ResourceIteratorCloseFailedException extends RuntimeException {
    public ResourceIteratorCloseFailedException(String message) {
        super(message);
    }

    public ResourceIteratorCloseFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
