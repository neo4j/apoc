package apoc.lock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.List;

public class Lock {

    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    @Procedure(name = "apoc.lock.all", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given nodes and relationships.")
    public void all(@Name("nodes") List<Node> nodes, @Name("rels") List<Relationship> rels) {
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @Procedure(name = "apoc.lock.nodes", mode = Mode.WRITE)
    @Description("Acquires a write lock on the given nodes.")
    public void nodes(@Name("nodes") List<Node> nodes) {
        for (Node node : nodes) {
            tx.acquireWriteLock(node);
        }
    }

    @Procedure(name = "apoc.lock.read.nodes", mode = Mode.READ)
    @Description("Acquires a read lock on the given nodes.")
    public void readLockOnNodes(@Name("nodes") List<Node> nodes) {
        for (Node node : nodes) {
            tx.acquireReadLock(node);
        }
    }

    @Procedure(name = "apoc.lock.read.rels", mode = Mode.WRITE)
    @Description("Acquires a read lock on the given relationships.")
    public void rels(@Name("rels") List<Relationship> rels) {
        for (Relationship rel : rels) {
            tx.acquireWriteLock(rel);
        }
    }

    @Procedure(name = "apoc.lock.read.rels", mode = Mode.READ)
    @Description("Acquires a write lock on the given relationships.")
    public void readLocksOnRels(@Name("rels") List<Relationship> rels) {
        for (Relationship rel : rels) {
            tx.acquireReadLock(rel);
        }
    }
}
