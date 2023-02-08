package apoc.util.kernel;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.BatchJobResult;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.forAllNodes;
import static org.junit.Assert.assertEquals;

public class MultiThreadedGlobalGraphOperationsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        createData();
    }

    @AfterClass
    public static void teardown() {
       db.shutdown();
    }

    private static void createData() {
        db.executeTransactionally("UNWIND range(1,1000) as x MERGE (s{id:x}) MERGE (e{id:x+1}) merge (s)-[:REL{id:x}]->(e)");
    }

    @Test
    public void shouldforAllNodesWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllNodes(db, Executors.newFixedThreadPool(4), 10,
                (nodeCursor) -> counter.incrementAndGet() );
        assertEquals(1001, counter.get());
        assertEquals(1001, result.getSucceeded());
        assertEquals(0, result.getFailures());
    }
}
