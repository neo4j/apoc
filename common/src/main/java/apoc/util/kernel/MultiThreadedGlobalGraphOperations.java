package apoc.util.kernel;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class MultiThreadedGlobalGraphOperations {

    public static BatchJobResult forAllNodes(GraphDatabaseAPI db, ExecutorService executorService, int batchSize, Consumer<NodeCursor> consumer) {
        BatchJobResult result = new BatchJobResult();
        AtomicInteger processing = new AtomicInteger();
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) ) {
            KernelTransaction ktx = tx.kernelTransaction();
            Function<Read, Scan<NodeCursor>> scanFunction = Read::allNodesScan;
            Scan<NodeCursor> scan = scanFunction.apply( ktx.dataRead() );
            Function<KernelTransaction,NodeCursor> cursorAllocator = ktx2 -> ktx2.cursors().allocateNodeCursor( ktx2.cursorContext() );
            executorService.submit( new BatchJob( scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing ) );
        }

        try {
            while ( processing.get() > 0 ) {
                Thread.sleep( 10 );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    public static class BatchJobResult {
        final AtomicInteger batches = new AtomicInteger();
        final AtomicLong succeeded = new AtomicLong();
        final AtomicLong failures = new AtomicLong();

        public void incrementSuceeded() {
            succeeded.incrementAndGet();
        }

        public void incrementFailures() {
            failures.incrementAndGet();
        }

        public long getSucceeded() {
            return succeeded.get();
        }

        public long getFailures() {
            return failures.get();
        }
    }

    private static class BatchJob implements Callable<Void> {
        private final Scan<NodeCursor> scan;
        private final int batchSize;
        private final GraphDatabaseAPI db;
        private final Consumer<NodeCursor> consumer;
        private final BatchJobResult result;
        private final Function<KernelTransaction,NodeCursor> cursorAllocator;
        private final ExecutorService executorService;
        private final AtomicInteger processing;

        public BatchJob(Scan<NodeCursor> scan, int batchSize, GraphDatabaseAPI db, Consumer<NodeCursor> consumer,
                BatchJobResult result, Function<KernelTransaction,NodeCursor> cursorAllocator, ExecutorService executorService, AtomicInteger processing ) {
            this.scan = scan;
            this.batchSize = batchSize;
            this.db = db;
            this.consumer = consumer;
            this.result = result;
            this.cursorAllocator = cursorAllocator;
            this.executorService = executorService;
            this.processing = processing;
            processing.incrementAndGet();
        }

        @Override
        public Void call() {
            try (InternalTransaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                KernelTransaction ktx = tx.kernelTransaction();
                try ( NodeCursor cursor = cursorAllocator.apply( ktx )) {
                    if (scan.reserveBatch( cursor, batchSize, ktx.cursorContext(), AccessMode.Static.FULL )) {
                        // Branch out so that all available threads will get saturated
                        executorService.submit( new BatchJob( scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing ) );
                        executorService.submit( new BatchJob( scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing ) );
                        while (processAndReport(cursor)) {
                            // just continue processing...
                        }
                    }
                }
                tx.commit();
                return null;
            } finally {
                result.batches.incrementAndGet();
                processing.decrementAndGet();
            }
        }

        private boolean processAndReport(NodeCursor cursor) {
            if (cursor.next()) {
                try {
                    consumer.accept(cursor);
                    result.incrementSuceeded();
                } catch (Exception e) {
                    result.incrementFailures();
                }
                return true;
            }
            return false;
        }
    }
}
