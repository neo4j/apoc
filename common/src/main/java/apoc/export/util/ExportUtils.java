/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.util;

import apoc.export.cypher.ExportFileManager;
import apoc.result.ExportProgressInfo;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.TerminationGuard;

public class ExportUtils {
    private static final int QUEUE_CAPACITY = 1000;

    private ExportUtils() {}

    public static Stream<ExportProgressInfo> getProgressInfoStream(
            GraphDatabaseService db,
            ExecutorService executorService,
            TerminationGuard terminationGuard,
            String format,
            ExportConfig exportConfig,
            ProgressReporter reporter,
            ExportFileManager cypherFileManager,
            BiConsumer<Transaction, ProgressReporter> dump) {
        long timeout = exportConfig.getTimeoutSeconds();
        return streamProgressInfo(
                db,
                executorService,
                terminationGuard,
                reporter,
                dump,
                ExportProgressInfo.EMPTY,
                pi -> new ExportProgressInfo((ExportProgressInfo) pi)
                        .drain(cypherFileManager.getStringWriter(format), exportConfig),
                timeout,
                (int) timeout);
    }

    /**
     * Runs {@code dump} on {@code executorService} and hands its progress reports back to the calling thread through a
     * bounded queue, so a streaming export can emit rows while the dump is still running.
     *
     * @param tombstone        the marker that ends the stream; also what a terminal report from the reporter is
     *                         translated into.
     * @param enrich           converts a progress report into the type to emit, e.g. by draining the writer.
     * @param timeoutSeconds   how long to wait when queueing a report, and for the dump to finish once the tombstone
     *                         has been seen.
     * @param pollTimeoutSeconds how long the consumer waits for the next report before giving up.
     */
    public static <T extends ProgressInfo> Stream<T> streamProgressInfo(
            GraphDatabaseService db,
            ExecutorService executorService,
            TerminationGuard terminationGuard,
            ProgressReporter reporter,
            BiConsumer<Transaction, ProgressReporter> dump,
            T tombstone,
            Function<ProgressInfo, T> enrich,
            long timeoutSeconds,
            int pollTimeoutSeconds) {
        final ArrayBlockingQueue<T> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        // ProgressReporter.done() signals the end of the dump with ProgressInfo.EMPTY, which is null; anything else is
        // a progress update to forward.
        ProgressReporter reporterWithConsumer = reporter.withConsumer((pi) ->
                QueueUtil.put(queue, pi == null || pi == tombstone ? tombstone : enrich.apply(pi), timeoutSeconds));
        Future<Boolean> dumpFuture = Util.inTxFuture(
                null,
                executorService,
                db,
                threadBoundTx -> {
                    dump.accept(threadBoundTx, reporterWithConsumer);
                    return true;
                },
                0,
                _ignored -> {},
                _ignored -> QueueUtil.put(queue, tombstone, timeoutSeconds));
        QueueBasedSpliterator<T> spliterator = new QueueBasedSpliterator<>(
                queue,
                tombstone,
                terminationGuard,
                pollTimeoutSeconds,
                () -> rethrowDumpFailure(dumpFuture, timeoutSeconds));
        return StreamSupport.stream(spliterator, false);
    }

    /**
     * The dump enqueues the tombstone from a {@code finally} block, so the stream terminates cleanly whether the dump
     * succeeded or threw. Without this check a failed export is indistinguishable from an export of an empty graph.
     */
    private static void rethrowDumpFailure(Future<Boolean> dumpFuture, long timeoutSeconds) {
        try {
            dumpFuture.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw cause instanceof RuntimeException re ? re : new RuntimeException("Error exporting data", cause);
        } catch (TimeoutException e) {
            throw new RuntimeException(
                    "Error exporting data, the export did not complete within " + timeoutSeconds + " seconds", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for the export to complete", e);
        }
    }
}
