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

import apoc.result.ProgressInfo;
import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ProgressReporter implements Reporter {
    private final SizeCounter sizeCounter;
    private final PrintWriter out;
    private final long batchSize;
    long time;
    int counter;
    long totalEntities = 0;
    long lastBatch = 0;
    long start = System.currentTimeMillis();
    private final ProgressInfo progressInfo;
    private Consumer<ProgressInfo> consumer;

    public ProgressReporter(SizeCounter sizeCounter, PrintWriter out, ProgressInfo progressInfo) {
        this.sizeCounter = sizeCounter;
        this.out = out;
        this.time = start;
        this.progressInfo = progressInfo;
        this.batchSize = progressInfo.getBatchSize();
    }

    public ProgressReporter withConsumer(Consumer<ProgressInfo> consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public void progress(String msg) {
        long now = System.currentTimeMillis();
        // todo report percentages back
        println(String.format(
                msg + " %d. %d%%: %s time %d ms total %d ms",
                counter++,
                percent(),
                progressInfo,
                now - time,
                now - start));
        time = now;
    }

    private void println(String message) {
        if (out != null) out.println(message);
    }

    private long percent() {
        return sizeCounter == null ? 100 : sizeCounter.getPercent();
    }

    public void update(long nodes, long relationships, long properties) {
        time = System.currentTimeMillis();
        progressInfo.update(nodes, relationships, properties);
        totalEntities += nodes + relationships;
        acceptBatch();
    }

    public void acceptBatch() {
        if (batchSize != -1 && totalEntities / batchSize > lastBatch) {
            updateRunningBatch(progressInfo);
            if (consumer != null) {
                consumer.accept(progressInfo);
            }
        }
    }

    public void updateRunningBatch(ProgressInfo progressInfo) {
        lastBatch = Math.max(totalEntities / batchSize, lastBatch);
        progressInfo.setBatches(lastBatch);
        this.progressInfo.setRows(totalEntities);
        this.progressInfo.updateTime(start);
    }

    @Override
    public void done() {
        if (totalEntities % batchSize != 0) lastBatch++;
        updateRunningBatch(progressInfo);
        progressInfo.done(start);
        if (consumer != null) {
            consumer.accept(progressInfo);
        }
        if (consumer != null) {
            consumer.accept(ProgressInfo.EMPTY);
        }
    }

    public ProgressInfo getTotal() {
        progressInfo.done(start);
        return progressInfo;
    }

    public Stream<ProgressInfo> stream() {
        return Stream.of(getTotal());
    }

    public void nextRow() {
        this.progressInfo.nextRow();
        this.totalEntities++;
        acceptBatch();
    }
}
