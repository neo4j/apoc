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
package apoc.text;

import org.neo4j.procedure.memory.ProcedureMemoryTracker;

/**
 * An {@link Appendable} that charges the heap its buffer occupies to a {@link ProcedureMemoryTracker}, for callers
 * whose output size cannot be estimated before the work is done.
 *
 * <p>The charge happens when the buffer's capacity grows, not on every append: {@code Formatter} emits width padding
 * one character at a time, so charging per append would dominate the cost of formatting.
 */
final class TrackedAppendable implements Appendable {

    private final StringBuilder sb = new StringBuilder();
    private final ProcedureMemoryTracker tracker;
    private long charged;

    TrackedAppendable(ProcedureMemoryTracker tracker) {
        this.tracker = tracker;
        chargeGrowth();
    }

    @Override
    public Appendable append(CharSequence csq) {
        sb.append(csq);
        chargeGrowth();
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) {
        sb.append(csq, start, end);
        chargeGrowth();
        return this;
    }

    @Override
    public Appendable append(char c) {
        sb.append(c);
        chargeGrowth();
        return this;
    }

    @Override
    public String toString() {
        return sb.toString();
    }

    private void chargeGrowth() {
        long needed = 2L * sb.capacity();
        if (needed > charged) {
            tracker.allocateHeap(needed - charged);
            charged = needed;
        }
    }
}
