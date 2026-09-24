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
package apoc.util;

import org.neo4j.procedure.memory.ProcedureMemory;
import org.neo4j.procedure.memory.ProcedureMemoryTracker;

/**
 * Helpers for charging allocations made inside a procedure or function body against the transaction
 * memory budget, so that oversized results are refused by {@code db.memory.transaction.*} instead of
 * silently exhausting the heap.
 */
public final class ProcedureMemoryUtil {

    /** A String's fixed cost: object header, coder and hash fields, and the header of its backing byte array. */
    private static final long STRING_OVERHEAD_BYTES = 56L;

    private ProcedureMemoryUtil() {}

    /**
     * Upper bound on the heap a String of the given length occupies (2 bytes per char, before compaction).
     * A length so large that its byte size is not representable saturates to {@link Long#MAX_VALUE}, which the
     * tracker then refuses with a message stating the size - more use to a caller than an arithmetic overflow.
     */
    public static long sizeOfString(long chars) {
        if (chars > (Long.MAX_VALUE - STRING_OVERHEAD_BYTES) / 2L) {
            return Long.MAX_VALUE;
        }
        return 2L * chars + STRING_OVERHEAD_BYTES;
    }

    /**
     * Charges {@code bytes} to a new transaction-scoped tracker and hands the tracker back to the caller.
     * Throws {@code MemoryLimitExceededException} before the caller allocates anything.
     *
     * <p>The tracker is closed if the charge is refused; without that, a rejected charge would leak the tracker
     * for the rest of the transaction.
     */
    public static ProcedureMemoryTracker charge(ProcedureMemory memory, long bytes) {
        ProcedureMemoryTracker tracker = memory.newTracker();
        try {
            tracker.allocateHeap(bytes);
        } catch (RuntimeException e) {
            tracker.close();
            throw e;
        }
        return tracker;
    }
}
