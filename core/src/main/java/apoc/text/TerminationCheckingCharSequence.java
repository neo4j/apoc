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

import org.neo4j.procedure.TerminationGuard;

/**
 * Wraps the input of a regular expression match so that the transaction's termination guard is checked periodically
 * while the match runs.
 * <p>
 * {@link java.util.regex.Matcher} never checks for interruption and reads the input through {@link #charAt(int)} on
 * every backtracking step, so a pathological pattern otherwise pins a thread until it completes. Checking the guard
 * here makes {@code dbms.killQuery} and the transaction timeout effective for any match, however long it runs.
 */
final class TerminationCheckingCharSequence implements CharSequence {

    private static final int CHECK_INTERVAL = 10_000;

    private final CharSequence delegate;
    private final TerminationGuard guard;
    private int countdown = CHECK_INTERVAL;

    TerminationCheckingCharSequence(CharSequence delegate, TerminationGuard guard) {
        this.delegate = delegate;
        this.guard = guard;
    }

    @Override
    public char charAt(int index) {
        if (--countdown <= 0) {
            countdown = CHECK_INTERVAL;
            guard.check();
        }
        return delegate.charAt(index);
    }

    @Override
    public int length() {
        return delegate.length();
    }

    /**
     * Returns the <i>unwrapped</i> subsequence of the delegate, so that {@code Matcher.group()} and
     * {@code Matcher.replaceAll()} produce plain `STRING` values and this wrapper never escapes into a result.
     */
    @Override
    public CharSequence subSequence(int start, int end) {
        return delegate.subSequence(start, end);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
