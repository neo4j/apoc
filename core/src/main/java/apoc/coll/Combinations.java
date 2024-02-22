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
package apoc.coll;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

/**
 * This code was copied from project commons-math3
 */
class Combinations implements Iterable<int[]> {
    private final int n;
    private final int k;

    public Combinations(int n, int k) {
        checkBinomial(n, k);
        this.n = n;
        this.k = k;
    }

    @Override
    public Iterator<int[]> iterator() {
        if (k == 0 || k == n) {
            return new SingletonIterator(IntStream.range(0, k).toArray());
        }
        return new LexicographicIterator(n, k);
    }

    private static class LexicographicIterator implements Iterator<int[]> {
        private final int k;
        private final int[] c;
        private boolean more = true;
        private int j;

        LexicographicIterator(int n, int k) {
            this.k = k;
            c = new int[k + 3];
            if (k == 0 || k >= n) {
                more = false;
                return;
            }
            // Initialize c to start with lexicographically first k-set
            for (int i = 1; i <= k; i++) {
                c[i] = i - 1;
            }
            // Initialize sentinels
            c[k + 1] = n;
            c[k + 2] = 0;
            j = k; // Set up invariant: j is smallest index such that c[j + 1] > j
        }

        @Override
        public boolean hasNext() {
            return more;
        }

        @Override
        public int[] next() {
            if (!more) {
                throw new NoSuchElementException();
            }
            // Copy return value (prepared by last activation)
            final int[] ret = new int[k];
            System.arraycopy(c, 1, ret, 0, k);

            // Prepare next iteration
            // T2 and T6 loop
            int x = 0;
            if (j > 0) {
                x = j;
                c[j] = x;
                j--;
                return ret;
            }
            // T3
            if (c[1] + 1 < c[2]) {
                c[1]++;
                return ret;
            } else {
                j = 2;
            }
            // T4
            boolean stepDone = false;
            while (!stepDone) {
                c[j - 1] = j - 2;
                x = c[j] + 1;
                if (x == c[j + 1]) {
                    j++;
                } else {
                    stepDone = true;
                }
            }
            // T5
            if (j > k) {
                more = false;
                return ret;
            }
            // T6
            c[j] = x;
            j--;
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class SingletonIterator implements Iterator<int[]> {
        private final int[] singleton;
        private boolean more = true;

        SingletonIterator(final int[] singleton) {
            this.singleton = singleton;
        }

        @Override
        public boolean hasNext() {
            return more;
        }

        @Override
        public int[] next() {
            if (more) {
                more = false;
                return singleton;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static void checkBinomial(final int n, final int k) {
        if (n < k) {
            throw new IllegalArgumentException(
                    "must have n >= k for binomial coefficient (n, k), got k = %d, n = %d".formatted(k, n));
        }
        if (n < 0) {
            throw new IllegalArgumentException("must have n >= 0 for binomial coefficient (n, k), got n = " + n);
        }
    }
}
