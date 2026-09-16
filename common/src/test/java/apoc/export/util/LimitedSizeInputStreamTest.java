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

import static apoc.export.util.LimitedSizeInputStream.MAX_ABSOLUTE_SIZE;
import static apoc.export.util.LimitedSizeInputStream.SIZE_MULTIPLIER;
import static apoc.export.util.LimitedSizeInputStream.toDynamicLimitedIStream;
import static apoc.export.util.LimitedSizeInputStream.toLimitedIStream;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;

class LimitedSizeInputStreamTest {

    @BeforeAll
    static void setup() {
        // ensure an ApocConfig instance exists so LimitedSizeInputStream's static config lookups
        // (SIZE_MULTIPLIER, MAX_ABSOLUTE_SIZE) don't NPE if this is the first test to touch the class
        new apoc.ApocConfig(Mockito.mock(Config.class));
    }

    private static InputStream infiniteZeroStream() {
        return new InputStream() {
            @Override
            public int read() {
                return 0;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                Arrays.fill(b, off, off + len, (byte) 0);
                return len;
            }
        };
    }

    @Test
    void unknownLengthIsBoundedRatherThanUnlimited() throws IOException {
        // a negative ratio is a deliberate admin opt-out, not the "unknown length" case under test
        assumeTrue(SIZE_MULTIPLIER >= 0, "ratio protection disabled in this JVM run");

        try (InputStream limited = toLimitedIStream(infiniteZeroStream(), -1)) {
            byte[] buf = new byte[8192];
            long ceiling = MAX_ABSOLUTE_SIZE + buf.length;

            IOException e = assertThrows(IOException.class, () -> {
                long total = 0;
                while (total <= ceiling) {
                    int n = limited.read(buf);
                    if (n < 0) break;
                    total += n;
                }
            });
            assertTrue(e.getMessage().contains("compression bomb"));
        }
    }

    @Test
    void dynamicBoundTracksActualConsumedSourceBytesNotADeclaredLength() throws IOException {
        assumeTrue(SIZE_MULTIPLIER > 0, "ratio protection disabled/unbounded in this JVM run");

        AtomicLong consumedSourceBytes = new AtomicLong(0);

        // simulates a "decompressor" that barely touches its source (as tracked by
        // consumedSourceBytes) while producing a huge amount of decompressed output per call -
        // i.e. a massive amplification ratio, regardless of what any declared length claims.
        InputStream bombOutput = new InputStream() {
            @Override
            public int read() {
                consumedSourceBytes.incrementAndGet();
                return 0;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                consumedSourceBytes.incrementAndGet();
                Arrays.fill(b, off, off + len, (byte) 0);
                return len;
            }
        };

        try (InputStream limited = toDynamicLimitedIStream(bombOutput, consumedSourceBytes::get)) {
            byte[] buf = new byte[1024 * 1024];

            IOException e = assertThrows(IOException.class, () -> {
                for (int i = 0; i < 10_000; i++) {
                    limited.read(buf);
                }
            });
            assertTrue(e.getMessage().contains("compression bomb"));
        }

        // it must have been stopped after consuming only a handful of "source" bytes - proving the
        // budget tracks real consumption instead of a declared length, which would otherwise have
        // let all 10_000 MB of bogus output through unchecked.
        assertTrue(consumedSourceBytes.get() < 10);
    }
}
