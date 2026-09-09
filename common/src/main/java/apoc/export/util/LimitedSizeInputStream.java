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

import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_RATIO;
import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_SIZE;
import static apoc.ApocConfig.DEFAULT_MAX_DECOMPRESSION_RATIO;
import static apoc.ApocConfig.DEFAULT_MAX_DECOMPRESSION_SIZE;
import static apoc.ApocConfig.apocConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.LongSupplier;

public class LimitedSizeInputStream extends InputStream {
    public static final String SIZE_EXCEEDED_ERROR =
            """
            The file dimension exceeded maximum size in bytes, %s,
            which is %s times the width of the original file.
            The InputStream has been blocked because the file could be a compression bomb attack.""";

    public static final int SIZE_MULTIPLIER =
            apocConfig().getInt(APOC_MAX_DECOMPRESSION_RATIO, DEFAULT_MAX_DECOMPRESSION_RATIO);

    // Absolute ceiling used whenever the source size cannot be trusted (unknown, or attacker
    // declared) as the basis for a ratio-based budget.
    public static final long MAX_ABSOLUTE_SIZE =
            apocConfig().getLong(APOC_MAX_DECOMPRESSION_SIZE, DEFAULT_MAX_DECOMPRESSION_SIZE);

    private final InputStream stream;
    private final LongSupplier maxSizeSupplier;
    private long total;

    private LimitedSizeInputStream(InputStream stream, LongSupplier maxSizeSupplier) {
        this.stream = stream;
        this.maxSizeSupplier = maxSizeSupplier;
    }

    @Override
    public int read() throws IOException {
        int i = stream.read();
        if (i >= 0) incrementCounter(1);
        return i;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int i = stream.read(b, off, len);
        if (i >= 0) incrementCounter(i);
        return i;
    }

    @Override
    public void close() throws IOException {
        stream.close();
        super.close();
    }

    private void incrementCounter(int size) throws IOException {
        total += size;
        long maxSize = maxSizeSupplier.getAsLong();
        if (total > maxSize) {
            close();
            String msgError = String.format(SIZE_EXCEEDED_ERROR, maxSize, SIZE_MULTIPLIER);
            throw new IOException(msgError);
        }
    }

    /**
     * A negative ratio is a deliberate, admin-configured opt-out of the decompression-bomb
     * protection, distinct from a source size that is merely unknown or untrusted.
     */
    private static boolean isRatioProtectionDisabled() {
        return SIZE_MULTIPLIER < 0;
    }

    private static long ratioBound(long size) {
        if (isRatioProtectionDisabled()) {
            return Long.MAX_VALUE;
        }
        // size < 0 means the source length is unknown (e.g. chunked transfer encoding) - fall back
        // to the absolute ceiling instead of disabling enforcement.
        if (size < 0) {
            return MAX_ABSOLUTE_SIZE;
        }
        if (SIZE_MULTIPLIER != 0 && size > Long.MAX_VALUE / SIZE_MULTIPLIER) {
            // avoid overflow for pathologically large (e.g. attacker-declared) sizes
            return MAX_ABSOLUTE_SIZE;
        }
        return Math.min(size * SIZE_MULTIPLIER, MAX_ABSOLUTE_SIZE);
    }

    /**
     * Bounds a stream using an already-known, trustworthy source size (e.g. a local file's length,
     * or an in-memory byte array's length). The resulting budget is still capped by the configured
     * absolute size, so a bogus/huge declared size can't be used to inflate it unboundedly.
     */
    public static InputStream toLimitedIStream(InputStream stream, long total) {
        long maxSize = ratioBound(total);
        return new LimitedSizeInputStream(stream, () -> maxSize);
    }

    /**
     * Bounds a stream whose true size cannot be trusted upfront - typically decompressed output read
     * over HTTP(S)/FTP, where the remote server's declared Content-Length may be absent, chunked, or
     * simply forged. The budget is (re)computed on every read from the number of bytes actually
     * consumed so far from the underlying compressed/source stream, so a missing or forged declared
     * length can no longer be used to disable or inflate the decompression-ratio protection.
     */
    public static InputStream toDynamicLimitedIStream(InputStream stream, LongSupplier consumedSourceBytes) {
        return new LimitedSizeInputStream(stream, () -> ratioBound(consumedSourceBytes.getAsLong()));
    }
}
