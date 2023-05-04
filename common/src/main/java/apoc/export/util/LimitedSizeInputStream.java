package apoc.export.util;

import java.io.IOException;
import java.io.InputStream;

public class LimitedSizeInputStream extends InputStream {

    private final InputStream stream;
    private final long maxSize;
    private long total;

    public LimitedSizeInputStream(InputStream stream, long maxSize) {
        this.stream = stream;
        this.maxSize = maxSize;
    }

    @Override
    public int read() throws IOException {
        int i = stream.read();
        if (i>=0) incrementCounter(1);
        return i;
    }

    @Override
    public int read(byte b[]) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int i = stream.read(b, off, len);
        if (i >= 0) incrementCounter(i);
        return i;
    }

    private void incrementCounter(int size) throws IOException {
        total += size;
        if (total > maxSize) {
            throw new IOException("InputStream exceeded maximum size in bytes.");
        }
    }

    public static InputStream toLimitedIStream(InputStream stream, long total) {
        // to prevent potential bomb attack
        return new LimitedSizeInputStream(stream, total * 100);
    }

}
