package apoc.export.util;

import java.io.IOException;
import java.io.InputStream;

public class LimitedSizeInputStream extends InputStream {
    public static final String SIZE_EXCEEDED_ERROR = """
            The file dimension exceeded maximum size in bytes, %s,
            which is %s times the width of the original file.
            The InputStream has been blocked because the file could be a compression bomb attack.""";

    public static final int SIZE_MULTIPLIER = 100;

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
        if (i >= 0) incrementCounter(1);
        return i;
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
            close();
            String msgError = String.format(SIZE_EXCEEDED_ERROR,
                    maxSize, SIZE_MULTIPLIER);
            throw new IOException(msgError);
        }
    }

    public static InputStream toLimitedIStream(InputStream stream, long total) {
        // to prevent potential bomb attack
        return new LimitedSizeInputStream(stream, total * SIZE_MULTIPLIER);
    }

}
