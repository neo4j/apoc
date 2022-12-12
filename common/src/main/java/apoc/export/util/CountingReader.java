package apoc.export.util;

import java.io.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class CountingReader extends FilterReader implements SizeCounter {
    public static final int BUFFER_SIZE = 1024 * 1024;
    private final long total;
    private long count=0;

    public CountingReader(Reader reader, long total) {
        super(new BufferedReader(reader, BUFFER_SIZE));
        this.total = total;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int read = super.read(cbuf, off, len);
        count+=read;
        return read;
    }

    @Override
    public int read() throws IOException {
        count++;
        return super.read();
    }

    @Override
    public long skip(long n) throws IOException {
        count += n;
        return super.skip(n);
    }

    @Override
    public long getPercent() {
        if (total <= 0) return 0;
        return count*100 / total;
    }
}
