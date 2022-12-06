package apoc.export.util;

import org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.commons.io.input.BOMInputStream;

import java.io.*;
import java.nio.channels.SeekableByteChannel;

/**
 * @author mh
 * @since 22.05.16
 */
public class CountingInputStream extends FilterInputStream implements SizeCounter {
    public static final int BUFFER_SIZE = 1024 * 1024;
    private final long total;
    private long count=0;

    public CountingInputStream(InputStream stream, long total) {
        super(toBufferedStream(stream));
        this.total = total;
    }

    private static BufferedInputStream toBufferedStream(InputStream stream) {
        final BOMInputStream bomInputStream = new BOMInputStream(stream);
        return new BufferedInputStream(bomInputStream, BUFFER_SIZE);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        int read = super.read(buf, off, len);
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

	public CountingReader asReader() throws IOException {
		Reader reader = new InputStreamReader(in,"UTF-8");
        return new CountingReader(reader,total);
	}

    public SeekableByteChannel asChannel() throws IOException {
        return new SeekableInMemoryByteChannel(this.readAllBytes());
    }
}
