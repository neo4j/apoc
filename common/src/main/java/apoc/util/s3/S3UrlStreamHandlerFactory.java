package apoc.util.s3;

import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

@SuppressWarnings("unused") // used from SupportedProtocols
public class S3UrlStreamHandlerFactory implements URLStreamHandlerFactory {
    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        return new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL url) {
                return new S3URLConnection(url);
            }
        };
    }
}
