package apoc.util;

import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;



public class ApocUrlStreamHandlerFactory extends URLStreamHandlerProvider
{

    @Override
    public URLStreamHandler createURLStreamHandler( String protocol ) {
        SupportedProtocols supportedProtocol = FileUtils.of( protocol );
        return supportedProtocol == null ? null : FileUtils.createURLStreamHandler( supportedProtocol );
    }
}
