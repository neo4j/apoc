package apoc.util.google.cloud;

import apoc.util.SupportedProtocols;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

@SuppressWarnings("unused") // used from SupportedProtocols
public class GCStorageURLStreamHandlerFactory implements URLStreamHandlerFactory {

    public GCStorageURLStreamHandlerFactory() {}

    @Override
    public URLStreamHandler createURLStreamHandler(final String protocol) {
        final SupportedProtocols supportedProtocols = SupportedProtocols.valueOf(protocol);
        return supportedProtocols == SupportedProtocols.gs ? new GCStorageURLStreamHandler() : null;
    }
}
