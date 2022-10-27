package apoc.util;

public enum SupportedProtocols
{
    http(true, null),
    https(true, null),
    ftp(true, null),
    s3(Util.classExists("com.amazonaws.services.s3.AmazonS3"),
       "apoc.util.s3.S3UrlStreamHandlerFactory"),
    gs(Util.classExists("com.google.cloud.storage.Storage"),
       "apoc.util.google.cloud.GCStorageURLStreamHandlerFactory"),
    hdfs(Util.classExists("org.apache.hadoop.fs.FileSystem"),
         "org.apache.hadoop.fs.FsUrlStreamHandlerFactory"),
    file(true, null);

    private final boolean enabled;

    private final String urlStreamHandlerClassName;

    SupportedProtocols(boolean enabled, String urlStreamHandlerClassName) {
        this.enabled = enabled;
        this.urlStreamHandlerClassName = urlStreamHandlerClassName;
        }

    public boolean isEnabled() {
        return enabled;
    }

    String getUrlStreamHandlerClassName() {
        return urlStreamHandlerClassName;
    }
}
