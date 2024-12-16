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
package apoc.util;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.util.LimitedSizeInputStream.toLimitedIStream;
import static apoc.util.Util.ERROR_BYTES_OR_STRING;
import static apoc.util.Util.REDIRECT_LIMIT;
import static apoc.util.Util.isRedirect;

import apoc.ApocConfig;
import apoc.export.util.CountingInputStream;
import apoc.export.util.CountingReader;
import apoc.export.util.ExportConfig;
import apoc.util.hdfs.HDFSUtils;
import apoc.util.s3.S3URLConnection;
import apoc.util.s3.S3UploadUtils;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public static String getLoadFileUrl(String fileName, URLAccessChecker urlAccessChecker)
            throws MalformedURLException, URLAccessValidationError {
        URL url;
        try {
            url = URI.create(fileName).toURL();
        } catch (MalformedURLException | IllegalArgumentException e) {
            String withFile = "file:///" + fileName;
            url = URI.create(withFile).toURL();
        }
        return urlAccessChecker.checkURL(url).getFile();
    }

    public static String getFileUrl(String fileName) throws MalformedURLException {
        try {
            return new URL(fileName).getFile();
        } catch (MalformedURLException e) {
            if (e.getMessage().contains("no protocol")) {
                return fileName;
            }
            throw e;
        }
    }

    public static StreamConnection getStreamConnection(
            SupportedProtocols protocol,
            String urlAddress,
            Map<String, Object> headers,
            String payload,
            URLAccessChecker urlAccessChecker)
            throws IOException, URLAccessValidationError, URISyntaxException {
        switch (protocol) {
            case s3:
                return FileUtils.openS3InputStream(urlAddress);
            case hdfs:
                return FileUtils.openHdfsInputStream(urlAddress);
            case ftp:
            case http:
            case https:
            case gs:
                return readHttpInputStream(urlAddress, headers, payload, REDIRECT_LIMIT, urlAccessChecker);
            default:
                try {
                    URL url = urlAccessChecker.checkURL(URI.create(urlAddress).toURL());
                    return new StreamConnection.FileStreamConnection(url.toURI());
                } catch (IllegalArgumentException iae) {
                    return new StreamConnection.FileStreamConnection(getLoadFileUrl(urlAddress, urlAccessChecker));
                }
        }
    }

    public static URLStreamHandler createURLStreamHandler(SupportedProtocols protocol) {
        URLStreamHandler handler = Optional.ofNullable(protocol.getUrlStreamHandlerClassName())
                .map(Util::createInstanceOrNull)
                .map(urlStreamHandlerFactory ->
                        ((URLStreamHandlerFactory) urlStreamHandlerFactory).createURLStreamHandler(protocol.name()))
                .orElse(null);
        return handler;
    }

    public static SupportedProtocols of(String name) {
        try {
            return SupportedProtocols.valueOf(name);
        } catch (Exception e) {
            return SupportedProtocols.file;
        }
    }

    public static SupportedProtocols from(URL url) {
        return of(url.getProtocol());
    }

    public static SupportedProtocols from(String source) {
        try {
            final URL url = new URL(source);
            return from(url);
        } catch (MalformedURLException e) {
            if (!e.getMessage().contains("no protocol")) {
                try {
                    // in case new URL(source) throw e.g. unknown protocol: hdfs, because of missing jar,
                    // we retrieve the related enum and throw the associated MissingDependencyException(..)
                    // otherwise we return unknown protocol: yyyyy
                    return SupportedProtocols.valueOf(new URI(source).getScheme());
                } catch (Exception ignored) {
                }

                // in case a Windows user write an url like `C:/User/...`
                if (e.getMessage().contains("unknown protocol") && Util.isWindows()) {
                    throw new RuntimeException(e.getMessage()
                            + "\n Please note that for Windows absolute paths they have to be explicit by prepending `file:` or supplied without the drive, "
                            + "\n e.g. `file:C:/my/path/file` or `/my/path/file`, instead of `C:/my/path/file`");
                }
                throw new RuntimeException(e);
            }
            return SupportedProtocols.file;
        }
    }

    public static final String ERROR_READ_FROM_FS_NOT_ALLOWED = "Import file %s not enabled, please set "
            + APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM + "=true in your neo4j.conf";
    public static final String ACCESS_OUTSIDE_DIR_ERROR =
            "You're providing a directory outside the import directory " + "defined into `server.directories.import`";

    public static CountingReader readerFor(Object input, String compressionAlgo, URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return readerFor(input, null, null, compressionAlgo, urlAccessChecker);
    }

    public static CountingReader readerFor(
            Object input,
            Map<String, Object> headers,
            String payload,
            String compressionAlgo,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return inputStreamFor(input, headers, payload, compressionAlgo, urlAccessChecker)
                .asReader();
    }

    public static CountingInputStream inputStreamFor(
            Object input,
            Map<String, Object> headers,
            String payload,
            String compressionAlgo,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        if (input == null) return null;
        if (input instanceof String) {
            String fileName = (String) input;
            fileName = changeFileUrlIfImportDirectoryConstrained(fileName, urlAccessChecker);
            return FileUtils.openInputStream(fileName, headers, payload, compressionAlgo, urlAccessChecker);
        } else if (input instanceof byte[]) {
            return getInputStreamFromBinary((byte[]) input, compressionAlgo);
        } else {
            throw new RuntimeException(ERROR_BYTES_OR_STRING);
        }
    }

    public static String changeFileUrlIfImportDirectoryConstrained(String url, URLAccessChecker urlAccessChecker)
            throws IOException, URLAccessValidationError {
        apocConfig().checkReadAllowed(url, urlAccessChecker);
        if (isFile(url) && isImportUsingNeo4jConfig()) {
            if (!apocConfig().getBoolean(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM)) {
                throw new RuntimeException(String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, url));
            }
            getLoadFileUrl(url, urlAccessChecker);
            final Path resolvedPath = resolvePath(url);
            return resolvedPath.normalize().toUri().toString();
        }
        return url;
    }

    private static Path resolvePath(String url) throws IOException {
        Path urlPath = getPath(url);
        final Path resolvedPath;
        if (apocConfig().isImportFolderConfigured() && isImportUsingNeo4jConfig()) {
            Path basePath = Paths.get(apocConfig().getImportDir());
            urlPath = relativizeIfSamePrefix(urlPath, basePath);
            resolvedPath = basePath.resolve(urlPath).toAbsolutePath().normalize();
            if (!pathStartsWithOther(resolvedPath, basePath)) {
                throw new IOException(ACCESS_OUTSIDE_DIR_ERROR);
            }
        } else {
            resolvedPath = urlPath;
        }
        return resolvedPath;
    }

    private static Path relativizeIfSamePrefix(Path urlPath, Path basePath) {
        if (FilenameUtils.getPrefixLength(urlPath.toString()) > 0 && !urlPath.startsWith(basePath.toAbsolutePath())) {
            // if the import folder is configured to be used as root folder we consider
            // it as root directory in order to reproduce the same LOAD CSV behaviour
            urlPath = urlPath.getRoot().relativize(urlPath);
        }
        return urlPath;
    }

    private static Path getPath(String url) {
        Path urlPath;
        URL toURL = null;
        try {
            final URI uri = URI.create(url.trim()).normalize();
            toURL = uri.toURL();
            urlPath = Paths.get(uri);
        } catch (Exception e) {
            if (toURL != null) {
                urlPath = Paths.get(StringUtils.isBlank(toURL.getFile()) ? toURL.getHost() : toURL.getFile());
            } else {
                urlPath = Paths.get(url);
            }
        }
        return urlPath;
    }

    private static boolean pathStartsWithOther(Path resolvedPath, Path basePath) throws IOException {
        try {
            return resolvedPath.toFile().getCanonicalFile().toPath().startsWith(basePath.toRealPath());
        } catch (Exception e) {
            if (e instanceof NoSuchFileException) { // If we're about to create a file this exception has been thrown
                return resolvedPath.toFile().getCanonicalFile().toPath().startsWith(basePath);
            }
            return false;
        }
    }

    public static boolean isFile(String fileName) {
        return from(fileName) == SupportedProtocols.file;
    }

    public static OutputStream getOutputStream(String fileName) {
        return getOutputStream(fileName, ExportConfig.EMPTY);
    }

    public static OutputStream getOutputStream(String fileName, ExportConfig config) {
        if (fileName.equals("-")) {
            return null;
        }
        return getOutputStream(from(fileName), fileName, config);
    }

    public static OutputStream getOutputStream(SupportedProtocols protocol, String fileName, ExportConfig config) {
        if (fileName == null) return null;
        final CompressionAlgo compressionAlgo = CompressionAlgo.valueOf(config.getCompressionAlgo());
        final OutputStream outputStream;
        try {
            switch (protocol) {
                case s3 -> outputStream = S3UploadUtils.writeFile(fileName);
                case hdfs -> outputStream = HDFSUtils.writeFile(fileName);
                default -> {
                    final File file = isImportUsingNeo4jConfig()
                            ? resolvePath(fileName).toFile()
                            : new File(getFileUrl(fileName));
                    outputStream = new FileOutputStream(file);
                }
            }
            return new BufferedOutputStream(compressionAlgo.getOutputStream(outputStream));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isImportUsingNeo4jConfig() {
        return apocConfig().getBoolean(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG);
    }

    public static StreamConnection openS3InputStream(String urlAddress) throws IOException {
        if (!SupportedProtocols.s3.isEnabled()) {
            throw new MissingDependencyException(
                    "Cannot find the S3 jars in the plugins folder. \n"
                            + "Please put these files into the plugins folder :\n\n"
                            + "aws-java-sdk-core-x.y.z.jar\n"
                            + "aws-java-sdk-s3-x.y.z.jar\n"
                            + "httpclient-x.y.z.jar\n"
                            + "httpcore-x.y.z.jar\n"
                            + "joda-time-x.y.z.jar\n"
                            + "\nSee the documentation: https://neo4j.com/docs/apoc/current/import/web-apis/#_using_google_cloud_storage");
        }
        return S3URLConnection.openS3InputStream(new URL(urlAddress));
    }

    public static StreamConnection openHdfsInputStream(String urlAddress) throws IOException {
        if (!SupportedProtocols.hdfs.isEnabled()) {
            throw new MissingDependencyException(
                    "Cannot find the HDFS/Hadoop jars in the plugins folder. \n"
                            + "\nPlease, see the documentation: https://neo4j.com/docs/apoc/current/import/web-apis/#_using_google_cloud_storage");
        }
        return HDFSUtils.readFile(new URL(urlAddress));
    }

    /**
     * @return a File pointing to Neo4j's log directory, if it exists and is readable, null otherwise.
     */
    public static File getLogDirectory() {
        String neo4jHome = apocConfig().getString("server.directories.neo4j_home", "");
        String logDir = apocConfig().getString("server.directories.logs", "");

        File logs = logDir.isEmpty() ? new File(neo4jHome, "logs") : new File(logDir);

        if (logs.exists() && logs.canRead() && logs.isDirectory()) {
            return logs;
        }

        return null;
    }

    public static CountingInputStream getInputStreamFromBinary(byte[] urlOrBinary, String compressionAlgo) {
        return CompressionAlgo.valueOf(compressionAlgo).toInputStream(urlOrBinary);
    }

    public static StreamConnection readHttpInputStream(
            String urlAddress,
            Map<String, Object> headers,
            String payload,
            int redirectLimit,
            URLAccessChecker urlAccessChecker)
            throws IOException {
        URL url = ApocConfig.apocConfig().checkAllowedUrlAndPinToIP(urlAddress, urlAccessChecker);
        URLConnection con = openUrlConnection(url, headers);
        writePayload(con, payload);
        String newUrl = handleRedirect(con, urlAddress);
        if (newUrl != null && !urlAddress.equals(newUrl)) {
            con.getInputStream().close();
            if (redirectLimit == 0) {
                throw new IOException("Redirect limit exceeded");
            }
            return readHttpInputStream(newUrl, headers, payload, --redirectLimit, urlAccessChecker);
        }

        return new StreamConnection.UrlStreamConnection(con);
    }

    public static URLConnection openUrlConnection(URL src, Map<String, Object> headers) throws IOException {
        URLConnection con = src.openConnection();
        con.setRequestProperty("User-Agent", "APOC Procedures for Neo4j");
        if (con instanceof HttpURLConnection) {
            HttpURLConnection http = (HttpURLConnection) con;
            http.setInstanceFollowRedirects(false);
            if (headers != null) {
                Object method = headers.get("method");
                if (method != null) {
                    http.setRequestMethod(method.toString());
                    http.setChunkedStreamingMode(1024 * 1024);
                }
                headers.forEach((k, v) -> con.setRequestProperty(k, v == null ? "" : v.toString()));
            }
        }

        con.setConnectTimeout(apocConfig().getInt("apoc.http.timeout.connect", 10_000));
        con.setReadTimeout(apocConfig().getInt("apoc.http.timeout.read", 60_000));
        return con;
    }

    private static void writePayload(URLConnection con, String payload) throws IOException {
        if (payload == null) return;
        con.setDoOutput(true);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
        writer.write(payload);
        writer.close();
    }

    private static String handleRedirect(URLConnection con, String url) throws IOException {
        if (!(con instanceof HttpURLConnection)) return url;
        if (!isRedirect(((HttpURLConnection) con))) return url;
        return con.getHeaderField("Location");
    }

    public static CountingInputStream openInputStream(
            Object input,
            Map<String, Object> headers,
            String payload,
            String compressionAlgo,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        if (input instanceof String) {
            String urlAddress = (String) input;
            final ArchiveType archiveType = ArchiveType.from(urlAddress);
            if (archiveType.isArchive()) {
                return getStreamCompressedFile(urlAddress, headers, payload, archiveType, urlAccessChecker);
            }

            StreamConnection sc = getStreamConnection(urlAddress, headers, payload, urlAccessChecker);
            return sc.toCountingInputStream(compressionAlgo);
        } else if (input instanceof byte[]) {
            return FileUtils.getInputStreamFromBinary((byte[]) input, compressionAlgo);
        } else {
            throw new RuntimeException(ERROR_BYTES_OR_STRING);
        }
    }

    private static CountingInputStream getStreamCompressedFile(
            String urlAddress,
            Map<String, Object> headers,
            String payload,
            ArchiveType archiveType,
            URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        StreamConnection sc;
        InputStream stream;
        String[] tokens = urlAddress.split("!");
        urlAddress = tokens[0];
        String zipFileName;
        if (tokens.length == 2) {
            zipFileName = tokens[1];
            sc = getStreamConnection(urlAddress, headers, payload, urlAccessChecker);
            stream = getFileStreamIntoCompressedFile(sc.getInputStream(), zipFileName, archiveType);
            stream = toLimitedIStream(stream, sc.getLength());
        } else throw new IllegalArgumentException("filename can't be null or empty");

        return new CountingInputStream(stream, sc.getLength());
    }

    public static StreamConnection getStreamConnection(
            String urlAddress, Map<String, Object> headers, String payload, URLAccessChecker urlAccessChecker)
            throws IOException, URISyntaxException, URLAccessValidationError {
        return FileUtils.getStreamConnection(
                FileUtils.from(urlAddress), urlAddress, headers, payload, urlAccessChecker);
    }

    private static InputStream getFileStreamIntoCompressedFile(InputStream is, String fileName, ArchiveType archiveType)
            throws IOException {
        try (ArchiveInputStream archive = archiveType.getInputStream(is)) {
            ArchiveEntry archiveEntry;

            while ((archiveEntry = archive.getNextEntry()) != null) {
                if (!archiveEntry.isDirectory() && archiveEntry.getName().equals(fileName)) {
                    return new ByteArrayInputStream(IOUtils.toByteArray(archive));
                }
            }
        }

        return null;
    }

    public static Object getStringOrCompressedData(StringWriter writer, ExportConfig config) {
        try {
            final String compression = config.getCompressionAlgo();
            final String writerString = writer.toString();
            Object data = compression.equals(CompressionAlgo.NONE.name())
                    ? writerString
                    : CompressionAlgo.valueOf(compression).compress(writerString, config.getCharset());
            writer.getBuffer().setLength(0);
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
