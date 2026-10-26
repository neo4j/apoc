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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_LABEL_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_TYPE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_TYPE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_SOURCE_LABEL;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_TARGET_LABEL;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_UNIQUENESS;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;
import static org.neo4j.graphdb.schema.IndexType.POINT;
import static org.neo4j.graphdb.schema.IndexType.RANGE;
import static org.neo4j.graphdb.schema.IndexType.TEXT;
import static org.neo4j.graphdb.schema.IndexType.VECTOR;

import apoc.export.util.LimitedSizeInputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexType;

class UtilTest {

    @TempDir
    static Path tempDir;

    /** Tracks whether the underlying archive source stream has been closed. */
    private static class CloseTrackingInputStream extends FilterInputStream {
        boolean closed = false;

        CloseTrackingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    @BeforeAll
    static void setup() {
        // ensure an ApocConfig instance exists so LimitedSizeInputStream's static config lookups
        // (SIZE_MULTIPLIER, MAX_ABSOLUTE_SIZE) don't NPE if this is the first test to touch the class
        new apoc.ApocConfig(Mockito.mock(Config.class));
    }

    private static Path createZip(Path path, String entryName, byte[] content) throws IOException {
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(path))) {
            zip.putNextEntry(new ZipEntry(entryName));
            zip.write(content);
            zip.closeEntry();
        }
        return path;
    }

    @Test
    void archiveEntryIsStreamedNotMaterialized() throws Exception {
        byte[] content = "some json payload".getBytes(StandardCharsets.UTF_8);
        Path zip = createZip(tempDir.resolve("entry.zip"), "data.json", content);
        byte[] zipBytes = Files.readAllBytes(zip);
        CloseTrackingInputStream source = new CloseTrackingInputStream(new ByteArrayInputStream(zipBytes));

        try (InputStream stream =
                Util.getFileStreamIntoCompressedFile(source, "data.json", ArchiveType.ZIP, zipBytes.length)) {
            // a ByteArrayInputStream would mean the whole entry was materialized in memory first
            assertFalse(stream instanceof ByteArrayInputStream);
            assertArrayEquals(content, stream.readAllBytes());
        }

        // closing the returned entry stream must close the archive and, in turn, the source
        assertTrue(source.closed);
    }

    @Test
    void missingArchiveEntryClosesArchiveAndSource() throws Exception {
        Path zip = createZip(
                tempDir.resolve("entry.zip"), "data.json", "some json payload".getBytes(StandardCharsets.UTF_8));
        byte[] zipBytes = Files.readAllBytes(zip);
        CloseTrackingInputStream source = new CloseTrackingInputStream(new ByteArrayInputStream(zipBytes));

        assertNull(Util.getFileStreamIntoCompressedFile(source, "other.json", ArchiveType.ZIP, zipBytes.length));
        assertTrue(source.closed);
    }

    @Test
    void tarArchiveEntryIsStreamedNotMaterialized() throws Exception {
        byte[] content = "some json payload".getBytes(StandardCharsets.UTF_8);
        Path tar = tempDir.resolve("entry.tar");
        try (TarArchiveOutputStream tarOut =
                new TarArchiveOutputStream(Files.newOutputStream(tar), StandardCharsets.UTF_8.name())) {
            TarArchiveEntry entry = new TarArchiveEntry("data.json");
            entry.setSize(content.length);
            tarOut.putArchiveEntry(entry);
            tarOut.write(content);
            tarOut.closeArchiveEntry();
        }
        byte[] tarBytes = Files.readAllBytes(tar);
        CloseTrackingInputStream source = new CloseTrackingInputStream(new ByteArrayInputStream(tarBytes));

        try (InputStream stream =
                Util.getFileStreamIntoCompressedFile(source, "data.json", ArchiveType.TAR, tarBytes.length)) {
            assertFalse(stream instanceof ByteArrayInputStream);
            assertArrayEquals(content, stream.readAllBytes());
        }

        assertTrue(source.closed);
    }

    @Test
    void highRatioZipEntryIsRejectedBySizeBound() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                LimitedSizeInputStream.SIZE_MULTIPLIER >= 0, "ratio protection disabled in this JVM run");

        // 40 MB of zeros compresses to a few tens of KB, so with the default 200x ratio budget
        // (here: 200 * zip size, well below 40 MB) reading the entry must hit the size bound
        byte[] bomb = new byte[40 * 1024 * 1024];
        Path zip = createZip(tempDir.resolve("bomb.zip"), "data.json", bomb);
        byte[] zipBytes = Files.readAllBytes(zip);

        try (InputStream stream = Util.getFileStreamIntoCompressedFile(
                new ByteArrayInputStream(zipBytes), "data.json", ArchiveType.ZIP, zipBytes.length)) {
            IOException e = assertThrows(IOException.class, stream::readAllBytes);
            assertTrue(e.getMessage().contains("compression bomb"));
        }
    }

    @Test
    void handleRedirectResolvesRelativeLocationAgainstOriginalUrl() throws Exception {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("/relative/path");
        // con.getURL() reflects the IP-pinned connection URL (see ApocConfig#checkAllowedUrlAndPinToIP /
        // WebURLAccessRule#substituteHostByIP), which must NOT be used as the base for resolving the
        // relative Location - otherwise the next hop inherits the pinned IP instead of the original host.
        when(mockCon.getURL()).thenReturn(new URL("https://93.184.216.34/old/path"));

        String resolved = Util.handleRedirect(mockCon, "https://example.com/old/path");

        assertEquals("https://example.com/relative/path", resolved);
    }

    @Test
    void handleRedirectResolvesAbsoluteLocationRegardlessOfBase() throws Exception {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("https://other.example.com/new/path");
        when(mockCon.getURL()).thenReturn(new URL("https://93.184.216.34/old/path"));

        String resolved = Util.handleRedirect(mockCon, "https://example.com/old/path");

        assertEquals("https://other.example.com/new/path", resolved);
    }

    @Test
    void handleRedirectReturnsOriginalUrlWhenNotHttpConnection() throws Exception {
        URLConnection mockCon = mock(URLConnection.class);

        String resolved = Util.handleRedirect(mockCon, "https://example.com/old/path");

        assertEquals("https://example.com/old/path", resolved);
    }

    @Test
    void handleRedirectReturnsOriginalUrlWhenNotRedirect() throws Exception {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(200);

        String resolved = Util.handleRedirect(mockCon, "https://example.com/old/path");

        assertEquals("https://example.com/old/path", resolved);
    }

    @Test
    void isCrossOriginTreatsDifferentHostsAsCrossOrigin() throws Exception {
        assertTrue(Util.isCrossOrigin(
                new URL("https://api.example.com/report"), new URL("https://attacker.example/collect")));
        assertTrue(Util.isCrossOrigin(
                new URL("https://api.example.com/report"), new URL("https://evil.api.example.com/collect")));
        assertFalse(Util.isCrossOrigin(
                new URL("https://api.example.com/report"), new URL("https://API.example.com/other")));
    }

    @Test
    void isCrossOriginKeepsCredentialsOnSameHostRedirects() throws Exception {
        assertFalse(Util.isCrossOrigin(new URL("https://example.com/a"), new URL("https://example.com/b")));
        // scheme upgrades are explicitly allowed by isRedirect and must keep their credentials
        assertFalse(Util.isCrossOrigin(new URL("http://example.com/a"), new URL("https://example.com/b")));
    }

    @Test
    void isCrossOriginTreatsDifferentPortsAsCrossOrigin() throws Exception {
        assertTrue(Util.isCrossOrigin(new URL("https://example.com/a"), new URL("https://example.com:8443/b")));
        // explicit default port is the same origin
        assertFalse(Util.isCrossOrigin(new URL("https://example.com/a"), new URL("https://example.com:443/b")));
    }

    @Test
    void filterHeadersForCrossOriginRedirectDropsCredentials() {
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("Authorization", "Basic " + Util.encodeUserColonPassToBase64("user:secret"));
        headers.put("Cookie", "session=abc");
        headers.put("X-Api-Key", "secret");
        headers.put("method", "POST");
        headers.put("Accept", "application/json");
        headers.put("accept-encoding", "gzip");
        headers.put("EmptyHeader", null);

        Map<String, Object> filtered = Util.filterHeadersForCrossOriginRedirect(headers);

        assertEquals(Map.of("Accept", "application/json", "accept-encoding", "gzip"), filtered);
    }

    @Test
    void filterHeadersForCrossOriginRedirectHandlesNullHeaders() {
        assertNull(Util.filterHeadersForCrossOriginRedirect(null));
    }

    /**
     * If any new constraints or indexes are added, this test will fail.
     * Add the new constraints/indexes to the tests as well and update
     * the apoc.schema.* procedures to work with them.
     */
    @Test
    void testAPOCisAwareOfAllConstraints() {
        assertEquals(
                Arrays.stream(ConstraintType.values()).collect(Collectors.toSet()),
                Set.of(
                        UNIQUENESS,
                        NODE_PROPERTY_EXISTENCE,
                        RELATIONSHIP_PROPERTY_EXISTENCE,
                        NODE_KEY,
                        RELATIONSHIP_KEY,
                        RELATIONSHIP_UNIQUENESS,
                        RELATIONSHIP_PROPERTY_TYPE,
                        NODE_PROPERTY_TYPE,
                        RELATIONSHIP_SOURCE_LABEL,
                        RELATIONSHIP_TARGET_LABEL,
                        NODE_LABEL_EXISTENCE));
    }

    @Test
    void testAPOCisAwareOfAllIndexes() {
        assertEquals(
                Arrays.stream(IndexType.values()).collect(Collectors.toSet()),
                Set.of(FULLTEXT, LOOKUP, TEXT, RANGE, POINT, VECTOR));
    }
}
