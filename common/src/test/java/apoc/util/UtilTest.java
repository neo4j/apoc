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

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexType;

class UtilTest {

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
