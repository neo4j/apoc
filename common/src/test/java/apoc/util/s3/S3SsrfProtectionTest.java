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
package apoc.util.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import apoc.util.FileUtils;
import apoc.util.SupportedProtocols;
import java.net.URL;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.graphdb.security.URLAccessValidationError;

class S3SsrfProtectionTest {

    private static URLAccessChecker blocking(String blockedHost) throws Exception {
        URLAccessChecker checker = mock(URLAccessChecker.class);
        when(checker.checkURL(any(URL.class))).thenAnswer(inv -> {
            URL url = inv.getArgument(0);
            if (url.getHost().equals(blockedHost)) {
                throw new URLAccessValidationError("access to " + blockedHost + " is blocked");
            }
            return url;
        });
        return checker;
    }

    @Test
    void portStrippingOnlyAffectsTrailingDefaultPort() {
        assertEquals(
                "host:8080", S3ParamsExtractor.extract("s3://a:b@host:8080/b/k").getEndpoint());
        assertEquals(
                "host:4430", S3ParamsExtractor.extract("s3://a:b@host:4430/b/k").getEndpoint());
        assertEquals("host", S3ParamsExtractor.extract("s3://a:b@host:443/b/k").getEndpoint());
        assertEquals("host", S3ParamsExtractor.extract("s3://a:b@host:80/b/k").getEndpoint());
    }

    @Test
    void awsEndpointDetection() {
        assertTrue(
                S3ParamsExtractor.extract("s3://s3.us-east-2.amazonaws.com/b/k").isAwsEndpoint());
        assertTrue(S3ParamsExtractor.extract("s3://us-east-2/b/k").isAwsEndpoint());
        assertFalse(S3ParamsExtractor.extract("s3://attacker.example.com/b/k").isAwsEndpoint());
        assertFalse(S3ParamsExtractor.extract("s3://amazonaws.com.attacker.example/b/k")
                .isAwsEndpoint());
        assertFalse(S3ParamsExtractor.extract("s3://10.0.0.5:9000/b/k").isAwsEndpoint());
    }

    @Test
    void ambientCredentialsNeverUsedWithCustomEndpoint() {
        S3Params params = S3ParamsExtractor.extract("s3://attacker.example.com/b/k");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new S3Aws(params, null));
        assertTrue(e.getMessage().contains("accessKey"));
    }

    @Test
    void ambientCredentialsStillAllowedForAws() {
        S3Params params = S3ParamsExtractor.extract("s3://s3.us-east-2.amazonaws.com/b/k");
        assertNotNull(new S3Aws(params, null).getClient());
    }

    @Test
    void blockedHostIsRejected() throws Exception {
        URLAccessChecker checker = blocking("10.0.0.5");
        assertThrows(
                URLAccessValidationError.class,
                () -> FileUtils.checkRemoteStorageUrl(SupportedProtocols.s3, "s3://a:b@10.0.0.5:9000/b/k", checker));
    }

    @Test
    void hostIsValidatedAfterRegionStripping() throws Exception {
        URLAccessChecker checker = blocking("169.254.169.254");
        assertThrows(
                URLAccessValidationError.class,
                () -> FileUtils.checkRemoteStorageUrl(
                        SupportedProtocols.s3, "s3://a:b@us-east-1.169.254.169.254/b/k", checker));
    }

    @Test
    void hostIsValidatedAfterPortNormalization() throws Exception {
        URLAccessChecker checker = mock(URLAccessChecker.class);
        FileUtils.checkRemoteStorageUrl(SupportedProtocols.s3, "s3://a:b@host:8080/b/k", checker);
        org.mockito.ArgumentCaptor<URL> captor = org.mockito.ArgumentCaptor.forClass(URL.class);
        verify(checker).checkURL(captor.capture());
        assertEquals("host", captor.getValue().getHost());
        assertEquals(8080, captor.getValue().getPort());
    }

    @Test
    void hdfsHostIsValidated() throws Exception {
        URLAccessChecker checker = blocking("10.0.0.5");
        assertThrows(
                URLAccessValidationError.class,
                () -> FileUtils.checkRemoteStorageUrl(SupportedProtocols.hdfs, "hdfs://10.0.0.5:9000/file", checker));
    }

    @Test
    void hdfsWithoutBlockedHostIsAllowed() throws Exception {
        URLAccessChecker checker = blocking("10.0.0.5");
        FileUtils.checkRemoteStorageUrl(SupportedProtocols.hdfs, "hdfs://namenode.example.com:8020/file", checker);
        verify(checker).checkURL(any(URL.class));
    }

    @Test
    void hdfsWithUnparseableHostFailsClosed() {
        URLAccessChecker checker = mock(URLAccessChecker.class);
        assertThrows(
                URLAccessValidationError.class,
                () -> FileUtils.checkRemoteStorageUrl(SupportedProtocols.hdfs, "hdfs://bad_host:8020/file", checker));
    }

    @Test
    void realAwsAndOtherProtocolsAreNotChecked() throws Exception {
        URLAccessChecker checker = mock(URLAccessChecker.class);
        FileUtils.checkRemoteStorageUrl(SupportedProtocols.s3, "s3://us-east-1/b/k", checker);
        FileUtils.checkRemoteStorageUrl(SupportedProtocols.file, "file:///x", checker);
        verify(checker, never()).checkURL(any(URL.class));
        assertNull(S3ParamsExtractor.extract("s3://us-east-1/b/k").getEndpointUri());
    }
}
