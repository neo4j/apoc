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

import static org.junit.Assert.*;

import org.junit.Test;

public class S3ParamsExtractorTest {

    @Test
    public void testEncodedS3Url() {
        S3Params params = S3ParamsExtractor.extract(
                "s3://accessKeyId:some%2Fsecret%2Fkey:some%2Fsession%2Ftoken@s3.us-east-2.amazonaws.com:1234/bucket/path/to/key");
        assertEquals("some/secret/key", params.getSecretKey());
        assertEquals("some/session/token", params.getSessionToken());
        assertEquals("accessKeyId", params.getAccessKey());
        assertEquals("bucket", params.getBucket());
        assertEquals("path/to/key", params.getKey());
        assertEquals("s3.us-east-2.amazonaws.com:1234", params.getEndpoint());
        assertEquals("us-east-2", params.getRegion());
    }

    @Test
    public void testEncodedS3UrlQueryParams() {
        S3Params params = S3ParamsExtractor.extract(
                "s3://s3.us-east-2.amazonaws.com:1234/bucket/path/to/key?accessKey=accessKeyId&secretKey=some%2Fsecret%2Fkey&sessionToken=some%2Fsession%2Ftoken");
        assertEquals("some/secret/key", params.getSecretKey());
        assertEquals("some/session/token", params.getSessionToken());
        assertEquals("accessKeyId", params.getAccessKey());
        assertEquals("bucket", params.getBucket());
        assertEquals("path/to/key", params.getKey());
        assertEquals("s3.us-east-2.amazonaws.com:1234", params.getEndpoint());
    }

    @Test
    public void testExtractEndpointPort() {
        assertEquals(
                "s3.amazonaws.com",
                S3ParamsExtractor.extract("s3://s3.amazonaws.com:80/bucket/path/to/key")
                        .getEndpoint());
        assertEquals(
                "s3.amazonaws.com:1234",
                S3ParamsExtractor.extract("s3://s3.amazonaws.com:1234/bucket/path/to/key")
                        .getEndpoint());
    }

    @Test
    public void testExtractRegion() {
        assertEquals(
                "us-east-2",
                S3ParamsExtractor.extract("s3://s3.us-east-2.amazonaws.com:80/bucket/path/to/key")
                        .getRegion());
        assertNull(S3ParamsExtractor.extract("s3://s3.amazonaws.com:80/bucket/path/to/key")
                .getRegion());
    }
}
