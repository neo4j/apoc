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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.neo4j.test.assertion.Assert;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

/**
 * Utility class for testing Amazon S3 related functionality.
 */
public class S3TestUtil {

    /**
     * Read file object as a string from S3 bucket. This code expects valid AWS credentials are set up.
     * @param s3Url String containing url to S3 bucket.
     * @return the s3 string object
     */
    public static String readS3FileToString(String s3Url) throws SdkException {
        try {
            ResponseInputStream<GetObjectResponse> response = getS3Object(s3Url);
            return new String(response.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ResponseInputStream<GetObjectResponse> getS3Object(String s3Url)
            throws MalformedURLException, SdkException {
        S3Params s3Params = S3ParamsExtractor.extract(new URL(s3Url));
        S3Aws s3Aws = new S3Aws(s3Params, s3Params.getRegion());
        S3Client s3Client = s3Aws.getClient();

        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(s3Params.getBucket())
                .key(s3Params.getKey())
                .build());
    }

    public static void assertStringFileEquals(String expected, String s3Url) {
        assertS3KeyEventually(() -> {
            final String actual = readS3FileToString(s3Url);
            assertEquals(expected, actual);
        });
    }

    public static void assertS3KeyEventually(Runnable runnable) {
        Assert.assertEventually(
                () -> {
                    try {
                        runnable.run();
                        return true;
                    } catch (NoSuchKeyException e) {
                        return false;
                    }
                },
                v -> v,
                30L,
                TimeUnit.SECONDS);
    }
}
