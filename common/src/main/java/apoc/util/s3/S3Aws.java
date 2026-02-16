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

import static apoc.export.util.LimitedSizeInputStream.toLimitedIStream;

import apoc.util.StreamConnection;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3Aws {

    S3Client s3Client;

    public S3Aws(S3Params s3Params, String region) {

        AwsCredentialsProvider credentialsProvider =
                getCredentialsProvider(s3Params.getAccessKey(), s3Params.getSecretKey(), s3Params.getSessionToken());

        S3ClientBuilder builder = S3Client.builder();
        builder.credentialsProvider(credentialsProvider)
                .overrideConfiguration(S3URLConnection.buildClientOverrideConfig())
                .forcePathStyle(true)
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .checksumValidationEnabled(false)
                        .build());

        region = Objects.nonNull(region) ? region : s3Params.getRegion();
        String endpoint = s3Params.getEndpoint();
        if (Objects.nonNull(endpoint)) {
            URI endpointUri;
            if (!endpoint.startsWith("http://") && !endpoint.startsWith("https://")) {
                String protocol = S3URLConnection.getConfiguredProtocol();
                endpointUri = URI.create(protocol + "://" + endpoint);
            } else {
                endpointUri = URI.create(endpoint);
            }
            builder.endpointOverride(endpointUri);
            if (region != null) {
                builder.region(Region.of(region));
            }
        } else if (Objects.nonNull(region)) {
            builder.region(Region.of(region));
        }

        s3Client = builder.build();
    }

    public S3Client getClient() {
        return s3Client;
    }

    public StreamConnection getS3AwsInputStream(S3Params s3Params) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3Params.getBucket())
                .key(s3Params.getKey())
                .build();
        ResponseInputStream<GetObjectResponse> response = s3Client.getObject(getObjectRequest);
        GetObjectResponse metadata = response.response();
        return new StreamConnection() {
            @Override
            public InputStream getInputStream() {
                return toLimitedIStream(response, getLength());
            }

            @Override
            public String getEncoding() {
                return metadata.contentEncoding();
            }

            @Override
            public long getLength() {
                return metadata.contentLength();
            }

            @Override
            public String getName() {
                return s3Params.getKey();
            }
        };
    }

    private static AwsCredentialsProvider getCredentialsProvider(
            final String accessKey, final String secretKey, final String sessionToken) {

        if (Objects.nonNull(accessKey) && !accessKey.isEmpty() && Objects.nonNull(secretKey) && !secretKey.isEmpty()) {
            final AwsCredentials credentials;
            if (Objects.isNull(sessionToken) || sessionToken.isEmpty()) {
                credentials = AwsBasicCredentials.create(accessKey, secretKey);
            } else {
                credentials = AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
            }
            return StaticCredentialsProvider.create(credentials);
        }
        return DefaultCredentialsProvider.create();
    }
}
