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

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import apoc.util.Util;
import java.io.File;
import java.net.URI;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Container implements AutoCloseable {
    private static final String S3_BUCKET_NAME = "test-bucket";
    private final LocalStackContainer localstack;
    private final S3Client s3;

    public S3Container() {
        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:1.2.0")).withServices(S3);
        localstack.addExposedPorts(4566);
        localstack.start();

        s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(getCredentialsProvider())
                .forcePathStyle(true)
                .build();
        s3.createBucket(CreateBucketRequest.builder().bucket(S3_BUCKET_NAME).build());
    }

    public void close() {
        Util.close(localstack);
    }

    public URI getEndpoint() {
        return localstack.getEndpoint();
    }

    public String getRegion() {
        return localstack.getRegion();
    }

    public StaticCredentialsProvider getCredentialsProvider() {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));
    }

    public String getUrl(String key) {
        AwsCredentials credentials = getCredentialsProvider().resolveCredentials();
        return String.format(
                "s3://%s.%s/%s/%s?accessKey=%s&secretKey=%s",
                getRegion(),
                getEndpoint().toString().replace("http://", ""),
                S3_BUCKET_NAME,
                key,
                credentials.accessKeyId(),
                credentials.secretAccessKey());
    }

    @SuppressWarnings("unused") // used from extended
    public String putFile(String fileName) {
        final File file = new File(fileName);
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(S3_BUCKET_NAME)
                        .key(file.getName())
                        .build(),
                file.toPath());
        return getUrl(file.getName());
    }
}
