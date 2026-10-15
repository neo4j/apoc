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

import java.net.URI;

public class S3Params {

    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String endpoint;
    private final String bucket;
    private final String key;
    private final String region;

    public S3Params(
            String accessKey,
            String secretKey,
            String sessionToken,
            String endpoint,
            String bucket,
            String key,
            String region) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.sessionToken = sessionToken;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.key = key;
        this.region = region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }

    public String getEndpoint() {
        return endpoint;
    }

    /**
     * @return the exact URI the AWS SDK will connect to, or null if no custom endpoint is set (i.e. real AWS)
     */
    public URI getEndpointUri() {
        if (endpoint == null) {
            return null;
        }
        if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
            return URI.create(endpoint);
        }
        return URI.create(S3URLConnection.getConfiguredProtocol() + "://" + endpoint);
    }

    /**
     * @return true if there is no custom endpoint, or the endpoint is an official AWS one
     */
    public boolean isAwsEndpoint() {
        URI uri = getEndpointUri();
        if (uri == null) {
            return true;
        }
        String host = uri.getHost();
        if (host == null) {
            return false;
        }
        host = host.toLowerCase();
        return host.equals("amazonaws.com")
                || host.endsWith(".amazonaws.com")
                || host.equals("amazonaws.com.cn")
                || host.endsWith(".amazonaws.com.cn");
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }

    public String getRegion() {
        return region;
    }
}
