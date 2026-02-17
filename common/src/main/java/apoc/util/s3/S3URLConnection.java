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

import apoc.util.StreamConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Objects;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;

public class S3URLConnection extends URLConnection {

    public static final String PROP_S3_HANDLER_USER_AGENT = "s3.handler.userAgent";
    public static final String PROP_S3_HANDLER_PROTOCOL = "s3.handler.protocol";
    public static final String PROP_S3_HANDLER_SIGNER_OVERRIDE = "s3.handler.signerOverride";

    public S3URLConnection(URL url) {
        super(url);
    }

    @Override
    public void connect() {}

    public static ClientOverrideConfiguration buildClientOverrideConfig() {
        final String userAgent = System.getProperty(PROP_S3_HANDLER_USER_AGENT, null);

        ClientOverrideConfiguration.Builder builder = ClientOverrideConfiguration.builder();

        if (userAgent != null) {
            builder.putHeader("User-Agent", userAgent);
        }

        return builder.build();
    }

    public static String getConfiguredProtocol() {
        return System.getProperty(PROP_S3_HANDLER_PROTOCOL, "https").toLowerCase();
    }

    public static StreamConnection openS3InputStream(URL url) {
        S3Params s3Params = S3ParamsExtractor.extract(url);
        String region = Objects.nonNull(s3Params.getRegion()) ? s3Params.getRegion() : Region.US_EAST_1.id();
        return new S3Aws(s3Params, region).getS3AwsInputStream(s3Params);
    }
}
