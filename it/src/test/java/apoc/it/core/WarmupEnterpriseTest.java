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
package apoc.it.core;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import apoc.util.TestContainerUtil;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

public class WarmupEnterpriseTest {

    @Test
    public void testWarmupIsntAllowedWithOtherStorageEngines() {
        final var conf = Map.of(
                GraphDatabaseInternalSettings.include_versions_under_development.name(), "true",
                GraphDatabaseSettings.db_format.name(), "multiversion");
        withSession(conf, session -> {
            assertThatThrownBy(() -> testCall(session, "CALL apoc.warmup.run()", (r) -> {}))
                    .isExactlyInstanceOf(ClientException.class)
                    .hasMessageContaining(
                            "Failed to invoke procedure `apoc.warmup.run`: Caused by: java.lang.IllegalArgumentException: `apoc.warmup.run` is only supported on record storage databases");
        });
    }

    @Test
    public void testWarmupOnEnterpriseStorageEngine() {
        final var conf = Map.of(
                GraphDatabaseInternalSettings.include_versions_under_development.name(), "true",
                GraphDatabaseSettings.db_format.name(), "high_limit");
        withSession(conf, session -> {
            testCall(session, "CALL apoc.warmup.run(true,true,true)", r -> {
                assertEquals(true, r.get("indexesLoaded"));
                assertNotEquals(0L, r.get("indexPages"));
            });
        });
    }

    @Test
    public void testOnRecordFormatDb() {
        withDriver(Map.of(), d -> {
            try (final var s =
                    d.session(SessionConfig.builder().withDatabase("system").build())) {
                s.run("CREATE DATABASE blockdb OPTIONS {storeFormat: 'block'} WAIT")
                        .consume();
            }
            try (final var s =
                    d.session(SessionConfig.builder().withDatabase("blockdb").build())) {
                assertThatThrownBy(() ->
                                s.run("CALL apoc.warmup.run(true,true,true)").consume())
                        .isExactlyInstanceOf(ClientException.class)
                        .hasMessageContaining(
                                "Failed to invoke procedure `apoc.warmup.run`: Caused by: java.lang.IllegalArgumentException: `apoc.warmup.run` is only supported on record storage databases");
            }
        });
    }

    private void withDriver(Map<String, String> conf, Consumer<Driver> f) {
        try (final var container = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true)) {
            conf.forEach(container::withNeo4jConfig);
            container.start();
            try (final var driver = container.getDriver()) {
                f.accept(driver);
            }
        }
    }

    private void withSession(Map<String, String> conf, Consumer<Session> f) {
        withDriver(conf, d -> f.accept(d.session()));
    }
}
