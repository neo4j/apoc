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

import static apoc.util.TestContainerUtil.createNeo4jContainer;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Session;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDockerTestBase {
    Neo4jContainerExtension neo4jContainer;
    Session session;

    abstract TestContainerUtil.Neo4jVersion neo4jEdition();

    @BeforeAll
    void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createNeo4jContainer(
                List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI(), neo4jEdition(), null, Map.of());
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    void afterAll() {
        try {
            if (session != null) session.close();
        } finally {
            if (neo4jContainer != null) neo4jContainer.close();
        }
    }
}
