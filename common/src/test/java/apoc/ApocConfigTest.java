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
package apoc;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

public class ApocConfigTest {

    private ApocConfig apocConfig;
    private File apocConfigFile;

    @BeforeEach
    void setup() throws Exception {
        InternalLogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.get(GraphDatabaseSettings.allow_file_urls)).thenReturn(false);

        apocConfigFile =
                new File(getClass().getClassLoader().getResource("apoc.conf").toURI());
        when(neo4jConfig.get(GraphDatabaseSettings.configuration_directory))
                .thenReturn(Path.of(apocConfigFile.getParent()));

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        apocConfig =
                new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);
    }

    @Test
    void testDetermineNeo4jConfFolderDefault() {
        assertEquals(apocConfigFile.getParent(), apocConfig.determineNeo4jConfFolder());
    }

    @Test
    void testApocConfFileBeingLoaded() {
        apocConfig.init();

        assertEquals("bar", apocConfig.getConfig().getString("foo"));
    }
}
