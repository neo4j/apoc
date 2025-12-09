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
package apoc.export.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;

@EnterpriseDbmsExtension
public class FormatUtilsTest {

    @Inject
    GraphDatabaseService db;

    @Test
    void formatString() {
        assertEquals("\"\\n\"", FormatUtils.formatString("\n"));
        assertEquals("\"\\t\"", FormatUtils.formatString("\t"));
        assertEquals("\"\\\"\"", FormatUtils.formatString("\""));
        assertEquals("\"\\\\\"", FormatUtils.formatString("\\"));
        assertEquals("\"\\n\"", FormatUtils.formatString('\n'));
        assertEquals("\"\\t\"", FormatUtils.formatString('\t'));
        assertEquals("\"\\\"\"", FormatUtils.formatString('"'));
        assertEquals("\"\\\\\"", FormatUtils.formatString('\\'));
    }

    @Test
    void joinLabels() {
        final String delimiter = ":";
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            assertEquals("", FormatUtils.joinLabels(node, delimiter));

            node.addLabel(Label.label("label_a"));
            node.addLabel(Label.label("label_c"));
            node.addLabel(Label.label("label_b"));
            assertEquals("label_a:label_b:label_c", FormatUtils.joinLabels(node, delimiter));
        }
    }
}
