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
package org.neo4j.cypher.export;

import java.util.Collection;
import java.util.Map;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public sealed interface ExportData {
    record Database() implements ExportData {}

    record NodesAndRels(Collection<Node> nodes, Collection<Relationship> rels) implements ExportData {}

    record Query(String cypher, Map<String, Object> params) implements ExportData {}
}
