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
package apoc.result;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;

/**
 * @author mh
 * @since 26.02.16
 */
public class VirtualPathResult {
    @Description("The created virtual start node.")
    public final Node from;

    @Description("The created virtual relationship.")
    public final Relationship rel;

    @Description("The created virtual end node.")
    public final Node to;

    public VirtualPathResult(Node from, Relationship rel, Node to) {
        this.from = from;
        this.rel = rel;
        this.to = to;
    }
}
