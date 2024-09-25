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
package apoc.refactor;

import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;

public class RefactorRelationshipResult {
    @Description("The internal id of the original entity.")
    public long input;

    @Description("The copied entity.")
    public Relationship output;

    @Description("Any error that occurred during the copy process.")
    public String error;

    public RefactorRelationshipResult(Long id) {
        this.input = id;
    }

    public RefactorRelationshipResult withError(Exception e) {
        this.error = e.getMessage();
        return this;
    }

    public RefactorRelationshipResult withError(String message) {
        this.error = message;
        return this;
    }

    public RefactorRelationshipResult withOther(Relationship rel) {
        this.output = rel;
        return this;
    }
}
