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

import java.util.Arrays;
import java.util.List;
import org.neo4j.procedure.Description;

public class AssertSchemaResult {
    @Description("The label associated with the constraint or index.")
    public final Object label;

    @Description("The property key associated with the constraint or index.")
    public final String key;

    @Description("The property keys associated with the constraint or index.")
    public final List<String> keys;

    @Description("Whether or not this is a uniqueness constraint.")
    public boolean unique = false;

    @Description("The action applied to this constraint or index; can be [\"KEPT\", \"CREATED\", \"DROPPED\"]")
    public String action = "KEPT";

    public AssertSchemaResult(Object label, List<String> keys) {
        this.label = label;
        if (keys.size() == 1) {
            this.key = keys.get(0);
            this.keys = keys;
        } else {
            this.keys = keys;
            this.key = null;
        }
    }

    public AssertSchemaResult(String label, String key) {
        this.label = label;
        this.keys = Arrays.asList(key);
        this.key = key;
    }

    public AssertSchemaResult unique() {
        this.unique = true;
        return this;
    }

    public AssertSchemaResult dropped() {
        this.action = "DROPPED";
        return this;
    }

    public AssertSchemaResult created() {
        this.action = "CREATED";
        return this;
    }
}
