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
package apoc.util;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_LABEL_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_TYPE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_TYPE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_SOURCE_LABEL;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_TARGET_LABEL;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_UNIQUENESS;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;
import static org.neo4j.graphdb.schema.IndexType.POINT;
import static org.neo4j.graphdb.schema.IndexType.RANGE;
import static org.neo4j.graphdb.schema.IndexType.TEXT;
import static org.neo4j.graphdb.schema.IndexType.VECTOR;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexType;

public class UtilTest {

    /**
     * If any new constraints or indexes are added, this test will fail.
     * Add the new constraints/indexes to the tests as well and update
     * the apoc.schema.* procedures to work with them.
     */
    @Test
    public void testAPOCisAwareOfAllConstraints() {
        assertEquals(
                Arrays.stream(ConstraintType.values()).collect(Collectors.toSet()),
                Set.of(
                        UNIQUENESS,
                        NODE_PROPERTY_EXISTENCE,
                        RELATIONSHIP_PROPERTY_EXISTENCE,
                        NODE_KEY,
                        RELATIONSHIP_KEY,
                        RELATIONSHIP_UNIQUENESS,
                        RELATIONSHIP_PROPERTY_TYPE,
                        NODE_PROPERTY_TYPE,
                        RELATIONSHIP_SOURCE_LABEL,
                        RELATIONSHIP_TARGET_LABEL,
                        NODE_LABEL_EXISTENCE));
    }

    @Test
    public void testAPOCisAwareOfAllIndexes() {
        assertEquals(
                Arrays.stream(IndexType.values()).collect(Collectors.toSet()),
                Set.of(FULLTEXT, LOOKUP, TEXT, RANGE, POINT, VECTOR));
    }
}
