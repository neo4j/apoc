package apoc.util;

import org.junit.Test;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.NODE_PROPERTY_TYPE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_KEY;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_PROPERTY_TYPE;
import static org.neo4j.graphdb.schema.ConstraintType.RELATIONSHIP_UNIQUENESS;
import static org.neo4j.graphdb.schema.ConstraintType.UNIQUENESS;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;
import static org.neo4j.graphdb.schema.IndexType.POINT;
import static org.neo4j.graphdb.schema.IndexType.RANGE;
import static org.neo4j.graphdb.schema.IndexType.TEXT;

public class UtilTest {

    /**
     * If any new constraints or indexes are added, this test will fail.
     * Add the new constraints/indexes to the tests as well and update
     * the apoc.schema.* procedures to work with them.
     */
    @Test
    public void testAPOCisAwareOfAllConstraints() {
        assertEquals(Arrays.stream(ConstraintType.values()).collect(Collectors.toSet()), Set.of(
                UNIQUENESS,
                NODE_PROPERTY_EXISTENCE,
                RELATIONSHIP_PROPERTY_EXISTENCE,
                NODE_KEY,
                RELATIONSHIP_KEY,
                RELATIONSHIP_UNIQUENESS,
                RELATIONSHIP_PROPERTY_TYPE,
                NODE_PROPERTY_TYPE
        ));
    }

    @Test
    public void testAPOCisAwareOfAllIndexes() {
        assertEquals(Arrays.stream(IndexType.values()).collect(Collectors.toSet()), Set.of(
                FULLTEXT,
                LOOKUP,
                TEXT,
                RANGE,
                POINT
        ));
    }
}
