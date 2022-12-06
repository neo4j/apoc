package apoc.label;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class Label {

    @UserFunction("apoc.label.exists")
    @Description("Returns true or false depending on whether or not the given label exists.")
    public boolean exists(@Name("node") Object element, @Name("label") String label) {

        return element instanceof Node ? ((Node) element).hasLabel(org.neo4j.graphdb.Label.label(label)) :
                element instanceof Relationship ? ((Relationship) element).isType(RelationshipType.withName(label)) : false;

    }
}