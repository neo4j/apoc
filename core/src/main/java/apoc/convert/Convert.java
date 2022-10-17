package apoc.convert;

import apoc.coll.SetBackedList;
import apoc.meta.Types;
import apoc.util.Util;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 29.05.16
 */
public class Convert {

    @Context
    public Log log;

    @UserFunction("apoc.convert.toMap")
    @Description("Converts the given value into a map.")
    public Map<String, Object> toMap(@Name("map") Object map) {

        if (map instanceof Entity) {
            return ((Entity)map).getAllProperties();
        } else if (map instanceof Map) {
            return (Map<String, Object>) map;
        } else {
            return null;
        }
    }

    @UserFunction("apoc.convert.toList")
    @Description("Converts the given value into a list.")
    public List<Object> toList(@Name("value") Object list) {
        return ConvertUtils.convertToList(list);
    }

    @UserFunction("apoc.convert.toNode")
    @Description("Converts the given value into a node.")
    public Node toNode(@Name("node") Object node) {
        return node instanceof Node ? (Node) node :  null;
    }

    @UserFunction("apoc.convert.toRelationship")
    @Description("Converts the given value into a relationship.")
    public Relationship toRelationship(@Name("rel") Object relationship) {
        return relationship instanceof Relationship ? (Relationship) relationship :  null;
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> convertToList(Object list, Class<T> type) {
        List<Object> convertedList = ConvertUtils.convertToList(list);
        if (convertedList == null) {
        	return null;
        }
        Stream<T> stream = null;
        Types varType = Types.of(type);
    	switch (varType) {
    	case INTEGER:
    		stream = (Stream<T>) convertedList.stream().map(Util::toLong);
    		break;
    	case FLOAT:
    		stream = (Stream<T>) convertedList.stream().map(Util::toDouble);
    		break;
    	case STRING:
    		stream = (Stream<T>) convertedList.stream().map(Util::toString);
    		break;
    	case BOOLEAN:
    		stream = (Stream<T>) convertedList.stream().map(Util::toBoolean);
    		break;
    	case NODE:
    		stream = (Stream<T>) convertedList.stream().map(this::toNode);
    		break;
    	case RELATIONSHIP:
    		stream = (Stream<T>) convertedList.stream().map(this::toRelationship);
    		break;
		default:
			throw new RuntimeException("Supported types are: Integer, Float, String, Boolean, Node, Relationship");
    	}
    	return stream.collect(Collectors.toList());
    }

	@SuppressWarnings("unchecked")
    @UserFunction("apoc.convert.toSet")
    @Description("Converts the given value into a set.")
    public List<Object> toSet(@Name("list") Object value) {
        List list = ConvertUtils.convertToList(value);
        return list == null ? null : new SetBackedList(new LinkedHashSet<>(list));
    }

	@UserFunction("apoc.convert.toNodeList")
	@Description("Converts the given value into a list of nodes.")
	public List<Node> toNodeList(@Name("list") Object list) {
        return convertToList(list, Node.class);
	}

	@UserFunction("apoc.convert.toRelationshipList")
	@Description("Converts the given value into a list of relationships.")
	public List<Relationship> toRelationshipList(@Name("relList") Object list) {
        return convertToList(list, Relationship.class);
	}
}
