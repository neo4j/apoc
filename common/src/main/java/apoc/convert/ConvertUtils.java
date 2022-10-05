package apoc.convert;

import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author mh
 * @since 29.05.16
 */
public class ConvertUtils {
    @SuppressWarnings("unchecked")
    public static List convertToList(Object list) {
        if (list == null) return null;
        else if (list instanceof List) return (List) list;
        else if (list instanceof Collection) return new ArrayList((Collection)list);
        else if (list instanceof Iterable) return Iterables.asList((Iterable)list);
        else if (list instanceof Iterator) return Iterators.asList((Iterator)list);
        else if (list.getClass().isArray()) {
            final Object[] objectArray;
            if (list.getClass().getComponentType().isPrimitive()) {
                int length = Array.getLength(list);
                objectArray = new Object[length];
                for (int i = 0; i < length; i++) {
                    objectArray[i] = Array.get(list, i);
                }
            } else {
                objectArray = (Object[]) list;
            }
            List result = new ArrayList<>(objectArray.length);
            Collections.addAll(result, objectArray);
            return result;
        }
        return Collections.singletonList(list);
    }
}
