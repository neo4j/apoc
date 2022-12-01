package apoc.export.arrow;

import apoc.meta.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.graphdb.GraphDatabaseService;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.arrow.ExportArrowStrategy.fromMetaType;
import static apoc.export.arrow.ExportArrowStrategy.toField;

public interface ExportResultStrategy {

    default Schema schemaFor(GraphDatabaseService db, List<Map<String, Object>> records) {
        final List<Field> fields = records.stream()
                .flatMap(m -> m.entrySet().stream())
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), fromMetaType(Types.of(e.getValue()))))
                .collect(Collectors.groupingBy(e -> e.getKey(), Collectors.mapping(e -> e.getValue(), Collectors.toSet())))
                .entrySet()
                .stream()
                .map(e -> toField(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return new Schema(fields);
    }
}
