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
package apoc.export.arrow;

import apoc.util.JsonUtil;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.values.storable.DurationValue;

public interface ExportArrowStrategy<IN, OUT> {

    OUT export(IN data, ArrowConfig config);

    Object convertValue(Object data);

    ArrowWriter newArrowWriter(VectorSchemaRoot root, OutputStream out);

    Schema schemaFor(List<Map<String, Object>> rows);

    TerminationGuard getTerminationGuard();

    BufferAllocator getBufferAllocator();

    GraphDatabaseService getGraphDatabaseApi();

    ExecutorService getExecutorService();

    Log getLogger();

    /** Fields for which a type mismatch was already logged during this export. */
    Set<String> getWarnedFields();

    static String fromMetaType(Object value) {
        final apoc.meta.Types type = apoc.meta.Types.of(value);
        if (type == apoc.meta.Types.LIST) {
            final apoc.meta.Types innerType = apoc.meta.Types.from(apoc.meta.Types.elementTypeName(value));
            if (innerType == apoc.meta.Types.LIST) {
                return "AnyArray";
            }
            return fromMetaType(innerType) + "Array";
        }
        return fromMetaType(type);
    }

    static String fromMetaType(apoc.meta.Types type) {
        return switch (type) {
            case INTEGER -> "Long";
            case FLOAT -> "Double";
            case LIST -> "AnyArray";
            case BOOLEAN -> "Boolean";
            case MAP -> "Map";
            case RELATIONSHIP -> "Relationship";
            case NODE -> "Node";
            case PATH -> "Path";
            case POINT -> "Point";
            case DATE -> "Date";
            case LOCAL_TIME, DATE_TIME, LOCAL_DATE_TIME -> "DateTime";
            case TIME -> "Time";
            case DURATION -> "Duration";
            default -> "String";
        };
    }

    static Field toField(String fieldName, Set<String> propertyTypes) {
        if (propertyTypes.size() > 1) {
            // return string type
            return new Field(fieldName, FieldType.nullable(new ArrowType.Utf8()), null);
        } else {
            // convert to RelatedType
            final String type = propertyTypes.iterator().next();
            switch (type) {
                case "Boolean":
                    return new Field(fieldName, FieldType.nullable(Types.MinorType.BIT.getType()), null);
                case "Long":
                    return new Field(fieldName, FieldType.nullable(Types.MinorType.BIGINT.getType()), null);
                case "Double":
                    return new Field(fieldName, FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
                case "DateTime":
                case "LocalDateTime":
                case "Date":
                    return new Field(fieldName, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
                case "Duration":
                case "Node":
                case "Relationship":
                    //                    return new Field(fieldName,
                    // FieldType.nullable(Types.MinorType.STRUCT.getType()), null);
                case "Point":
                case "Map":
                    //                    return new Field(fieldName, FieldType.nullable(Types.MinorType.MAP.getType()),
                    // null);
                case "DateTimeArray":
                case "DateArray":
                case "BooleanArray":
                case "LongArray":
                case "DoubleArray":
                case "StringArray":
                case "PointArray":
                default:
                    return (type.endsWith("Array"))
                            ? new Field(
                                    fieldName,
                                    FieldType.nullable(Types.MinorType.LIST.getType()),
                                    List.of(toField("$data$", Set.of(type.replace("Array", "")))))
                            : new Field(fieldName, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
            }
        }
    }

    default void write(int index, Object value, FieldVector fieldVector) {
        if (fieldVector instanceof BaseVariableWidthVector) {
            writeBaseVariableWidthVector(index, value, (BaseVariableWidthVector) fieldVector);
        } else if (fieldVector instanceof BigIntVector) {
            writeBigIntVector(index, value, (BigIntVector) fieldVector);
        } else if (fieldVector instanceof DateMilliVector) {
            writeDateMilliVector(index, value, (DateMilliVector) fieldVector);
        } else if (fieldVector instanceof Float8Vector) {
            writeFloat8Vector(index, value, (Float8Vector) fieldVector);
        } else if (fieldVector instanceof BitVector) {
            writeBitVector(index, value, (BitVector) fieldVector);
        } else if (fieldVector instanceof ListVector) {
            writeListVector(index, value, fieldVector);
        }
    }

    private static boolean isIntegral(Object value) {
        return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte;
    }

    /**
     * A value whose type disagrees with the (sampled) schema can't be written to its vector:
     * write null instead of aborting the export, and leave a trace in the log (the value itself is not logged).
     * Only the first mismatch per field is logged, to avoid flooding the log on large exports.
     */
    private void warnTypeMismatch(String fieldName, Object value) {
        if (getWarnedFields().add(fieldName)) {
            getLogger()
                    .warn(
                            "Arrow export: value of type %s does not match the schema type of field '%s' and was written as null. Further mismatches for this field are not logged.",
                            value.getClass().getSimpleName(), fieldName);
        }
    }

    private void writeListVector(int index, Object value, FieldVector fieldVector) {
        ListVector listVector = (ListVector) fieldVector;
        if (value == null) {
            listVector.setNull(index);
            return;
        }
        UnionListWriter listWriter = listVector.getWriter();
        Object[] array;
        if (value instanceof Collection) {
            final Collection collection = (Collection) value;
            array = collection.toArray(new Object[collection.size()]);
        } else if (value instanceof Object[]) {
            array = (Object[]) value;
        } else {
            warnTypeMismatch(fieldVector.getName(), value);
            listVector.setNull(index);
            return;
        }
        listWriter.setPosition(index);
        listWriter.startList();
        FieldVector inner = listVector.getChildrenFromFields().get(0);
        for (int i = 0; i < array.length; i++) {
            Object val = convertValue(array[i]);
            if (val == null) {
                listWriter.writeNull();
            } else if (inner instanceof ListVector) {
                write(i, val, inner);
            } else if (inner instanceof BaseVariableWidthVector) {
                final byte[] bytes;
                if (val instanceof String) {
                    bytes = val.toString().getBytes(StandardCharsets.UTF_8);
                } else {
                    bytes = JsonUtil.writeValueAsBytes(val);
                }
                ArrowBuf tempBuf = fieldVector.getAllocator().buffer(bytes.length);
                tempBuf.setBytes(0, bytes);
                listWriter.varChar().writeVarChar(0, bytes.length, tempBuf);
                tempBuf.close();
            } else if (inner instanceof BigIntVector) {
                if (isIntegral(val)) {
                    listWriter.bigInt().writeBigInt(((Number) val).longValue());
                } else {
                    warnTypeMismatch(inner.getName(), val);
                    listWriter.writeNull();
                }
            } else if (inner instanceof Float8Vector) {
                if (val instanceof Number) {
                    listWriter.float8().writeFloat8(((Number) val).doubleValue());
                } else {
                    warnTypeMismatch(inner.getName(), val);
                    listWriter.writeNull();
                }
            } else if (inner instanceof BitVector) {
                if (val instanceof Boolean) {
                    listWriter.bit().writeBit((Boolean) val ? 1 : 0);
                } else {
                    warnTypeMismatch(inner.getName(), val);
                    listWriter.writeNull();
                }
            } // TODO datemilli
        }
        listWriter.endList();
    }

    private void writeBitVector(int index, Object value, BitVector fieldVector) {
        BitVector baseVector = fieldVector;
        if (value == null) {
            baseVector.setNull(index);
        } else {
            if (value instanceof Boolean) {
                baseVector.setSafe(index, (Boolean) value ? 1 : 0);
            } else {
                warnTypeMismatch(fieldVector.getName(), value);
                baseVector.setNull(index);
            }
        }
    }

    private void writeFloat8Vector(int index, Object value, Float8Vector fieldVector) {
        Float8Vector baseVector = fieldVector;
        if (value == null) {
            baseVector.setNull(index);
        } else {
            if (value instanceof Number) {
                baseVector.setSafe(index, ((Number) value).doubleValue());
            } else {
                warnTypeMismatch(fieldVector.getName(), value);
                baseVector.setNull(index);
            }
        }
    }

    private void writeDateMilliVector(int index, Object value, DateMilliVector fieldVector) {
        DateMilliVector baseVector = fieldVector;
        final Long dateInMillis;
        if (value == null) {
            dateInMillis = null;
        } else if (value instanceof Date) {
            dateInMillis = ((Date) value).getTime();
        } else if (value instanceof LocalDateTime) {
            dateInMillis = ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else if (value instanceof ZonedDateTime) {
            dateInMillis = ((ZonedDateTime) value).toInstant().toEpochMilli();
        } else if (value instanceof OffsetDateTime) {
            dateInMillis = ((OffsetDateTime) value).toInstant().toEpochMilli();
        } else {
            dateInMillis = null;
        }
        if (dateInMillis == null) {
            baseVector.setNull(index);
        } else {
            baseVector.setSafe(index, dateInMillis);
        }
    }

    private void writeBigIntVector(int index, Object value, BigIntVector fieldVector) {
        BigIntVector baseVector = fieldVector;
        if (value == null) {
            baseVector.setNull(index);
        } else {
            if (isIntegral(value)) {
                baseVector.setSafe(index, ((Number) value).longValue());
            } else {
                warnTypeMismatch(fieldVector.getName(), value);
                baseVector.setNull(index);
            }
        }
    }

    private void writeBaseVariableWidthVector(int index, Object value, BaseVariableWidthVector fieldVector) {
        final BaseVariableWidthVector baseVector = fieldVector;
        if (value == null) {
            baseVector.setNull(index);
            return;
        }
        if (value instanceof DurationValue) {
            value = ((DurationValue) value).toString();
        }
        if (value instanceof String) {
            baseVector.setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
        } else {
            baseVector.setSafe(index, JsonUtil.writeValueAsBytes(value));
        }
    }
}
