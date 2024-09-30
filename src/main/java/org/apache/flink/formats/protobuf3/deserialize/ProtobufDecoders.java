package org.apache.flink.formats.protobuf3.deserialize;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.PbFormatUtils;
import com.google.protobuf.WireFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author YosanHo
 */
public class ProtobufDecoders {
    private final boolean readDefaultValues;

    public ProtobufDecoders(boolean readDefaultValues) {
        this.readDefaultValues = readDefaultValues;
    }

    public interface Decoder extends Serializable {
        default Object decode(CodedInputStream input) throws IOException {
            return decode(input, 0);
        }

        default RowData decode(byte[] bytes) throws IOException {
            return (RowData) decode(CodedInputStream.newInstance(bytes));
        }

        Object decode(CodedInputStream input, int tag) throws IOException;
    }

    public Decoder createConverter(RowType rowType) {
        Decoder doRowConverter = createDoRowConverter(rowType);
        return (input, t) -> GenericRowData.of((Object[]) doRowConverter.decode(input));
    }

    private Decoder createRowConverter(RowType rowType) {
        Decoder doRowConverter = createDoRowConverter(rowType);

        return (input, t) -> {
            int length = input.readRawVarint32();
            input.checkRecursionLimit();
            final int oldLimit = input.pushLimit(length);
            Object rows = doRowConverter.decode(input);
            input.checkLastTagWas(0);
            if (input.getBytesUntilLimit() != 0) {
                throw new InvalidProtocolBufferException(
                        "While parsing a protocol message, the input ended unexpectedly "
                                + "in the middle of a field.  This could mean either that the "
                                + "input has been truncated or that an embedded message "
                                + "misreported its own length.");
            }
            input.popLimit(oldLimit);
            return GenericRowData.of((Object[]) rows);
        };
    }


    private Decoder createDoRowConverter(RowType rowType) {
        List<LogicalType> fields = rowType.getChildren();
        final Decoder[] fieldConverters = fields.stream()
                .map(this::createConverter)
                .toArray(Decoder[]::new);
        final int arity = fields.size();

        return (input, t) -> {
            Object[] row = new Object[arity];
            boolean skipTag = false;
            int tag = 0;
            int preIndex = -1;
            while (true) {
                if (!skipTag) {
                    tag = input.readTag();
                }
                if (tag == 0) {
                    break;
                }
                int fieldNumber = WireFormat.getTagFieldNumber(tag);
                int i = fieldNumber - 1;
                LogicalType fieldType = fields.get(i);
                skipTag = skipTag(fieldType);

                Object convertedField = fieldConverters[i].decode(input, tag);
                if (convertedField instanceof Tuple2) {
                    Tuple2<Object, Integer> tuple2 = (Tuple2<Object, Integer>) convertedField;
                    row[i] = tuple2.f0;
                    tag = tuple2.f1;
                } else {
                    row[i] = convertedField;
                }
                if (readDefaultValues && i > preIndex + 1) {
                    for (int j = preIndex + 1; j < i; j++) {
                        row[j] = defaultValue(fields.get(j));
                    }
                }
                preIndex = i;
            }
            if (readDefaultValues) {
                for (int j = preIndex + 1; j < arity; j++) {
                    row[j] = defaultValue(fields.get(j));
                }
            }
            return row;
        };
    }


    private Decoder createConverter(LogicalType type) {
        if (type instanceof RowType) {
            return createRowConverter((RowType) type);
        }
        if (type instanceof MapType) {
            return createMapConverter((MapType) type);
        }
        if (type instanceof ArrayType) {
            return createArrayConverter((ArrayType) type);
        }
        return createPrimitiveConverter(type);
    }

    private Decoder createMapConverter(MapType mapType) {
        Decoder mapConverter0 = createMapEntryConverter(mapType);

        return (input, tag) -> {
            Map<Object, Object> map = new HashMap<>();
            Tuple2 entry = (Tuple2) mapConverter0.decode(input);
            if (entry.f0 != null && entry.f1 != null) {
                map.put(entry.f0, entry.f1);
            }
            int preTag = tag;
            while ((tag = input.readTag()) == preTag) {
                entry = (Tuple2) mapConverter0.decode(input);
                if (entry.f0 != null && entry.f1 != null) {
                    map.put(entry.f0, entry.f1);
                }
            }
            return Tuple2.of(new GenericMapData(map), tag);
        };
    }

    private Object defaultValue(LogicalType dataType) {
        switch (dataType.getTypeRoot()) {
            case VARCHAR:
                return StringData.fromString("");
            case INTEGER:
                return 0;
            case BIGINT:
                return 0L;
            case FLOAT:
                return 0.0f;
            case DOUBLE:
                return 0.0d;
            case BOOLEAN:
                return false;
            case VARBINARY:
                return new byte[0];
            default:
                return null;
        }
    }

    private Decoder createMapEntryConverter(MapType mapType) {
        Decoder keyConverter = createConverter(mapType.getKeyType());
        Decoder valueConverter = createConverter(mapType.getValueType());

        int keyWireType = PbFormatUtils.protoFieldType(mapType.getKeyType()).getWireType();
        int valueWireType = PbFormatUtils.protoFieldType(mapType.getValueType()).getWireType();
        int keyTag = PbFormatUtils.makeTag(1, keyWireType);
        int valueTag = PbFormatUtils.makeTag(2, valueWireType);

        return (input, tag) -> {
            int length = input.readRawVarint32();
            int limit = input.pushLimit(length);
            Object key = null;
            Object value = null;
            while (true) {
                tag = input.readTag();
                if (tag == 0) {
                    break;
                }
                if (tag == keyTag) {
                    key = keyConverter.decode(input);
                } else if (tag == valueTag) {
                    value = valueConverter.decode(input);
                } else if (!input.skipField(tag)) {
                    break;
                }
            }
            input.popLimit(limit);
            return Tuple2.of(key, value);
        };
    }


    private Decoder createPrimitiveConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
                return (input, t) -> StringData.fromString(input.readString());
            case INTEGER:
                return (input, t) -> input.readInt32();
            case BIGINT:
                return (input, t) -> input.readInt64();
            case FLOAT:
                return (input, t) -> input.readFloat();
            case DOUBLE:
                return (input, t) -> input.readDouble();
            case BOOLEAN:
                return (input, t) -> input.readBool();
            case VARBINARY:
                return (input, t) -> input.readByteArray();
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private Decoder createArrayConverter(ArrayType arrayType) {
        LogicalType valueType = arrayType.getElementType();
        if (valueType instanceof RowType) {
            Decoder rowConverter = createRowConverter((RowType) valueType);
            return (input, tag) -> {
                List<RowData> list = new ArrayList<>();
                list.add((RowData) rowConverter.decode(input));
                int preTag = tag;
                while ((tag = input.readTag()) == preTag) {
                    list.add((RowData) rowConverter.decode(input));
                }
                GenericArrayData arr = new GenericArrayData(list.toArray(new RowData[0]));
                return Tuple2.of(arr, tag);
            };
        } else {
            LogicalTypeRoot typeRoot = valueType.getTypeRoot();
            if (LogicalTypeRoot.VARCHAR.equals(typeRoot)) {
                return (input, tag) -> {
                    List<StringData> strList = new ArrayList<>();
                    strList.add(StringData.fromString(input.readString()));
                    int preTag = tag;
                    while ((tag = input.readTag()) == preTag) {
                        strList.add(StringData.fromString(input.readString()));
                    }
                    return Tuple2.of(new GenericArrayData(strList.toArray(new StringData[0])), tag);
                };
            } else {
                return createNumberArrayConverter(typeRoot);
            }
        }
    }

    private Decoder createNumberArrayConverter(LogicalTypeRoot typeRoot) {
        switch (typeRoot) {
            case INTEGER:
                return (input, tag) -> {
                    int length = input.readRawVarint32();
                    int limit = input.pushLimit(length);
                    List<Integer> list = new ArrayList<>();
                    while (input.getBytesUntilLimit() > 0) {
                        list.add(input.readInt32());
                    }
                    input.popLimit(limit);
                    return new GenericArrayData(list.toArray(new Integer[0]));
                };
            case BIGINT:
                return (input, tag) -> {
                    int length = input.readRawVarint32();
                    int limit = input.pushLimit(length);
                    List<Long> list = new ArrayList<>();
                    while (input.getBytesUntilLimit() > 0) {
                        list.add(input.readInt64());
                    }
                    input.popLimit(limit);
                    return new GenericArrayData(list.toArray(new Long[0]));
                };
            case FLOAT:
                return (input, tag) -> {
                    int length = input.readRawVarint32();
                    int limit = input.pushLimit(length);
                    List<Float> list = new ArrayList<>();
                    while (input.getBytesUntilLimit() > 0) {
                        list.add(input.readFloat());
                    }
                    input.popLimit(limit);
                    return new GenericArrayData(list.toArray(new Float[0]));
                };
            case DOUBLE:
                return (input, tag) -> {
                    int length = input.readRawVarint32();
                    int limit = input.pushLimit(length);
                    List<Double> list = new ArrayList<>();
                    while (input.getBytesUntilLimit() > 0) {
                        list.add(input.readDouble());
                    }
                    input.popLimit(limit);
                    return new GenericArrayData(list.toArray(new Double[0]));
                };
        }
        throw new IllegalArgumentException("Unsupported type for array's element LogicalTypeRoot: " + typeRoot);
    }

    private boolean skipTag(LogicalType fieldType) {
        if (fieldType instanceof MapType) {
            return true;
        } else if (fieldType instanceof ArrayType) {
            LogicalType valueType = ((ArrayType) fieldType).getElementType();
            return valueType instanceof VarCharType || valueType instanceof RowType;
        }
        return false;
    }
}
