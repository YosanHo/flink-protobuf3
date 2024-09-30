package org.apache.flink.formats.protobuf3.serialize;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

import java.util.List;

/**
 * @author YosanHo
 */
public class SizeComputers {

    public interface SizeComputer {
        int computeSize(Object obj, int idx);
    }

    public static SizeComputer create(RowType rowType) {
        return create(rowType.getChildren());
    }

    private static SizeComputer create(LogicalType type) {
        SizeComputer computer = createNotNullable(type);
        return (o, idx) -> {
            if (o == null) {
                return 0;
            }
            return computer.computeSize(o, idx);
        };
    }

    private static SizeComputer createNotNullable(LogicalType type) {
        if (type instanceof RowType) {
            return createSub(((RowType) type).getChildren());
        }
        if (type instanceof ArrayType) {
            return createArray(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return createMap(mapType.getKeyType(), mapType.getValueType());
        }
        switch (type.getTypeRoot()) {
            case INTEGER:
                return (v, i) -> CodedOutputStream.computeInt32Size(i, (Integer) v);
            case BIGINT:
                return (v, i) -> CodedOutputStream.computeInt64Size(i, (Long) v);
            case FLOAT:
                return (v, i) -> CodedOutputStream.computeFloatSize(i, (Float) v);
            case DOUBLE:
                return (v, i) -> CodedOutputStream.computeDoubleSize(i, (Double) v);
            case VARCHAR:
                return (v, i) -> CodedOutputStream.computeStringSize(i, v.toString());
            case BOOLEAN:
                return (v, i) -> CodedOutputStream.computeBoolSize(i, (Boolean) v);
            case VARBINARY:
                return (v, i) -> CodedOutputStream.computeBytesSize(i, ByteString.copyFrom((byte[]) v));
            default:
                return (v, i) -> 0;
        }
    }



    private static SizeComputer create(List<LogicalType> fields) {
        SizeComputer[] sizeComputers = fields.stream().map(SizeComputers::create).toArray(SizeComputer[]::new);
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldGetters[i] = RowData.createFieldGetter(fields.get(i), i);
        }
        return (o, idx) -> {
            int size = 0;
            RowData row = (RowData) o;
            for (int i = 0; i < fields.size(); i++) {
                size += sizeComputers[i].computeSize(fieldGetters[i].getFieldOrNull(row), i + 1);
            }
            return size;
        };
    }


    private static SizeComputer createSub(List<LogicalType> children) {
        SizeComputer computer = create(children);
        return (o, idx) -> {
            int tagSize = CodedOutputStream.computeTagSize(idx);
            int entrySize = computer.computeSize(o, -1);
            int entrySizeLen = CodedOutputStream.computeUInt32SizeNoTag(entrySize);
            return tagSize + entrySizeLen + entrySize;
        };

    }

    private static SizeComputer createMap(LogicalType keyType, LogicalType valueType) {
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        RowType rowType = RowType.of(keyType, valueType);
        SizeComputer computer = create(rowType);
        return (o, idx) -> {
            MapData map = (MapData) o;
            if (map.size() == 0) {
                return 0;
            }
            int size = 0;
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();
            for (int i = 0; i < map.size(); i++) {
                Object key = keyGetter.getElementOrNull(keyArray, i);
                Object value = valueGetter.getElementOrNull(valueArray, i);
                size += computer.computeSize(GenericRowData.of(key, value), idx);
            }
            return size;
        };
    }

    private static SizeComputer createArray(LogicalType elementType) {
        LogicalTypeRoot typeRoot = elementType.getTypeRoot();
        switch (typeRoot) {
            case VARCHAR:
                return (o, idx) -> {
                    ArrayData arrayData = (ArrayData) o;
                    if (arrayData.size() == 0) {
                        return 0;
                    }
                    int size = 0;
                    for (int i = 0; i < arrayData.size(); i++) {
                        size += CodedOutputStream.computeStringSizeNoTag(arrayData.getString(i).toString());
                    }
                    return size + arrayData.size();
                };
            case INTEGER:
                return (o, idx) -> {
                    ArrayData arrayData = (ArrayData) o;
                    if (arrayData.size() == 0) {
                        return 0;
                    }
                    int size = 0;
                    for (int i = 0; i < arrayData.size(); i++) {
                        size += CodedOutputStream.computeInt32SizeNoTag(arrayData.getInt(i));
                    }
                    return size + CodedOutputStream.computeInt32SizeNoTag(size) + 1;
                };
            case BIGINT:
                return (o, idx) -> {
                    ArrayData arrayData = (ArrayData) o;
                    if (arrayData.size() == 0) {
                        return 0;
                    }
                    int size = 0;
                    for (int i = 0; i < arrayData.size(); i++) {
                        size += CodedOutputStream.computeInt64SizeNoTag(arrayData.getLong(i));
                    }
                    return size + CodedOutputStream.computeInt32SizeNoTag(size) + 1;
                };
            case FLOAT:
                return (o, idx) -> {
                    ArrayData arrayData = (ArrayData) o;
                    if (arrayData.size() == 0) {
                        return 0;
                    }
                    int size = 4 * arrayData.size();
                    return size + CodedOutputStream.computeInt32SizeNoTag(size) + 1;
                };
            case DOUBLE:
                return (o, idx) -> {
                    ArrayData arrayData = (ArrayData) o;
                    if (arrayData.size() == 0) {
                        return 0;
                    }
                    int size = 8 * arrayData.size();
                    return size + CodedOutputStream.computeInt32SizeNoTag(size) + 1;
                };
            default:
                return (o, idx) -> 0;
        }
    }
}
