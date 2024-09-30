package org.apache.flink.formats.protobuf3.serialize;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.WireFormat;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author YosanHo
 */
public class ProtobufEncoders {

    public ProtobufEncoders() {
    }

    public interface Encoder {
        default byte[] encode(Object obj) {
            try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
                CodedOutputStream cos = CodedOutputStream.newInstance(bytes, 4096);
                encode(obj, cos, 0);
                cos.flush();
                return bytes.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        void encode(Object obj, CodedOutputStream cos, int idx) throws IOException;
    }

    public Encoder createEncoder(LogicalType type) {
        return wrapNullable(createNullableEncoder(type));
    }

    private Encoder wrapNullable(Encoder encoder) {
        return (obj, cos, idx) -> {
            if (obj != null) {
                encoder.encode(obj, cos, idx);
            }
        };
    }

    private Encoder createNullableEncoder(LogicalType type) {
        if (type instanceof RowType) {
            return createRowEncoder((RowType) type);
        }
        if (type instanceof ArrayType) {
            return createArrayEncoder((ArrayType) type);
        }
        if (type instanceof MapType) {
            return createMapEncoder((MapType) type);
        }
        LogicalTypeRoot typeRoot = type.getTypeRoot();
        switch (typeRoot) {
            case VARCHAR:
                return (o, cos, idx) -> cos.writeString(idx, o.toString());
            case INTEGER:
                return (o, cos, idx) -> cos.writeInt32(idx, (Integer) o);
            case FLOAT:
                return (o, cos, idx) -> cos.writeFloat(idx, (Float) o);
            case BIGINT:
                return (o, cos, idx) -> cos.writeInt64(idx, (Long) o);
            case DOUBLE:
                return (o, cos, idx) -> cos.writeDouble(idx, (Double) o);
            case BOOLEAN:
                return (o, cos, idx) -> cos.writeBool(idx, (Boolean) o);
            case VARBINARY:
                return (o, cos, idx) -> cos.writeBytes(idx, ByteString.copyFrom((byte[]) o));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private Encoder createRowEncoder(RowType rowType) {
        List<LogicalType> fields = rowType.getChildren();
        Encoder[] fieldEncoders = fields.stream().map(this::createEncoder).toArray(Encoder[]::new);
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldGetters[i] = RowData.createFieldGetter(fields.get(i), i);
        }
        SizeComputers.SizeComputer sizeComputer = SizeComputers.create(rowType);
        return (o, cos, idx) -> {
            if (idx > 0) {
                cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                int size = sizeComputer.computeSize(o, idx);
                cos.writeUInt32NoTag(size);
            }
            RowData row = (RowData) o;
            for (int i = 0; i < row.getArity(); i++) {
                fieldEncoders[i].encode(fieldGetters[i].getFieldOrNull(row), cos, i + 1);
            }
        };
    }

    private Encoder createArrayEncoder(ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        LogicalTypeRoot typeRoot = elementType.getTypeRoot();
        switch (typeRoot) {
            case ROW:
                List<LogicalType> children = elementType.getChildren();
                int fieldSize = children.size();
                Encoder elementEncoder = createEncoder(elementType);
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    for (int i = 0; i < array.size(); i++) {
                        if (array.isNullAt(i)) {
                            cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                            cos.writeUInt32NoTag(0);
                        } else {
                            RowData row = array.getRow(i, fieldSize);
                            elementEncoder.encode(row, cos, idx);
                        }

                    }
                };
            case VARCHAR:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeString(idx, array.getString(i).toString());
                    }
                };
            case INTEGER:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                    int dataSize = 0;
                    for (int i = 0; i < array.size(); i++) {
                        dataSize += CodedOutputStream.computeInt32SizeNoTag(array.getInt(i));
                    }
                    cos.writeUInt32NoTag(dataSize);
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeInt32NoTag(array.getInt(i));
                    }
                };
            case FLOAT:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                    int dataSize = 4 * array.size();
                    cos.writeUInt32NoTag(dataSize);
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeFloatNoTag(array.getFloat(i));
                    }
                };
            case BIGINT:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                    int dataSize = 0;
                    for (int i = 0; i < array.size(); i++) {
                        dataSize += CodedOutputStream.computeInt64SizeNoTag(array.getInt(i));
                    }
                    cos.writeUInt32NoTag(dataSize);
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeInt64NoTag(array.getLong(i));
                    }
                };
            case DOUBLE:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                    int dataSize = 8 * array.size();
                    cos.writeUInt32NoTag(dataSize);
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeDoubleNoTag(array.getDouble(i));
                    }
                };
            case BOOLEAN:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    cos.writeTag(idx, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                    int dataSize = array.size();
                    cos.writeUInt32NoTag(dataSize);
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeBoolNoTag(array.getBoolean(i));
                    }
                };
            case VARBINARY:
                return (o, cos, idx) -> {
                    ArrayData array = (ArrayData) o;
                    if (array.size() == 0) {
                        return;
                    }
                    for (int i = 0; i < array.size(); i++) {
                        cos.writeBytes(idx, ByteString.copyFrom(array.getBinary(i)));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + elementType);
        }
    }

    private Encoder createMapEncoder(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        RowType rowType = RowType.of(keyType, valueType);
        Encoder rowEncoder = createRowEncoder(rowType);
        return (o, cos, idx) -> {
            MapData map = (MapData) o;
            if (map.size() == 0) {
                return;
            }
            ArrayData keyArray = map.keyArray();
            ArrayData valueArray = map.valueArray();

            for (int i = 0; i < map.size(); i++) {
                Object key = keyGetter.getElementOrNull(keyArray, i);
                Object value = valueGetter.getElementOrNull(valueArray, i);
                rowEncoder.encode(GenericRowData.of(key, value), cos, idx);
            }
        };
    }

}
