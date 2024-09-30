package com.google.protobuf;


import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public class PbFormatUtils {
    public static int makeTag(int fieldNumber, int wireType) {
        return WireFormat.makeTag(fieldNumber, wireType);
    }

    public static WireFormat.FieldType protoFieldType(LogicalType type) {
        if (type instanceof RowType) {
            return WireFormat.FieldType.MESSAGE;
        }
        LogicalTypeRoot typeRoot = type.getTypeRoot();
        switch (typeRoot) {
            case VARCHAR:
                return WireFormat.FieldType.STRING;
            case INTEGER:
                return WireFormat.FieldType.INT32;
            case BIGINT:
                return WireFormat.FieldType.INT64;
            case FLOAT:
                return WireFormat.FieldType.FLOAT;
            case DOUBLE:
                return WireFormat.FieldType.DOUBLE;
            case BOOLEAN:
                return WireFormat.FieldType.BOOL;
            case VARBINARY:
                return WireFormat.FieldType.BYTES;
            default:
                throw new IllegalArgumentException("Unsupported type:" + type);
        }
    }
}

