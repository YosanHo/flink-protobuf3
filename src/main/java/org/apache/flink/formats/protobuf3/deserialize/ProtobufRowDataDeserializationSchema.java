package org.apache.flink.formats.protobuf3.deserialize;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * @author YosanHo
 */
public class ProtobufRowDataDeserializationSchema implements DeserializationSchema<RowData> {
    private final boolean readDefaultValues;
    private final TypeInformation<RowData> resultTypeInfo;
    private final RowType rowType;
    private transient ProtobufDecoders.Decoder decoder;

    public ProtobufRowDataDeserializationSchema(boolean readDefaultValues, RowType rowType, TypeInformation<RowData> resultTypeInfo) {
        this.readDefaultValues = readDefaultValues;
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        decoder = new ProtobufDecoders(readDefaultValues).createConverter(rowType);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        return decoder.decode(message);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }
}
