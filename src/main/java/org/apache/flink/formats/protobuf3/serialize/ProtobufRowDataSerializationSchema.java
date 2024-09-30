package org.apache.flink.formats.protobuf3.serialize;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author YosanHo
 */
public class ProtobufRowDataSerializationSchema implements SerializationSchema<RowData> {
    private final RowType rowType;
    private transient ProtobufEncoders.Encoder encoder;

    public ProtobufRowDataSerializationSchema(RowType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        encoder = new ProtobufEncoders().createEncoder(rowType);
    }

    @Override
    public byte[] serialize(RowData element) {
        return encoder.encode(element);
    }
}
