package org.apache.flink.formats.protobuf3;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf3.deserialize.ProtobufRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf3.serialize.ProtobufRowDataSerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author YosanHo
 */
public class ProtobufFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final String IDENTIFIER = "dynamic-protobuf3";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context,
                                                                               ReadableConfig formatOptions) {
        final boolean readDefaultValues = formatOptions.get(ProtobufFormatOptions.READ_DEFAULT_VALUES);
        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(physicalDataType);
                return new ProtobufRowDataDeserializationSchema(readDefaultValues, rowType, rowDataTypeInfo);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(DynamicTableSink.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                return new ProtobufRowDataSerializationSchema(rowType);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(ProtobufFormatOptions.READ_DEFAULT_VALUES);
        return result;
    }
}
