package org.apache.flink.formats.protobuf3;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @author YosanHo
 */
public class ProtobufFormatOptions {
    public static final ConfigOption<Boolean> READ_DEFAULT_VALUES =
            ConfigOptions.key("read-default-values")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to read as default values instead of null when some field does not exist in deserialization; default to false."
                                    + "If proto syntax is proto3, this value will be set true forcibly because proto3's standard is to use default values.");
}
