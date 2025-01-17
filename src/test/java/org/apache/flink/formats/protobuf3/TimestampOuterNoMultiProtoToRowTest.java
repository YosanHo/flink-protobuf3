/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf3;

import com.google.protobuf.Timestamp;
import org.apache.flink.formats.protobuf3.testproto.TimestampTestOuterNomultiProto;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test conversion of proto timestamp data with outer_classname options to flink internal data.
 */
public class TimestampOuterNoMultiProtoToRowTest {

    @Test
    public void testSimple() throws Exception {
        TimestampTestOuterNomultiProto.TimestampTestOuterNoMulti timestampTestOuterNoMulti =
                TimestampTestOuterNomultiProto.TimestampTestOuterNoMulti.newBuilder()
                        .setTs(Timestamp.newBuilder().setSeconds(1672498800).setNanos(123))
                        .build();

        RowType rowType = PbToRowTypeUtil.generateRowType(TimestampTestOuterNomultiProto.TimestampTestOuterNoMulti.getDescriptor());
        RowData row = ProtobufTestHelper.pbBytesToRow(timestampTestOuterNoMulti.toByteArray(), rowType);

        RowData rowData = row.getRow(0, 2);
        assertEquals(1672498800, rowData.getLong(0));
        assertEquals(123, rowData.getInt(1));
    }
}
