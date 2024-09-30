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
import org.apache.flink.formats.protobuf3.testproto.TestTimestampNomulti;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test conversion of proto timestamp data to flink internal data.
 */
public class TimestampNoMultiProtoToRowTest {

    @Test
    public void testSimple() throws Exception {
        TestTimestampNomulti.TimestampTestNoMulti timestampTestNoMulti =
                TestTimestampNomulti.TimestampTestNoMulti.newBuilder()
                        .setTs(Timestamp.newBuilder().setSeconds(1672498800).setNanos(123))
                        .build();

        RowType rowType = PbToRowTypeUtil.generateRowType(TestTimestampNomulti.TimestampTestNoMulti.getDescriptor());
        RowData row = ProtobufTestHelper.pbBytesToRow(timestampTestNoMulti.toByteArray(), rowType);

        RowData rowData = row.getRow(0, 2);
        assertEquals(1672498800, rowData.getLong(0));
        assertEquals(123, rowData.getInt(1));
    }
}
