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

import org.apache.flink.formats.protobuf3.testproto.OneofTest;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test conversion of proto one_of data to flink internal data.
 */
public class OneofProtoToRowTest {
    @Test
    public void testSimple() throws Exception {
        OneofTest oneofTest = OneofTest.newBuilder().setA(1).setB(2).build();
        RowType rowType = PbToRowTypeUtil.generateRowType(OneofTest.getDescriptor());
        RowData row = ProtobufTestHelper.pbBytesToRow(oneofTest.toByteArray(), rowType);
        assertTrue(row.isNullAt(0));
        assertEquals(2, row.getInt(1));
    }
}
