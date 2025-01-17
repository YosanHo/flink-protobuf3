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

import org.apache.flink.formats.protobuf3.testproto.SimpleTestMulti;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test conversion of flink internal primitive data to proto data.
 */
public class SimpleRowToProtoTest {
    @Test
    public void testSimple() throws Exception {
        RowData row = GenericRowData.of(
                1,
                2L,
                false,
                0.1f,
                0.01,
                StringData.fromString("hello"),
                new byte[]{1},
                1,
                2);
        RowType rowType = PbToRowTypeUtil.generateRowType(SimpleTestMulti.getDescriptor());
        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, rowType);
        SimpleTestMulti simpleTestMulti = SimpleTestMulti.parseFrom(bytes);
        assertTrue(simpleTestMulti.hasA());
        assertEquals(1, simpleTestMulti.getA());
        assertEquals(2L, simpleTestMulti.getB());
        assertFalse(simpleTestMulti.getC());
        assertEquals(Float.valueOf(0.1f), Float.valueOf(simpleTestMulti.getD()));
        assertEquals(Double.valueOf(0.01d), Double.valueOf(simpleTestMulti.getE()));
        assertEquals("hello", simpleTestMulti.getF());
        assertEquals(1, simpleTestMulti.getG().byteAt(0));
        assertEquals(1, simpleTestMulti.getFAbc7D());
    }

    @Test
    public void testNull() throws Exception {
        RowData row =
                GenericRowData.of(
                        null,
                        2L,
                        false,
                        0.1f,
                        0.01,
                        StringData.fromString("hello"),
                        null,
                        1,
                        2);
        RowType rowType = PbToRowTypeUtil.generateRowType(SimpleTestMulti.getDescriptor());
        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, rowType);
        SimpleTestMulti simpleTestMulti = SimpleTestMulti.parseFrom(bytes);
        assertFalse(simpleTestMulti.hasA());
        assertFalse(simpleTestMulti.hasG());
    }
}
