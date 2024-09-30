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

import com.google.protobuf.ByteString;
import org.apache.flink.formats.protobuf3.testproto.SimpleTestMulti;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test conversion of proto primitive data to flink internal data.
 */
public class SimpleProtoToRowTest {
    @Test
    public void testSimple() throws Exception {
        SimpleTestMulti simple =
                SimpleTestMulti.newBuilder()
                        .setA(1)
                        .setB(2L)
                        .setC(false)
                        .setD(0.1f)
                        .setE(0.01)
                        .setF("haha")
                        .setG(ByteString.copyFrom(new byte[]{1}))
                        .setFAbc7D(1) // test fieldNameToJsonName
                        .setVpr6S(2)
                        .build();
        RowType rowType = PbToRowTypeUtil.generateRowType(SimpleTestMulti.getDescriptor());
        RowData row = ProtobufTestHelper.pbBytesToRow(simple.toByteArray(), rowType);

        assertEquals(9, row.getArity());
        assertEquals(1, row.getInt(0));
        assertEquals(2L, row.getLong(1));
        assertFalse(row.getBoolean(2));
        assertEquals(Float.valueOf(0.1f), Float.valueOf(row.getFloat(3)));
        assertEquals(Double.valueOf(0.01d), Double.valueOf(row.getDouble(4)));
        assertEquals("haha", row.getString(5).toString());
        assertEquals(1, (row.getBinary(6))[0]);
        assertEquals(1, row.getInt(7));
        assertEquals(2, row.getInt(8));
    }

    @Test
    public void testNotExistsValueIgnoringDefault() throws Exception {
        SimpleTestMulti simple =
                SimpleTestMulti.newBuilder()
                        .setB(2L)
                        .setC(false)
                        .setD(0.1f)
                        .setE(0.01)
                        .setF("haha")
                        .build();
        RowType rowType = PbToRowTypeUtil.generateRowType(SimpleTestMulti.getDescriptor());
        RowData row = ProtobufTestHelper.pbBytesToRow(simple.toByteArray(), rowType);

        assertTrue(row.isNullAt(0));
        assertFalse(row.isNullAt(1));
    }

    @Test
    public void testDefaultValues() throws Exception {
        SimpleTestMulti simple = SimpleTestMulti.newBuilder().build();
        RowType rowType = PbToRowTypeUtil.generateRowType(SimpleTestMulti.getDescriptor());
        RowData row = ProtobufTestHelper.pbBytesToRow(simple.toByteArray(), rowType, true);

        assertFalse(row.isNullAt(0));
        assertFalse(row.isNullAt(1));
        assertFalse(row.isNullAt(2));
        assertFalse(row.isNullAt(3));
        assertFalse(row.isNullAt(4));
        assertFalse(row.isNullAt(5));
        assertFalse(row.isNullAt(6));
        assertFalse(row.isNullAt(7));
        assertFalse(row.isNullAt(8));
        assertEquals(0, row.getInt(0));
        assertEquals(0L, row.getLong(1));
        assertFalse(row.getBoolean(2));
        assertEquals(0.0f, row.getFloat(3), 0.0001);
        assertEquals(0.0d, row.getDouble(4), 0.0001);
        assertEquals("", row.getString(5).toString());
        assertArrayEquals(ByteString.EMPTY.toByteArray(), row.getBinary(6));
    }
}
