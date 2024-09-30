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
import org.apache.flink.formats.protobuf3.testproto.MapTest;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test conversion of flink internal map data to proto data.
 */
public class MapRowToProtoTest {
    @Test
    public void testSimple() throws Exception {
        Map<StringData, StringData> map1 = new HashMap<>();
        map1.put(StringData.fromString("a"), StringData.fromString("b"));
        Map<StringData, RowData> map2 = new HashMap<>();
        map2.put(StringData.fromString("c"), GenericRowData.of(1, 2L));
        Map<StringData, byte[]> map3 = new HashMap<>();
        map3.put(StringData.fromString("e"), new byte[]{1, 2, 3});
        RowData row =
                GenericRowData.of(
                        1,
                        new GenericMapData(map1),
                        new GenericMapData(map2),
                        new GenericMapData(map3));

        RowType rowType = PbToRowTypeUtil.generateRowType(MapTest.getDescriptor());
        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, rowType);

        MapTest mapTest = MapTest.parseFrom(bytes);
        assertEquals(1, mapTest.getA());
        assertEquals("b", mapTest.getMap1Map().get("a"));
        MapTest.InnerMessageTest innerMessageTest = mapTest.getMap2Map().get("c");
        assertEquals(1, innerMessageTest.getA());
        assertEquals(2L, innerMessageTest.getB());
        assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), mapTest.getMap3Map().get("e"));
    }

    @Test
    public void testNull() throws Exception {
        RowData row = GenericRowData.of(1, null, null, null);
        RowType rowType = PbToRowTypeUtil.generateRowType(MapTest.getDescriptor());
        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, rowType);
        MapTest mapTest = MapTest.parseFrom(bytes);
        Map<String, String> map = mapTest.getMap1Map();
        assertEquals(0, map.size());
    }
}
