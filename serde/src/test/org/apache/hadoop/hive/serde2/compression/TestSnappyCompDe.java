/**
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

package org.apache.hadoop.hive.serde2.compression;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.serde2.compression.SnappyCompDe;
import org.apache.hadoop.hive.serde2.thrift.ColumnBuffer;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.hive.service.rpc.thrift.TStringColumn;

import static org.junit.Assert.assertArrayEquals;

import java.awt.List;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

public class TestSnappyCompDe {

  private static HiveConf hiveConf = new HiveConf();
  private SnappyCompDe compDe = new SnappyCompDe();
  private ColumnBuffer[] testCols;

  @Before
  public void init() {

    ColumnBuffer columnInt = new ColumnBuffer(Type.INT_TYPE);
    columnInt.addValue(Type.INT_TYPE, 0);
    columnInt.addValue(Type.INT_TYPE, 1);
    columnInt.addValue(Type.INT_TYPE, 2);
    columnInt.addValue(Type.INT_TYPE, 3);

    ColumnBuffer columnStr = new ColumnBuffer(Type.STRING_TYPE);
    columnStr.addValue(Type.STRING_TYPE, "ABC");
    columnStr.addValue(Type.STRING_TYPE, "DEF");
    columnStr.addValue(Type.STRING_TYPE, "GHI");

    // Test trailing `false` in column
    ColumnBuffer columnBool = new ColumnBuffer(Type.BOOLEAN_TYPE);
    columnBool.addValue(Type.BOOLEAN_TYPE, true);
    columnBool.addValue(Type.BOOLEAN_TYPE, false);

    // Test nulls bitmask
    byte[] firstNull = {1};
    byte[] secondNull = {2};
    byte[] thirdNull = {3};
    ArrayList<String> someStrings = new ArrayList<String>();
    someStrings.add("test1");
    someStrings.add("test2");
    ColumnBuffer columnStr2 = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(someStrings, ByteBuffer.wrap(firstNull))));
    ColumnBuffer columnStr3 = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(someStrings, ByteBuffer.wrap(secondNull))));
    ColumnBuffer columnStr4 = new ColumnBuffer(TColumn.stringVal(
        new TStringColumn(someStrings, ByteBuffer.wrap(thirdNull))));

    testCols = new ColumnBuffer[]{
        columnInt, 
        columnStr, 
        columnBool,
        columnStr2,
        columnStr3,
        columnStr4};

    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_COMPRESSOR, compDe.getVendor() + "." + compDe.getName());
  }

  @Test
  public void testCompDe() {
    byte[] compressedCols = compDe.compress(testCols);
    ColumnBuffer[] decompressedCols = compDe.decompress(compressedCols);
    assertArrayEquals(testCols, decompressedCols);
  }
}
